#!/usr/bin/env python3
"""Interactive review of proactive engagement actions.

Primary source: presence's proactive_actions table (from discover_replies.py).
Fallback source: cultivate's actions table (if enabled).
"""

import argparse
import json
import re
import sys
import webbrowser
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from output.x_client import XClient
from engagement.proactive_cooldown import (
    DEFAULT_AUTHOR_COOLDOWN_HOURS,
    DEFAULT_TARGET_COOLDOWN_HOURS,
    ProactiveCooldownPolicy,
    evaluate_proactive_cooldown,
)
from engagement.reply_drafter import ReplyDrafter
from review_helpers import truncate, read_char, format_relationship_context

_EXEC_TAG_RE = re.compile(r"^\[(\w+)\]")
_VALID_EXEC_TYPES = {"like", "retweet", "reply", "quote_tweet", "follow"}
_COOLDOWN_EXEC_TYPES = {"like", "retweet", "reply", "quote_tweet"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse review options."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--author-cooldown-hours",
        type=int,
        default=None,
        help=(
            "Block actions to authors with recent posted/approved actions "
            f"(default: config proactive.account_cooldown_hours or {DEFAULT_AUTHOR_COOLDOWN_HOURS})"
        ),
    )
    parser.add_argument(
        "--target-cooldown-hours",
        type=int,
        default=DEFAULT_TARGET_COOLDOWN_HOURS,
        help=(
            "Block duplicate actions to the same target tweet "
            f"(default: {DEFAULT_TARGET_COOLDOWN_HOURS})"
        ),
    )
    parser.add_argument(
        "--dismiss-cooldown-blocked",
        action="store_true",
        help="Dismiss pending presence actions that are blocked by cooldowns and exit.",
    )
    return parser.parse_args(argv)


def _parse_platform_metadata(platform_metadata: str | dict | None) -> dict:
    """Parse stored platform metadata for optional re-drafting context."""
    if isinstance(platform_metadata, dict):
        return platform_metadata
    if not platform_metadata:
        return {}
    try:
        parsed = json.loads(platform_metadata)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _normalize_presence_action(row: dict) -> dict:
    """Normalize a presence proactive_actions row into review format."""
    return {
        "source": "presence",
        "id": row["id"],
        "action_type": row["action_type"],
        "target_handle": row.get("target_author_handle", ""),
        "target_tweet_id": row.get("target_tweet_id"),
        "target_tweet_text": row.get("target_tweet_text"),
        "draft_text": row.get("draft_text"),
        "relevance_score": row.get("relevance_score"),
        "discovery_source": row.get("discovery_source"),
        "relationship_context": row.get("relationship_context"),
        "platform_metadata": row.get("platform_metadata"),
    }


def _normalize_cultivate_action(action) -> dict:
    """Normalize a cultivate ProactiveAction into review format."""
    tweet_id = None
    tweet_text = None
    draft = None
    if action.payload:
        tweet_id = action.payload.get("tweet_id")
        tweet_text = action.payload.get("tweet_content")
        draft = action.payload.get("draft")

    return {
        "source": "cultivate",
        "id": action.action_id,
        "action_type": _get_cultivate_exec_type(action) or action.action_type,
        "target_handle": action.target_handle,
        "target_tweet_id": tweet_id,
        "target_tweet_text": tweet_text,
        "draft_text": draft,
        "relevance_score": None,
        "discovery_source": "cultivate",
        "relationship_context": action.person_context.to_json() if action.person_context else None,
        "platform_metadata": None,
        "_cultivate_action": action,  # keep original for status updates
    }


def _get_cultivate_exec_type(action) -> str | None:
    """Extract execution type from cultivate action payload or description."""
    if action.payload:
        t = action.payload.get("execution_type")
        if t:
            return t
    m = _EXEC_TAG_RE.match(action.description or "")
    if m and m.group(1) in _VALID_EXEC_TYPES:
        return m.group(1)
    return None


def _mark_completed(action: dict, db, bridge, posted_tweet_id: str):
    """Mark action as completed in the appropriate DB."""
    if action["source"] == "presence":
        db.mark_proactive_posted(action["id"], posted_tweet_id)
    elif action["source"] == "cultivate" and bridge:
        bridge.mark_action_completed(action["id"])


def _mark_dismissed(action: dict, db, bridge):
    """Dismiss action in the appropriate DB."""
    if action["source"] == "presence":
        db.dismiss_proactive_action(action["id"])
    elif action["source"] == "cultivate" and bridge:
        bridge.mark_action_dismissed(action["id"])


def _format_action_header(action: dict) -> str:
    """Format action header for display."""
    label = action["action_type"].upper()
    lines = [f"{label} -> @{action['target_handle']}"]

    if action.get("relevance_score") is not None:
        lines[0] += f"  (relevance: {action['relevance_score']:.2f})"

    ctx_str = format_relationship_context(action.get("relationship_context"))
    if ctx_str:
        lines.append(f"     [{ctx_str}]")

    if action.get("discovery_source"):
        lines.append(f"     Source: {action['discovery_source']}")

    return "\n".join(lines)


def _open_action_url(action: dict) -> None:
    """Open the tweet or author profile in the default browser."""
    handle = action.get("target_handle", "")
    tweet_id = action.get("target_tweet_id")
    if tweet_id and handle:
        url = f"https://x.com/{handle}/status/{tweet_id}"
    elif handle:
        url = f"https://x.com/{handle}"
    else:
        print("  No URL to open.")
        return
    print(f"  Opening {url}")
    webbrowser.open(url)


def _publish_text_action(
    x_client: XClient, action_type: str, text: str, target_tweet_id: str
):
    """Publish a reply or quote action and return the X client result."""
    if action_type == "reply":
        return x_client.reply(text, target_tweet_id)
    if action_type == "quote_tweet":
        return x_client.quote_post(text, target_tweet_id)
    raise ValueError(f"Unsupported text action type: {action_type}")


def _account_cooldown_hours(config) -> int:
    """Return the configured proactive account cooldown window."""
    if not config.proactive or not config.proactive.enabled:
        return 0
    return max(0, getattr(config.proactive, "account_cooldown_hours", 0))


def _account_cooldown_block_reason(action: dict, db, cooldown_hours: int) -> str | None:
    """Return a human-readable reason when approval is blocked by account cooldown."""
    legacy_action = dict(action)
    legacy_action["id"] = None
    result = evaluate_proactive_cooldown(
        db,
        legacy_action,
        ProactiveCooldownPolicy(
            author_cooldown_hours=cooldown_hours,
            target_cooldown_hours=0,
        ),
    )
    return result.reason


def _configured_author_cooldown_hours(config, override: int | None) -> int:
    """Return the author cooldown window for this review run."""
    if override is not None:
        return max(0, override)
    if config.proactive and config.proactive.enabled:
        return _account_cooldown_hours(config)
    return DEFAULT_AUTHOR_COOLDOWN_HOURS


def _cooldown_policy(config, args: argparse.Namespace) -> ProactiveCooldownPolicy:
    """Build cooldown policy from CLI options and config."""
    return ProactiveCooldownPolicy(
        author_cooldown_hours=_configured_author_cooldown_hours(
            config, args.author_cooldown_hours
        ),
        target_cooldown_hours=max(0, args.target_cooldown_hours),
    )


def _cooldown_block_reason(
    action: dict,
    db,
    policy: ProactiveCooldownPolicy,
) -> str | None:
    """Return a concise review message if this action is cooldown-blocked."""
    return evaluate_proactive_cooldown(db, action, policy).reason


def _dismiss_cooldown_blocked_actions(
    actions: list[dict],
    db,
    bridge,
    policy: ProactiveCooldownPolicy,
) -> int:
    """Dismiss pending actions blocked by cooldowns without touching others."""
    dismissed = 0
    for action in actions:
        if not evaluate_proactive_cooldown(db, action, policy).blocked:
            continue
        _mark_dismissed(action, db, bridge)
        dismissed += 1
    return dismissed


def main(argv: list[str] | None = None):
    args = parse_args(argv)
    with script_context() as (config, db):
        # Collect actions from both sources
        actions = []

        # Primary: presence's proactive_actions
        presence_actions = db.get_pending_proactive_actions(limit=20)
        for row in presence_actions:
            actions.append(_normalize_presence_action(row))

        # Fallback: cultivate's actions (if enabled)
        bridge = None
        if config.cultivate and config.cultivate.enabled and config.cultivate.proactive_review:
            try:
                from engagement.cultivate_bridge import CultivateBridge

                bridge = CultivateBridge.try_connect(config.cultivate.db_path)
                if bridge:
                    cultivate_actions = bridge.get_pending_proactive_actions(limit=20)
                    for ca in cultivate_actions:
                        actions.append(_normalize_cultivate_action(ca))
            except Exception as e:
                print(f"Cultivate bridge unavailable: {e}")

        if not actions:
            print("No pending proactive actions.")
            if bridge:
                bridge.close()
            return

        cooldown_policy = _cooldown_policy(config, args)
        if args.dismiss_cooldown_blocked:
            dismissed = _dismiss_cooldown_blocked_actions(
                actions, db, bridge, cooldown_policy
            )
            print(
                f"Dismissed {dismissed} cooldown-blocked proactive action"
                f"{'s' if dismissed != 1 else ''}."
            )
            if bridge:
                bridge.close()
            return

        print(f"\n{len(actions)} pending proactive action{'s' if len(actions) != 1 else ''}")
        presence_count = sum(1 for a in actions if a["source"] == "presence")
        cultivate_count = len(actions) - presence_count
        if presence_count:
            print(f"  {presence_count} from presence")
        if cultivate_count:
            print(f"  {cultivate_count} from cultivate")
        print()

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret,
        )

        drafter = ReplyDrafter(
            api_key=config.anthropic.api_key,
            model=config.synthesis.model,
            timeout=config.timeouts.anthropic_seconds,
            restricted_prompt_behavior=getattr(
                config.curated_sources, "restricted_prompt_behavior", "strict"
            ) if config.curated_sources else "strict",
        )

        my_handle = x_client.username

        # Daily cap enforcement
        max_daily = 999
        if config.proactive and config.proactive.enabled:
            max_daily = config.proactive.max_daily_replies

        quit_requested = False
        completed = 0

        for i, action in enumerate(actions):
            if quit_requested:
                break

            exec_type = action["action_type"]
            tweet_id = action.get("target_tweet_id")
            tweet_text = action.get("target_tweet_text")
            draft = action.get("draft_text")

            print(f"{'─' * 60}")
            print(f"{i + 1}/{len(actions)}  {_format_action_header(action)}")

            if tweet_text:
                print(f"\n  Tweet: \"{truncate(tweet_text, 120)}\"")
            if tweet_id:
                print(f"  Link:  https://x.com/{action['target_handle']}/status/{tweet_id}")

            # Check daily cap before allowing execution
            if exec_type == "reply" and db.count_daily_proactive_posts("reply") >= max_daily:
                print(f"\n  Daily reply cap reached ({max_daily}). Skipping remaining replies.")
                print()
                continue

            cooldown_block_reason = _cooldown_block_reason(
                action, db, cooldown_policy
            )
            if cooldown_block_reason:
                print(f"\n  Approval blocked: {cooldown_block_reason}.")

            if exec_type in ("like", "retweet"):
                if not tweet_id:
                    print("  No tweet_id, skipping.")
                    continue
                print()
                while True:
                    if cooldown_block_reason:
                        sys.stdout.write("  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    else:
                        sys.stdout.write("  [a]pprove  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    sys.stdout.flush()
                    choice = read_char().lower()
                    print(choice)

                    if choice == "o":
                        _open_action_url(action)
                        continue
                    elif choice == "q":
                        quit_requested = True
                    elif choice == "a":
                        if cooldown_block_reason:
                            print(f"  Approval blocked: {cooldown_block_reason}.")
                            continue
                        result = x_client.like(tweet_id) if exec_type == "like" else x_client.retweet(tweet_id)
                        if result.success:
                            _mark_completed(action, db, bridge, tweet_id)
                            print(f"  Done: {exec_type}d tweet {tweet_id}")
                            completed += 1
                        else:
                            print(f"  Error: {result.error}")
                    elif choice == "d":
                        _mark_dismissed(action, db, bridge)
                        print("  Dismissed.")
                    else:
                        print("  Skipped.")
                    break

            elif exec_type == "follow":
                print()
                while True:
                    sys.stdout.write("  [a]pprove  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    sys.stdout.flush()
                    choice = read_char().lower()
                    print(choice)

                    if choice == "o":
                        _open_action_url(action)
                        continue
                    elif choice == "q":
                        quit_requested = True
                    elif choice == "a":
                        user_id = x_client.get_user_id(action["target_handle"])
                        if not user_id:
                            print("  Could not resolve user ID, skipping.")
                        else:
                            result = x_client.follow(user_id)
                            if result.success:
                                _mark_completed(action, db, bridge, "")
                                print(f"  Done: followed @{action['target_handle']}")
                                completed += 1
                            else:
                                print(f"  Error: {result.error}")
                    elif choice == "d":
                        _mark_dismissed(action, db, bridge)
                        print("  Dismissed.")
                    else:
                        print("  Skipped.")
                    break

            elif exec_type in ("reply", "quote_tweet"):
                if not tweet_id:
                    print("  No tweet_id, skipping.")
                    continue

                # Show pre-drafted content or draft live
                if draft:
                    print(f"\n  Draft: \"{draft}\"")
                else:
                    print("\n  Drafting response...")
                    try:
                        result = drafter.draft_proactive(
                            their_tweet=tweet_text or "",
                            their_handle=action["target_handle"],
                            self_handle=my_handle,
                            conversation_context=_parse_platform_metadata(
                                action.get("platform_metadata")
                            ),
                        )
                        draft = result.reply_text
                    except Exception as e:
                        print(f"  Error drafting: {e}")
                        continue
                    print(f"  Draft: \"{draft}\"")

                print()
                while True:
                    if cooldown_block_reason:
                        sys.stdout.write("  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    else:
                        sys.stdout.write("  [a]pprove  [e]dit  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    sys.stdout.flush()
                    choice = read_char().lower()
                    print(choice)

                    if choice == "o":
                        _open_action_url(action)
                        continue
                    elif choice == "q":
                        quit_requested = True
                    elif choice == "a":
                        if cooldown_block_reason:
                            print(f"  Approval blocked: {cooldown_block_reason}.")
                            continue
                        result = _publish_text_action(
                            x_client, exec_type, draft, tweet_id
                        )
                        if result.success:
                            _mark_completed(action, db, bridge, result.tweet_id)
                            print(f"  Posted: {result.url}")
                            completed += 1
                        elif "403" in (result.error or ""):
                            _mark_dismissed(action, db, bridge)
                            print(f"  403 — reply restricted, auto-dismissed.")
                        else:
                            print(f"  Error: {result.error}")
                    elif choice == "e":
                        if cooldown_block_reason:
                            print(f"  Approval blocked: {cooldown_block_reason}.")
                            continue
                        edited = input("  Your text: ").strip()
                        if not edited:
                            print("  Empty, cancelled.")
                            continue
                        elif len(edited) > 280:
                            print(f"  Too long ({len(edited)} chars, max 280).")
                            continue
                        else:
                            result = _publish_text_action(
                                x_client, exec_type, edited, tweet_id
                            )
                            if result.success:
                                _mark_completed(action, db, bridge, result.tweet_id)
                                print(f"  Posted: {result.url}")
                                completed += 1
                            elif "403" in (result.error or ""):
                                _mark_dismissed(action, db, bridge)
                                print(f"  403 — reply restricted, auto-dismissed.")
                            else:
                                print(f"  Error: {result.error}")
                    elif choice == "d":
                        _mark_dismissed(action, db, bridge)
                        print("  Dismissed.")
                    else:
                        print("  Skipped.")
                    break

            else:
                print()
                while True:
                    sys.stdout.write("  [o]pen  [d]ismiss  [s]kip  [q]uit > ")
                    sys.stdout.flush()
                    choice = read_char().lower()
                    print(choice)
                    if choice == "o":
                        _open_action_url(action)
                        continue
                    elif choice == "q":
                        quit_requested = True
                    elif choice == "d":
                        _mark_dismissed(action, db, bridge)
                        print("  Dismissed.")
                    else:
                        print("  Skipped.")
                    break

        print(f"\nDone. {completed} actions completed.")

        if bridge:
            bridge.close()


if __name__ == "__main__":
    main()
