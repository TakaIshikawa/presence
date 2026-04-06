#!/usr/bin/env python3
"""Interactive review of cultivate's proactive engagement recommendations."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from output.x_client import XClient
from engagement.cultivate_bridge import CultivateBridge
from engagement.reply_drafter import ReplyDrafter
from review_helpers import truncate, read_char


def main():
    config = load_config()

    if not config.cultivate or not config.cultivate.enabled:
        print("Cultivate integration not enabled in config.")
        return

    if not config.cultivate.proactive_review:
        print("Proactive review disabled in config.")
        return

    bridge = CultivateBridge.try_connect(config.cultivate.db_path)
    if not bridge:
        print("Cultivate DB not found.")
        return

    actions = bridge.get_pending_proactive_actions(limit=20)
    if not actions:
        print("No pending proactive actions.")
        bridge.close()
        return

    print(f"\n{len(actions)} pending proactive action{'s' if len(actions) != 1 else ''}\n")

    x_client = XClient(
        config.x.api_key,
        config.x.api_secret,
        config.x.access_token,
        config.x.access_token_secret,
    )

    drafter = ReplyDrafter(
        api_key=config.anthropic.api_key,
        model=config.synthesis.model,
    )

    me = x_client.client.get_me()
    my_handle = me.data.username

    quit_requested = False
    completed = 0

    for i, action in enumerate(actions):
        if quit_requested:
            break

        ctx = action.person_context
        print(f"{'─' * 60}")
        print(f"{i + 1}/{len(actions)}  {_format_action_context(action)}")

        print(f"\n  Reason: {action.description}")

        # Show payload content if available
        tweet_content = None
        tweet_id = None
        if action.payload:
            tweet_id = action.payload.get("tweet_id")
            tweet_content = action.payload.get("tweet_content")
            if tweet_content:
                print(f"  Tweet:  \"{truncate(tweet_content, 120)}\"")
            if tweet_id:
                print(f"  Link:   https://x.com/{action.target_handle}/status/{tweet_id}")
        print()

        while True:
            if action.action_type in ("like", "retweet"):
                if not tweet_id:
                    print("  No tweet_id in payload, skipping.")
                    break
                sys.stdout.write("  [a]pprove  [d]ismiss  [s]kip  [q]uit > ")
                sys.stdout.flush()
                choice = read_char().lower()
                print(choice)

                if choice == "q":
                    quit_requested = True
                    break
                elif choice == "a":
                    if action.action_type == "like":
                        result = x_client.like(tweet_id)
                    else:
                        result = x_client.retweet(tweet_id)
                    if result.success:
                        bridge.mark_action_completed(action.action_id)
                        print(f"  Done: {action.action_type}d tweet {tweet_id}")
                        completed += 1
                    else:
                        print(f"  Error: {result.error}")
                    break
                elif choice == "d":
                    bridge.mark_action_dismissed(action.action_id)
                    print("  Dismissed.")
                    break
                else:
                    print("  Skipped.")
                    break

            elif action.action_type == "follow":
                sys.stdout.write("  [a]pprove  [d]ismiss  [s]kip  [q]uit > ")
                sys.stdout.flush()
                choice = read_char().lower()
                print(choice)

                if choice == "q":
                    quit_requested = True
                    break
                elif choice == "a":
                    # Need X user ID — look up from cultivate's people table
                    person_x_id = _get_x_user_id(bridge, action.target_person_id)
                    if not person_x_id:
                        print("  No X user ID found for this person, skipping.")
                        break
                    result = x_client.follow(person_x_id)
                    if result.success:
                        bridge.mark_action_completed(action.action_id)
                        print(f"  Done: followed @{action.target_handle}")
                        completed += 1
                    else:
                        print(f"  Error: {result.error}")
                    break
                elif choice == "d":
                    bridge.mark_action_dismissed(action.action_id)
                    print("  Dismissed.")
                    break
                else:
                    print("  Skipped.")
                    break

            elif action.action_type in ("reply", "quote_tweet"):
                if not tweet_id:
                    print("  No tweet_id in payload, skipping.")
                    break

                # Draft a reply/quote
                post_text = tweet_content or ""
                print("  Drafting response...")
                try:
                    draft = drafter.draft(
                        our_post="",
                        their_reply=post_text,
                        their_handle=action.target_handle,
                        self_handle=my_handle,
                        person_context=ctx,
                    )
                except Exception as e:
                    print(f"  Error drafting: {e}")
                    break

                print(f"  Draft: \"{draft}\"")
                print()
                sys.stdout.write("  [a]pprove  [e]dit  [d]ismiss  [s]kip  [q]uit > ")
                sys.stdout.flush()
                choice = read_char().lower()
                print(choice)

                if choice == "q":
                    quit_requested = True
                    break
                elif choice == "a":
                    if action.action_type == "reply":
                        result = x_client.reply(draft, tweet_id)
                    else:
                        result = x_client.quote_tweet(draft, tweet_id)
                    if result.success:
                        bridge.mark_action_completed(action.action_id)
                        print(f"  Posted: {result.url}")
                        completed += 1
                    else:
                        print(f"  Error: {result.error}")
                    break
                elif choice == "e":
                    edited = input("  Your text: ").strip()
                    if not edited:
                        print("  Empty, cancelled.")
                        continue
                    if len(edited) > 280:
                        print(f"  Too long ({len(edited)} chars, max 280). Try again.")
                        continue
                    if action.action_type == "reply":
                        result = x_client.reply(edited, tweet_id)
                    else:
                        result = x_client.quote_tweet(edited, tweet_id)
                    if result.success:
                        bridge.mark_action_completed(action.action_id)
                        print(f"  Posted: {result.url}")
                        completed += 1
                    else:
                        print(f"  Error: {result.error}")
                    break
                elif choice == "d":
                    bridge.mark_action_dismissed(action.action_id)
                    print("  Dismissed.")
                    break
                else:
                    print("  Skipped.")
                    break

            else:
                # Unknown action type — show and let user decide
                sys.stdout.write("  [d]ismiss  [s]kip  [q]uit > ")
                sys.stdout.flush()
                choice = read_char().lower()
                print(choice)
                if choice == "q":
                    quit_requested = True
                elif choice == "d":
                    bridge.mark_action_dismissed(action.action_id)
                    print("  Dismissed.")
                break

    print(f"\nDone. {completed} actions completed.")
    bridge.close()


def _format_action_context(action) -> str:
    """Format action header with relationship context lines."""
    lines = [f"{action.action_type.upper()} -> @{action.target_handle}"]
    ctx = action.person_context
    if ctx:
        parts = []
        if ctx.engagement_stage is not None:
            parts.append(f"{ctx.stage_name} (stage {ctx.engagement_stage})")
        if ctx.dunbar_tier is not None:
            parts.append(f"{ctx.tier_name} (tier {ctx.dunbar_tier})")
        if ctx.relationship_strength is not None:
            parts.append(f"strength: {ctx.relationship_strength:.2f}")
        if parts:
            lines.append(f"     [{' | '.join(parts)}]")
        if ctx.bio:
            lines.append(f"     Bio: {truncate(ctx.bio, 100)}")
    return "\n".join(lines)


def _get_x_user_id(bridge, person_id: str) -> str | None:
    """Look up X user ID from cultivate's people table."""
    row = bridge.conn.execute(
        "SELECT x_user_id FROM people WHERE id = ?", (person_id,)
    ).fetchone()
    return row["x_user_id"] if row and row["x_user_id"] else None


if __name__ == "__main__":
    main()
