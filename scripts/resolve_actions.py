#!/usr/bin/env python3
"""Resolve cultivate's strategic engagement actions into concrete, reviewable items.

Parses execution tags from action descriptions, fetches target tweets,
pre-drafts content for reply/quote_tweet actions, and writes resolved
payloads back to cultivate's actions table.
"""

import logging
import re
import signal
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.x_client import XClient
from engagement.cultivate_bridge import CultivateBridge
from engagement.reply_drafter import ReplyDrafter

logger = logging.getLogger(__name__)

WATCHDOG_TIMEOUT = 300  # 5 minutes

_EXEC_TAG_RE = re.compile(r"^\[(\w+)\]")
_VALID_EXEC_TYPES = {"like", "retweet", "reply", "quote_tweet", "follow"}


def parse_execution_type(description: str) -> str | None:
    """Extract execution type from description's [tag] prefix."""
    m = _EXEC_TAG_RE.match(description)
    if m and m.group(1) in _VALID_EXEC_TYPES:
        return m.group(1)
    return None


def is_already_resolved(payload: dict | None) -> bool:
    """Check if action payload already contains resolution data."""
    return bool(payload and payload.get("execution_type"))


def select_tweet_for_action(
    tweets: list[dict], execution_type: str
) -> dict | None:
    """Select best tweet from fetched list based on execution_type.

    For reply: pick most recent tweet with reply_settings='everyone'.
    For like/retweet/quote_tweet: pick the most recent tweet.
    """
    if not tweets:
        return None

    if execution_type == "reply":
        for tweet in tweets:
            if tweet.get("reply_settings", "everyone") == "everyone":
                return tweet
        return None

    return tweets[0]


def build_resolved_payload(
    execution_type: str,
    tweet: dict | None = None,
    draft: str | None = None,
    x_user_id: str | None = None,
) -> dict:
    """Construct the resolved payload dict."""
    payload = {
        "execution_type": execution_type,
        "resolved_at": datetime.now(timezone.utc).isoformat(),
    }
    if x_user_id:
        payload["x_user_id"] = x_user_id
    if tweet:
        payload["tweet_id"] = tweet["id"]
        payload["tweet_content"] = tweet["text"]
        if tweet.get("reply_settings"):
            payload["reply_settings"] = tweet["reply_settings"]
    if draft:
        payload["draft"] = draft
    return payload


def _get_x_user_id(bridge: CultivateBridge, person_id: str) -> str | None:
    """Look up X user ID from cultivate's people table."""
    row = bridge.conn.execute(
        "SELECT x_user_id FROM people WHERE id = ?", (person_id,)
    ).fetchone()
    return row["x_user_id"] if row and row["x_user_id"] else None


def _resolve_single_action(
    exec_type: str,
    action,
    tweets: list[dict],
    x_user_id: str,
    drafter: ReplyDrafter,
    my_handle: str,
) -> dict | None:
    """Resolve a single action. Returns payload dict or None."""
    if exec_type == "follow":
        return build_resolved_payload(
            execution_type="follow", x_user_id=x_user_id
        )

    tweet = select_tweet_for_action(tweets, exec_type)
    if not tweet:
        return None

    if exec_type in ("like", "retweet"):
        return build_resolved_payload(
            execution_type=exec_type, tweet=tweet, x_user_id=x_user_id
        )

    # reply or quote_tweet: pre-draft content
    draft = drafter.draft(
        our_post="",
        their_reply=tweet["text"],
        their_handle=action.target_handle,
        self_handle=my_handle,
        person_context=action.person_context,
    )

    return build_resolved_payload(
        execution_type=exec_type,
        tweet=tweet,
        draft=draft,
        x_user_id=x_user_id,
    )


def resolve_actions(
    bridge: CultivateBridge,
    x_client: XClient,
    drafter: ReplyDrafter,
    my_handle: str,
    limit: int = 40,
) -> dict:
    """Main resolution logic. Returns stats dict."""
    stats = {"resolved": 0, "skipped": 0, "errors": 0, "total": 0}

    actions = bridge.get_pending_proactive_actions(limit=limit)
    stats["total"] = len(actions)

    if not actions:
        logger.info("No pending actions to resolve")
        return stats

    by_person: dict[str, list] = defaultdict(list)
    for action in actions:
        by_person[action.target_person_id].append(action)

    logger.info(
        f"Resolving {len(actions)} actions across {len(by_person)} people"
    )

    for person_id, person_actions in by_person.items():
        sample = person_actions[0]
        x_user_id = _get_x_user_id(bridge, person_id)

        if not x_user_id:
            logger.warning(
                f"No x_user_id for @{sample.target_handle}, "
                f"skipping {len(person_actions)} actions"
            )
            stats["errors"] += len(person_actions)
            continue

        # Determine if we need tweets for this person
        needs_tweets = any(
            parse_execution_type(a.description)
            in ("like", "retweet", "reply", "quote_tweet")
            and not is_already_resolved(a.payload)
            for a in person_actions
        )

        tweets = []
        if needs_tweets:
            try:
                tweets = x_client.get_user_tweets(x_user_id, count=10)
            except Exception as e:
                logger.warning(
                    f"Failed to fetch tweets for @{sample.target_handle}: {e}"
                )
                stats["errors"] += len(person_actions)
                continue

        for action in person_actions:
            if is_already_resolved(action.payload):
                stats["skipped"] += 1
                continue

            exec_type = parse_execution_type(action.description)
            if not exec_type:
                logger.warning(
                    f"Cannot parse execution tag: {action.description[:60]}"
                )
                stats["errors"] += 1
                continue

            try:
                payload = _resolve_single_action(
                    exec_type=exec_type,
                    action=action,
                    tweets=tweets,
                    x_user_id=x_user_id,
                    drafter=drafter,
                    my_handle=my_handle,
                )

                if payload:
                    bridge.update_action_payload(action.action_id, payload)
                    tweet_info = ""
                    if payload.get("tweet_id"):
                        tweet_info = f" (tweet {payload['tweet_id'][:12]}...)"
                    logger.info(
                        f"  Resolved: {exec_type} -> @{action.target_handle}"
                        + tweet_info
                    )
                    stats["resolved"] += 1
                else:
                    logger.info(
                        f"  No suitable tweet for "
                        f"{exec_type} -> @{action.target_handle}"
                    )
                    stats["errors"] += 1

            except Exception as e:
                logger.error(
                    f"  Error resolving "
                    f"{exec_type} -> @{action.target_handle}: {e}"
                )
                stats["errors"] += 1

    return stats


def _timeout_handler(signum, frame):
    logger.error("WATCHDOG: resolve_actions exceeded timeout, exiting")
    sys.exit(1)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    with script_context() as (config, db):
        if not config.cultivate or not config.cultivate.enabled:
            logger.info("Cultivate integration not enabled")
            return

        bridge = CultivateBridge.try_connect(config.cultivate.db_path)
        if not bridge:
            logger.info("Cultivate DB not found")
            return

        try:
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
            )

            my_handle = x_client.username

            stats = resolve_actions(bridge, x_client, drafter, my_handle)

            logger.info(
                f"Done. Resolved {stats['resolved']}/{stats['total']} "
                f"({stats['skipped']} skipped, {stats['errors']} errors)"
            )
        finally:
            bridge.close()

    update_monitoring("resolve-actions")


if __name__ == "__main__":
    main()
