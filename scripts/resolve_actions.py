#!/usr/bin/env python3
"""Resolve cultivate's strategic engagement actions into concrete, reviewable items.

Parses execution tags from action descriptions, fetches target tweets,
pre-drafts content for reply/quote_tweet actions, and writes resolved
payloads back to cultivate's actions table.
"""

import logging
import re
import signal
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import anthropic
import tweepy

from runner import script_context, update_monitoring
from output.x_client import XClient
from engagement.cultivate_bridge import CultivateBridge
from engagement.reply_drafter import ReplyDrafter

logger = logging.getLogger(__name__)

WATCHDOG_TIMEOUT = 300  # 5 minutes

_EXEC_TAG_RE = re.compile(r"^\[(\w+)\]")
_VALID_EXEC_TYPES = {"like", "retweet", "reply", "quote_tweet", "follow"}
_REPLY_SETTINGS_ALLOWED = {"everyone", None, ""}
_PUBLIC_METRIC_KEYS = (
    "like_count",
    "retweet_count",
    "reply_count",
    "quote_count",
    "bookmark_count",
    "impression_count",
)


def parse_execution_type(description: str) -> str | None:
    """Extract execution type from description's [tag] prefix."""
    m = _EXEC_TAG_RE.match(description)
    if m and m.group(1) in _VALID_EXEC_TYPES:
        return m.group(1)
    return None


def is_already_resolved(payload: dict | None) -> bool:
    """Check if action payload already contains resolution data."""
    return bool(payload and payload.get("execution_type"))


def _parse_tweet_datetime(value: str | None) -> datetime | None:
    """Parse an ISO timestamp into a timezone-aware datetime."""
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalise_reply_settings(value: str | None) -> str | None:
    """Normalise reply settings for eligibility checks."""
    if value is None:
        return None
    return value.strip() or None


def _tweet_public_metrics(tweet: dict) -> dict:
    """Return public metrics as a plain dict."""
    metrics = tweet.get("public_metrics") or {}
    return metrics if isinstance(metrics, dict) else {}


def _tweet_public_metric_score(tweet: dict) -> int:
    """Collapse public metrics into a deterministic comparison score."""
    metrics = _tweet_public_metrics(tweet)
    score = 0
    for key in _PUBLIC_METRIC_KEYS:
        value = metrics.get(key, 0)
        if isinstance(value, (int, float)):
            score += int(value)
    return score


def _tweet_selection_key(tweet: dict) -> tuple:
    """Build a stable ordering key for tweet comparison."""
    created_at = _parse_tweet_datetime(tweet.get("created_at"))
    timestamp_rank = (
        -created_at.timestamp() if created_at else float("inf")
    )
    metrics = _tweet_public_metrics(tweet)

    return (
        timestamp_rank,
        -_tweet_public_metric_score(tweet),
        -int(metrics.get("like_count", 0) or 0),
        -int(metrics.get("retweet_count", 0) or 0),
        -int(metrics.get("reply_count", 0) or 0),
        -int(metrics.get("quote_count", 0) or 0),
        -int(metrics.get("bookmark_count", 0) or 0),
        -int(metrics.get("impression_count", 0) or 0),
        str(tweet.get("id", "")),
    )


def _is_tweet_eligible(tweet: dict, execution_type: str) -> bool:
    """Check whether a tweet may be used for an action."""
    if execution_type not in ("reply", "quote_tweet"):
        return True

    reply_settings = _normalise_reply_settings(tweet.get("reply_settings"))
    return reply_settings in _REPLY_SETTINGS_ALLOWED


def select_tweet_for_action(
    tweets: list[dict], execution_type: str
) -> tuple[dict | None, dict | None]:
    """Select the best tweet and return selection metadata.

    For reply and quote_tweet: only consider tweets with reply_settings that
    permit public engagement.
    """
    if not tweets:
        return None, None

    eligible_tweets = [
        tweet for tweet in tweets if _is_tweet_eligible(tweet, execution_type)
    ]
    if not eligible_tweets:
        return None, {
            "candidate_count": len(tweets),
            "eligible_count": 0,
            "selection_rationale": (
                f"No eligible tweets for {execution_type}; "
                "reply settings blocked all candidates"
            ),
        }

    chosen = sorted(eligible_tweets, key=_tweet_selection_key)[0]
    chosen_created_at = _parse_tweet_datetime(chosen.get("created_at"))
    chosen_metrics = _tweet_public_metrics(chosen)
    selection_metrics = {
        "candidate_count": len(tweets),
        "eligible_count": len(eligible_tweets),
        "public_metric_score": _tweet_public_metric_score(chosen),
        "created_at": (
            chosen_created_at.isoformat() if chosen_created_at else None
        ),
        "public_metrics": chosen_metrics,
    }
    selection_rationale = (
        f"Selected tweet {chosen['id']} from {len(eligible_tweets)} "
        f"eligible candidate(s) using recency-first scoring"
    )
    return chosen, {
        "candidate_count": len(tweets),
        "eligible_count": len(eligible_tweets),
        "selection_rationale": selection_rationale,
        "selection_metrics": selection_metrics,
    }


def build_resolved_payload(
    execution_type: str,
    tweet: dict | None = None,
    draft: str | None = None,
    x_user_id: str | None = None,
    selection_rationale: str | None = None,
    selection_metrics: dict | None = None,
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
        payload["tweet_created_at"] = tweet.get("created_at")
        payload["tweet_public_metrics"] = _tweet_public_metrics(tweet)
        if execution_type == "quote_tweet":
            payload["quote_tweet_id"] = tweet["id"]
            payload["quoted_tweet_id"] = tweet["id"]
        if tweet.get("reply_settings"):
            payload["reply_settings"] = tweet["reply_settings"]
    if draft:
        payload["draft"] = draft
    if selection_rationale:
        payload["selection_rationale"] = selection_rationale
    if selection_metrics:
        payload["selection_metrics"] = selection_metrics
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

    tweet, selection = select_tweet_for_action(tweets, exec_type)
    if not tweet:
        return None

    if exec_type in ("like", "retweet"):
        return build_resolved_payload(
            execution_type=exec_type,
            tweet=tweet,
            x_user_id=x_user_id,
            selection_rationale=selection["selection_rationale"],
            selection_metrics=selection["selection_metrics"],
        )

    if exec_type == "reply":
        draft = drafter.draft(
            our_post="",
            their_reply=tweet["text"],
            their_handle=action.target_handle,
            self_handle=my_handle,
            person_context=action.person_context,
        )
    else:
        result = drafter.draft_proactive(
            their_tweet=tweet["text"],
            their_handle=action.target_handle,
            self_handle=my_handle,
            person_context=action.person_context,
        )
        draft = result.reply_text

    return build_resolved_payload(
        execution_type=exec_type,
        tweet=tweet,
        draft=draft,
        x_user_id=x_user_id,
        selection_rationale=selection["selection_rationale"],
        selection_metrics=selection["selection_metrics"],
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
            except tweepy.TweepyException as e:
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

            except (tweepy.TweepyException, anthropic.APIError, sqlite3.Error) as e:
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
