#!/usr/bin/env python3
"""Process scheduled posts from publish queue at optimal times."""

import signal
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

WATCHDOG_TIMEOUT = 300  # 5 minutes
DEFAULT_MAX_RETRY_DELAY_MINUTES = 360

logger = logging.getLogger(__name__)


def _requested_platforms(platform: str) -> list[str]:
    if platform == 'all':
        return ['x', 'bluesky']
    return [platform]


def _already_published(item: dict, platform: str) -> bool:
    if platform == 'x':
        return bool(item.get('published'))
    if platform == 'bluesky':
        return bool(item.get('bluesky_uri'))
    return False


def _max_retry_delay_minutes(config) -> int:
    publish_queue_config = getattr(config, "publish_queue", None)
    value = getattr(publish_queue_config, "max_retry_delay_minutes", None)
    if isinstance(value, (int, float)) and value > 0:
        return int(value)
    return DEFAULT_MAX_RETRY_DELAY_MINUTES


def _ready_for_attempt(db, content_id: int, platform: str, now_iso: str) -> bool:
    state = db.get_publication_state(content_id, platform)
    if not state:
        return True
    next_retry_at = state.get("next_retry_at")
    return not (
        state.get("status") == "failed"
        and next_retry_at is not None
        and next_retry_at > now_iso
    )


def _timeout_handler(signum, frame):
    logger.error("WATCHDOG: Publish queue process exceeded 5-minute timeout, exiting")
    sys.exit(1)


def _defer_queue_item(db, queue_id: int, scheduled_at: str) -> None:
    db.conn.execute(
        """UPDATE publish_queue
           SET scheduled_at = ?, status = 'queued', error = NULL
           WHERE id = ?""",
        (scheduled_at, queue_id),
    )
    db.conn.commit()


# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from evaluation.posting_schedule import (
    embargo_windows_from_config,
    is_embargoed,
    next_allowed_slot,
)
from output.bluesky_client import BlueskyClient

try:
    from output.x_client import XClient, parse_thread_content
except ModuleNotFoundError as exc:
    if exc.name != "tweepy":
        raise

    class XClient:
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError(
                "tweepy is required for X publishing"
            ) from exc

    def parse_thread_content(content: str) -> list[str]:
        return [content]

try:
    from output.cross_poster import CrossPoster
except ModuleNotFoundError as exc:
    if exc.name != "tweepy":
        raise

    class CrossPoster:
        def __init__(self, bluesky_client=None):
            self.bluesky_client = bluesky_client

        def adapt_for_bluesky(self, text: str, content_type: str = "x_post") -> str:
            return text


def main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(WATCHDOG_TIMEOUT)

    with script_context() as (config, db):
        # Initialize clients
        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        bluesky_client = None
        if config.bluesky and config.bluesky.enabled:
            bluesky_client = BlueskyClient(
                config.bluesky.handle,
                config.bluesky.app_password
            )

        # Get current time
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        max_retry_delay_minutes = _max_retry_delay_minutes(config)
        embargo_windows = embargo_windows_from_config(config)

        # Get due queue items
        due_items = db.get_due_queue_items(now_iso)

        if not due_items:
            logger.info("No posts due for publishing")
            update_monitoring("run-publish-queue")
            return

        if is_embargoed(now, embargo_windows):
            next_slot = next_allowed_slot(now, embargo_windows)
            next_slot_iso = next_slot.isoformat()
            for item in due_items:
                _defer_queue_item(db, item["id"], next_slot_iso)
            logger.info(
                f"Publishing embargo active; deferred {len(due_items)} posts to {next_slot_iso}"
            )
            update_monitoring("run-publish-queue")
            return

        logger.info(f"Processing {len(due_items)} scheduled posts...")

        for item in due_items:
            queue_id = item['id']
            content_id = item['content_id']
            platform = item['platform']
            content = item['content']
            content_type = item['content_type']
            scheduled_at = item['scheduled_at']

            logger.info(f"Publishing content {content_id} (queued for {scheduled_at})")

            try:
                requested_platforms = _requested_platforms(platform)
                pending_platforms = [
                    p for p in requested_platforms
                    if (
                        not _already_published(item, p)
                        and _ready_for_attempt(db, content_id, p, now_iso)
                    )
                ]

                if not pending_platforms:
                    db.mark_queue_published(queue_id)
                    logger.info(f"  Queue item {queue_id} already completed")
                    continue

                # Parse content into tweets if it's a thread
                if content_type == 'x_thread':
                    tweets = parse_thread_content(content)
                else:
                    tweets = [content]

                platform_errors = []

                # Publish to X if needed
                if 'x' in pending_platforms:
                    if content_type == 'x_thread':
                        result = x_client.post_thread(tweets)
                    else:
                        result = x_client.post(content)

                    if result.success:
                        db.mark_published(content_id, result.url, tweet_id=result.tweet_id)
                        logger.info(f"  Posted to X: {result.url}")
                    else:
                        logger.error(f"  X posting failed: {result.error}")
                        db.upsert_publication_failure(
                            content_id,
                            "x",
                            str(result.error),
                            max_retry_delay_minutes=max_retry_delay_minutes,
                        )
                        platform_errors.append(f"X: {result.error}")

                # Cross-post to Bluesky if needed and configured
                if 'bluesky' in pending_platforms:
                    if bluesky_client:
                        cross_poster = CrossPoster(bluesky_client=bluesky_client)
                        bsky_tweets = [cross_poster.adapt_for_bluesky(t, content_type) for t in tweets]

                        if content_type == 'x_thread':
                            bsky_result = bluesky_client.post_thread(bsky_tweets)
                        else:
                            bsky_result = bluesky_client.post(bsky_tweets[0])

                        if bsky_result.success:
                            db.mark_published_bluesky(
                                content_id,
                                bsky_result.uri,
                                url=getattr(bsky_result, "url", None),
                            )
                            logger.info(f"  Posted to Bluesky: {bsky_result.url}")
                        else:
                            logger.error(f"  Bluesky posting failed: {bsky_result.error}")
                            db.upsert_publication_failure(
                                content_id,
                                "bluesky",
                                str(bsky_result.error),
                                max_retry_delay_minutes=max_retry_delay_minutes,
                            )
                            platform_errors.append(f"Bluesky: {bsky_result.error}")
                    else:
                        logger.error("  Bluesky posting failed: client not configured")
                        db.upsert_publication_failure(
                            content_id,
                            "bluesky",
                            "client not configured",
                            max_retry_delay_minutes=max_retry_delay_minutes,
                        )
                        platform_errors.append("Bluesky: client not configured")

                if platform_errors:
                    db.mark_queue_failed(queue_id, "; ".join(platform_errors))
                    logger.info(f"  Queue item {queue_id} failed for: {', '.join(platform_errors)}")
                else:
                    # Mark queue item as published
                    db.mark_queue_published(queue_id)
                    logger.info(f"  Queue item {queue_id} completed")

            except (sqlite3.Error, KeyError, IndexError, AttributeError, TypeError, ValueError) as e:
                logger.error(f"  Unexpected error publishing content {content_id}: {e}")
                db.mark_queue_failed(queue_id, str(e))

    update_monitoring("run-publish-queue")
    logger.info("Done")


if __name__ == "__main__":
    main()
