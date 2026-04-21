#!/usr/bin/env python3
"""Process scheduled posts from publish queue at optimal times."""

import signal
import sys
import logging
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

WATCHDOG_TIMEOUT = 300  # 5 minutes

logger = logging.getLogger(__name__)


def _timeout_handler(signum, frame):
    logger.error("WATCHDOG: Publish queue process exceeded 5-minute timeout, exiting")
    sys.exit(1)


# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from output.x_client import XClient, parse_thread_content
from output.bluesky_client import BlueskyClient
from output.cross_poster import CrossPoster


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

        # Get due queue items
        due_items = db.get_due_queue_items(now_iso)

        if not due_items:
            logger.info("No posts due for publishing")
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
                # Parse content into tweets if it's a thread
                if content_type == 'x_thread':
                    tweets = parse_thread_content(content)
                else:
                    tweets = [content]

                # Publish to X if needed
                if platform in ('x', 'all'):
                    if content_type == 'x_thread':
                        result = x_client.post_thread(tweets)
                    else:
                        result = x_client.post(content)

                    if result.success:
                        db.mark_published(content_id, result.url, tweet_id=result.tweet_id)
                        logger.info(f"  Posted to X: {result.url}")
                    else:
                        logger.error(f"  X posting failed: {result.error}")
                        db.upsert_publication_failure(content_id, "x", str(result.error))
                        db.mark_queue_failed(queue_id, f"X: {result.error}")
                        continue  # Skip Bluesky if X failed

                # Cross-post to Bluesky if needed and configured
                if platform in ('bluesky', 'all') and bluesky_client:
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
                        logger.warning(f"  Bluesky posting failed (non-fatal): {bsky_result.error}")
                        db.upsert_publication_failure(
                            content_id,
                            "bluesky",
                            str(bsky_result.error),
                        )

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
