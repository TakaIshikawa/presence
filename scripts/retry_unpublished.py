#!/usr/bin/env python3
"""Retry posting unpublished content that passed evaluation."""

import sys
import time
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from output.x_client import XClient
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)
from output.publish_errors import classify_publish_error, normalize_error_category

# Rate limiting: seconds between X posts
POST_DELAY_SECONDS = 30

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )

    with script_context() as (config, db):
        block_reason = get_x_api_block_reason(db)
        if block_reason:
            logger.warning(f"X API circuit breaker active; skipping retry: {block_reason}")
            return

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        # Get unpublished content that passed threshold
        min_score = config.synthesis.eval_threshold * 10
        unpublished = db.get_unpublished_content("x_post", min_score)

        logger.info("Found %d unpublished posts to retry", len(unpublished))

        posts_made = 0
        for item in unpublished:
            retry_num = (item.get("retry_count") or 0) + 1
            logger.info("Retrying (attempt %d/3): %s", retry_num, item['content'][:60])

            # Rate limiting
            if posts_made > 0:
                logger.info("Waiting %ds", POST_DELAY_SECONDS)
                time.sleep(POST_DELAY_SECONDS)

            result = x_client.post(item['content'])
            if result.success:
                db.mark_published(item['id'], result.url, tweet_id=result.tweet_id)
                logger.info("Posted: %s", result.url)
                posts_made += 1
            else:
                category = getattr(result, "error_category", None)
                if category:
                    category = normalize_error_category(category)
                else:
                    category = classify_publish_error(result.error, platform="x")
                db.upsert_publication_failure(
                    item["id"],
                    "x",
                    str(result.error),
                    error_category=category,
                )
                if mark_x_api_blocked_if_needed(db, result.error):
                    logger.warning("X API blocked; stopping retries")
                    break
                count = db.increment_retry(item['id'])
                if count >= 3:
                    logger.warning("Failed: %s -- abandoned after 3 attempts", result.error)
                else:
                    logger.warning("Failed: %s", result.error)
                if "429" in str(result.error):
                    logger.warning("Rate limited, stopping")
                    break

    logger.info("Done. %d posts made.", posts_made)


if __name__ == "__main__":
    main()
