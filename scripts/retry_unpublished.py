#!/usr/bin/env python3
"""Retry posting unpublished content that passed evaluation."""

import sys
import time
import logging
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config import load_config
from runner import script_context, update_monitoring
from storage.db import Database
from output.x_client import XClient

# Rate limiting: seconds between X posts
POST_DELAY_SECONDS = 30

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(message)s'
    )
    config = load_config()

    db = Database(config.paths.database)
    db.connect()

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
            count = db.increment_retry(item['id'])
            if count >= 3:
                logger.warning("Failed: %s — abandoned after 3 attempts", result.error)
            else:
                logger.warning("Failed: %s", result.error)
            if "429" in str(result.error):
                logger.warning("Rate limited, stopping")
                break

    db.close()
    logger.info("Done. %d posts made.", posts_made)


if __name__ == "__main__":
    main()
