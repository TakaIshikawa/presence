#!/usr/bin/env python3
"""Fetch content from curated external sources."""

import logging
import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import get_embedding_provider
from knowledge.store import KnowledgeStore
from knowledge.ingest import InsightExtractor, ingest_curated_post
from knowledge.curated_accounts import get_active_x_accounts
from output.x_client import XClient


def fetch_user_tweets(x_client, username: str, limit: int = 10) -> list[dict]:
    """Fetch recent tweets from a user using XClient methods."""
    logger = logging.getLogger(__name__)
    try:
        user_id = x_client.get_user_id(username)
        if not user_id:
            logger.error(f"User @{username} not found")
            return []

        tweets = x_client.get_user_tweets(user_id, count=limit)
        return [
            {
                "id": t["id"],
                "text": t["text"],
                "url": f"https://x.com/{username}/status/{t['id']}",
            }
            for t in tweets
        ]
    except Exception as e:
        logger.error(f"Error fetching tweets for @{username}: {e}")
        return []


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    with script_context() as (config, db):
        if not config.embeddings:
            logger.error("embeddings not configured")
            sys.exit(1)

        if not config.curated_sources:
            logger.info("No curated sources configured")
            sys.exit(0)

        # Initialize
        embedder = get_embedding_provider(
            config.embeddings.provider,
            config.embeddings.api_key,
            config.embeddings.model
        )

        store = KnowledgeStore(db.conn, embedder)
        extractor = InsightExtractor(config.anthropic.api_key, config.synthesis.model)

        x_client = XClient(
            config.x.api_key,
            config.x.api_secret,
            config.x.access_token,
            config.x.access_token_secret
        )

        # Fetch from curated X accounts (config + DB-approved)
        accounts = get_active_x_accounts(config, db)
        logger.info("=== Fetching from curated X accounts ===")
        for account in accounts:
            logger.info(f"Fetching @{account.identifier}...")
            tweets = fetch_user_tweets(x_client, account.identifier, limit=5)

            for tweet in tweets:
                if store.exists("curated_x", tweet["id"]):
                    logger.debug(f"Skipping {tweet['id']} (already exists)")
                    continue

                # Skip retweets and very short tweets
                if tweet["text"].startswith("RT @") or len(tweet["text"]) < 50:
                    continue

                logger.info(f"Processing tweet {tweet['id']}...")
                try:
                    ingest_curated_post(
                        store=store,
                        extractor=extractor,
                        post_id=tweet["id"],
                        content=tweet["text"],
                        url=tweet["url"],
                        author=account.identifier,
                        license_type=account.license
                    )
                    logger.info(f"Ingested tweet {tweet['id']}")
                    time.sleep(1)  # Rate limiting
                except Exception as e:
                    logger.error(f"Failed to ingest tweet {tweet['id']}: {e}")

        logger.info("=== Done ===")


if __name__ == "__main__":
    main()
