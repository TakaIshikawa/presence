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
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)

DEFAULT_MAX_X_ACCOUNTS_PER_RUN = 25
DEFAULT_X_TWEETS_PER_ACCOUNT = 5


def _int_config(value, default: int) -> int:
    return value if isinstance(value, int) and value > 0 else default


def _last_x_error(x_client) -> str | None:
    error = getattr(x_client, "last_error", None)
    return error if isinstance(error, str) and error else None


def _cached_user_id(db, x_client, username: str) -> str | None:
    normalized = username.lstrip("@").lower()
    key = f"x_user_id:{normalized}"
    cached = db.get_meta(key)
    if cached:
        return cached

    user_id = x_client.get_user_id(normalized)
    if user_id:
        db.set_meta(key, user_id)
    return user_id


def fetch_user_tweets(x_client, username: str, limit: int = 10, db=None) -> list[dict]:
    """Fetch recent tweets from a user using XClient methods."""
    logger = logging.getLogger(__name__)
    try:
        user_id = (
            _cached_user_id(db, x_client, username)
            if db is not None
            else x_client.get_user_id(username)
        )
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
        block_reason = get_x_api_block_reason(db)
        if block_reason:
            logger.warning(f"X API circuit breaker active; skipping curated fetch: {block_reason}")
            return

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
        max_accounts = _int_config(
            getattr(config.curated_sources, "max_x_accounts_per_run", DEFAULT_MAX_X_ACCOUNTS_PER_RUN),
            DEFAULT_MAX_X_ACCOUNTS_PER_RUN,
        )
        tweets_per_account = _int_config(
            getattr(config.curated_sources, "x_tweets_per_account", DEFAULT_X_TWEETS_PER_ACCOUNT),
            DEFAULT_X_TWEETS_PER_ACCOUNT,
        )
        accounts = get_active_x_accounts(
            config,
            db,
            limit=max_accounts,
            cursor_key="fetch_curated_x_account_cursor",
        )
        logger.info("=== Fetching from curated X accounts ===")
        logger.info(
            "Fetching %d accounts (cap=%d, tweets/account=%d)",
            len(accounts),
            max_accounts,
            tweets_per_account,
        )
        for account in accounts:
            logger.info(f"Fetching @{account.identifier}...")
            tweets = fetch_user_tweets(
                x_client,
                account.identifier,
                limit=tweets_per_account,
                db=db,
            )
            if mark_x_api_blocked_if_needed(db, _last_x_error(x_client)):
                logger.warning("X API blocked while fetching curated accounts; stopping")
                break

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
