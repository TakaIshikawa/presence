#!/usr/bin/env python3
"""Fetch content from curated external sources."""

import logging
import sys
import time
from pathlib import Path
from types import SimpleNamespace

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import get_embedding_provider
from knowledge.store import KnowledgeStore
from knowledge.ingest import InsightExtractor, ingest_curated_post, ingest_curated_article
from knowledge.curated_accounts import get_active_x_accounts
from knowledge.rss import fetch_feed_entries
from output.x_client import XClient
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)

DEFAULT_MAX_X_ACCOUNTS_PER_RUN = 25
DEFAULT_X_TWEETS_PER_ACCOUNT = 5
DEFAULT_RSS_ENTRIES_PER_SOURCE = 5


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


def _iter_config_sources(config, attr: str) -> list:
    curated_sources = getattr(config, "curated_sources", None)
    sources = getattr(curated_sources, attr, []) if curated_sources else []
    try:
        return list(sources or [])
    except TypeError:
        return []


def _active_feed_sources(config, db) -> list:
    """Return active blog/newsletter sources with optional feed URLs."""
    sources = []
    seen: set[tuple[str, str]] = set()

    for source_type, attr in (("blog", "blogs"), ("newsletter", "newsletters")):
        for source in _iter_config_sources(config, attr):
            identifier = getattr(source, "identifier", "")
            if not identifier:
                continue
            sources.append(source)
            seen.add((source_type, identifier.lower()))

        for row in db.get_active_curated_sources(source_type):
            identifier = row["identifier"]
            key = (source_type, identifier.lower())
            if key in seen:
                continue
            sources.append(SimpleNamespace(
                identifier=identifier,
                name=row["name"] or identifier,
                license=row["license"] or "attribution_required",
                feed_url=row.get("feed_url"),
            ))
            seen.add(key)

    return sources


def fetch_curated_feed_source(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    source,
    limit: int = DEFAULT_RSS_ENTRIES_PER_SOURCE,
) -> int:
    """Fetch a curated RSS/Atom source and ingest new article entries."""
    logger = logging.getLogger(__name__)
    feed_url = getattr(source, "feed_url", None)
    if not feed_url:
        logger.debug("Skipping %s; no feed_url configured", getattr(source, "identifier", "source"))
        return 0

    ingested = 0
    entries = fetch_feed_entries(feed_url, limit=limit)
    author = getattr(source, "name", None) or getattr(source, "identifier", "")
    license_type = getattr(source, "license", "attribution_required")

    for entry in entries:
        if store.exists("curated_article", entry.link):
            logger.debug("Skipping %s (already exists)", entry.link)
            continue

        content = entry.content or entry.summary
        if not content:
            continue

        ingest_curated_article(
            store=store,
            extractor=extractor,
            url=entry.link,
            content=content,
            title=entry.title,
            author=author,
            license_type=license_type,
        )
        ingested += 1

    return ingested


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

        # Fetch from curated X accounts (config + DB-approved)
        block_reason = get_x_api_block_reason(db)
        if block_reason:
            logger.warning(f"X API circuit breaker active; skipping curated X fetch: {block_reason}")
        else:
            x_client = XClient(
                config.x.api_key,
                config.x.api_secret,
                config.x.access_token,
                config.x.access_token_secret
            )

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

        rss_entries_per_source = _int_config(
            getattr(config.curated_sources, "rss_entries_per_source", DEFAULT_RSS_ENTRIES_PER_SOURCE),
            DEFAULT_RSS_ENTRIES_PER_SOURCE,
        )
        feed_sources = _active_feed_sources(config, db)
        logger.info("=== Fetching from curated RSS/Atom sources ===")
        logger.info("Fetching %d feed sources (entries/source=%d)", len(feed_sources), rss_entries_per_source)
        for source in feed_sources:
            try:
                count = fetch_curated_feed_source(
                    store,
                    extractor,
                    source,
                    limit=rss_entries_per_source,
                )
                if count:
                    logger.info("Ingested %d entries from %s", count, source.identifier)
            except Exception as e:
                logger.error("Failed to fetch feed for %s: %s", getattr(source, "identifier", "source"), e)

        logger.info("=== Done ===")


if __name__ == "__main__":
    main()
