#!/usr/bin/env python3
"""Fetch content from curated external sources."""

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from knowledge.embeddings import get_embedding_provider
from knowledge.store import KnowledgeStore
from knowledge.ingest import (
    InsightExtractor,
    ingest_curated_article,
    ingest_curated_newsletter,
    ingest_curated_post,
)
from knowledge.curated_accounts import get_active_x_accounts
from knowledge.rss import discover_feed_candidates, fetch_feed
from output.x_client import XClient
from output.x_api_guard import (
    get_x_api_block_reason,
    mark_x_api_blocked_if_needed,
)
from output.api_rate_guard import should_skip_optional_api_call

DEFAULT_MAX_X_ACCOUNTS_PER_RUN = 25
DEFAULT_X_TWEETS_PER_ACCOUNT = 5
DEFAULT_RSS_ENTRIES_PER_SOURCE = 5
DEFAULT_FEED_AUTODISCOVERY_ENABLED = True
DEFAULT_FEED_AUTODISCOVERY_TIMEOUT_SECONDS = 20.0
DEFAULT_SOURCE_FAILURE_THRESHOLD = 3
DEFAULT_SOURCE_COOLDOWN_HOURS = 24


def _int_config(value, default: int) -> int:
    return value if isinstance(value, int) and value > 0 else default


def _nonnegative_int_config(value, default: int) -> int:
    return value if isinstance(value, int) and value >= 0 else default


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
            if _last_x_error(x_client) is None and hasattr(x_client, "last_error"):
                x_client.last_error = f"User @{username} not found"
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


def _curated_source_row(db, source_type: str, identifier: str) -> dict | None:
    getter = getattr(db, "get_curated_source", None)
    if not callable(getter):
        return None
    row = getter(source_type, identifier)
    return row if isinstance(row, dict) else None


def _source_error(exc: Exception) -> str:
    return f"{exc.__class__.__name__}: {exc}"


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _quarantine_remaining(row: dict | None, failure_threshold: int, cooldown_hours: int) -> timedelta | None:
    if not row or failure_threshold <= 0 or cooldown_hours <= 0:
        return None
    failures = row.get("consecutive_failures") or 0
    if failures < failure_threshold:
        return None
    last_failure = _parse_time(row.get("last_failure_at"))
    if last_failure is None:
        return timedelta(hours=cooldown_hours)
    remaining = last_failure + timedelta(hours=cooldown_hours) - datetime.now(timezone.utc)
    return remaining if remaining.total_seconds() > 0 else None


def _is_quarantined(row: dict | None, failure_threshold: int, cooldown_hours: int) -> bool:
    return _quarantine_remaining(row, failure_threshold, cooldown_hours) is not None


def _record_source_success(db, source_type: str, identifier: str, status: str = "success") -> None:
    recorder = getattr(db, "record_curated_source_fetch_success", None)
    if callable(recorder):
        recorder(source_type, identifier, status=status)


def _record_source_failure(db, source_type: str, identifier: str, error: str) -> None:
    recorder = getattr(db, "record_curated_source_fetch_failure", None)
    if callable(recorder):
        recorder(source_type, identifier, error)


def _record_source_skipped(db, source_type: str, identifier: str, status: str = "quarantined") -> None:
    recorder = getattr(db, "record_curated_source_fetch_skipped", None)
    if callable(recorder):
        recorder(source_type, identifier, status=status)


def _source_homepage_url(source) -> str:
    homepage_url = getattr(source, "homepage_url", None)
    if homepage_url:
        return homepage_url
    identifier = getattr(source, "identifier", "")
    if identifier.startswith(("http://", "https://")):
        return identifier
    return f"https://{identifier}" if identifier else ""


def _cache_discovered_feed_url(db, source_type: str, identifier: str, feed_url: str) -> None:
    updater = getattr(db, "update_curated_source_feed_url", None)
    if callable(updater):
        updater(source_type, identifier, feed_url)
        return
    conn = getattr(db, "conn", None)
    if conn is not None:
        conn.execute(
            "UPDATE curated_sources SET feed_url = ? WHERE source_type = ? AND identifier = ?",
            (feed_url, source_type, identifier),
        )
        conn.commit()


def _active_feed_sources(config, db) -> list:
    """Return active blog/newsletter sources with optional feed URLs."""
    sources = []
    seen: set[tuple[str, str]] = set()

    for source_type, attr in (("blog", "blogs"), ("newsletter", "newsletters")):
        for source in _iter_config_sources(config, attr):
            identifier = getattr(source, "identifier", "")
            if not identifier:
                continue
            row = _curated_source_row(db, source_type, identifier)
            sources.append(SimpleNamespace(
                source_type=source_type,
                identifier=identifier,
                name=getattr(source, "name", None) or identifier,
                license=getattr(source, "license", "attribution_required"),
                feed_url=getattr(source, "feed_url", None) or (row.get("feed_url") if row else None),
                homepage_url=getattr(source, "homepage_url", None),
                feed_etag=row.get("feed_etag") if row else None,
                feed_last_modified=row.get("feed_last_modified") if row else None,
                health=row,
            ))
            seen.add((source_type, identifier.lower()))

        for row in db.get_active_curated_sources(source_type):
            identifier = row["identifier"]
            key = (source_type, identifier.lower())
            if key in seen:
                continue
            sources.append(SimpleNamespace(
                source_type=source_type,
                identifier=identifier,
                name=row["name"] or identifier,
                license=row["license"] or "attribution_required",
                feed_url=row.get("feed_url"),
                homepage_url=row.get("homepage_url"),
                feed_etag=row.get("feed_etag"),
                feed_last_modified=row.get("feed_last_modified"),
                health=row,
            ))
            seen.add(key)

    return sources


def fetch_curated_feed_source(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    source,
    db=None,
    limit: int = DEFAULT_RSS_ENTRIES_PER_SOURCE,
    failure_threshold: int = DEFAULT_SOURCE_FAILURE_THRESHOLD,
    cooldown_hours: int = DEFAULT_SOURCE_COOLDOWN_HOURS,
    autodiscovery_enabled: bool = False,
    autodiscovery_timeout: float = DEFAULT_FEED_AUTODISCOVERY_TIMEOUT_SECONDS,
    dry_run: bool = False,
) -> int:
    """Fetch a curated RSS/Atom source and ingest new article/newsletter entries."""
    logger = logging.getLogger(__name__)
    feed_url = getattr(source, "feed_url", None)

    source_type = getattr(source, "source_type", None)
    identifier = getattr(source, "identifier", "")
    row = getattr(source, "health", None)
    if row is None and db is not None and source_type and identifier:
        row = _curated_source_row(db, source_type, identifier)
    remaining = _quarantine_remaining(row, failure_threshold, cooldown_hours)
    if remaining is not None:
        logger.warning(
            "Skipping %s; source health cooldown active for %.1f more hours",
            identifier or feed_url,
            max(0.0, remaining.total_seconds() / 3600),
        )
        if db is not None and source_type and identifier and not dry_run:
            _record_source_skipped(db, source_type, identifier)
        return 0

    if not feed_url:
        if not autodiscovery_enabled:
            logger.debug("Skipping %s; no feed_url configured", identifier or "source")
            return 0

        homepage_url = _source_homepage_url(source)
        if not homepage_url:
            logger.debug("Skipping %s; no homepage URL available for feed autodiscovery", identifier or "source")
            return 0
        try:
            candidates = discover_feed_candidates(homepage_url, timeout=autodiscovery_timeout)
        except Exception as exc:
            if db is not None and source_type and identifier and not dry_run:
                _record_source_failure(db, source_type, identifier, _source_error(exc))
            raise

        if not candidates:
            logger.info("No feed candidates discovered for %s", identifier or homepage_url)
            if db is not None and source_type and identifier and not dry_run:
                _record_source_skipped(db, source_type, identifier, status="no_feed_discovered")
            return 0

        feed_url = candidates[0].url
        logger.info("Discovered feed for %s: %s", identifier or homepage_url, feed_url)
        try:
            setattr(source, "feed_url", feed_url)
        except Exception:
            pass
        if db is not None and source_type and identifier and not dry_run:
            _cache_discovered_feed_url(db, source_type, identifier, feed_url)

    try:
        result = fetch_feed(
            feed_url,
            limit=limit,
            etag=getattr(source, "feed_etag", None),
            last_modified=getattr(source, "feed_last_modified", None),
        )
    except Exception as exc:
        if db is not None and source_type and identifier and not dry_run:
            _record_source_failure(db, source_type, identifier, _source_error(exc))
        raise
    update_feed_cache = getattr(db, "update_curated_source_feed_cache", None)
    if db is not None and source_type and identifier and callable(update_feed_cache) and not dry_run:
        update_feed_cache(
            source_type,
            identifier,
            result.etag,
            result.last_modified,
        )
    if result.not_modified:
        logger.debug("Skipping %s; feed not modified", identifier or feed_url)
        if db is not None and source_type and identifier and not dry_run:
            _record_source_success(db, source_type, identifier, status="not_modified")
        return 0

    ingested = 0
    author = getattr(source, "name", None) or getattr(source, "identifier", "")
    license_type = getattr(source, "license", "attribution_required")
    knowledge_source_type = (
        "curated_newsletter" if source_type == "newsletter" else "curated_article"
    )
    ingest_entry = (
        ingest_curated_newsletter if source_type == "newsletter" else ingest_curated_article
    )

    try:
        for entry in result.entries:
            if store.exists(knowledge_source_type, entry.link):
                logger.debug("Skipping %s (already exists)", entry.link)
                continue

            content = entry.content or entry.summary
            if not content:
                continue

            if dry_run:
                logger.info(
                    "[dry-run] Would ingest %s entry from %s: %s (%s)",
                    knowledge_source_type,
                    identifier or feed_url,
                    entry.title,
                    entry.link,
                )
                ingested += 1
                continue

            ingest_entry(
                store=store,
                extractor=extractor,
                url=entry.link,
                content=content,
                title=entry.title,
                author=author,
                license_type=license_type,
                published_at=entry.published_at,
            )
            ingested += 1
    except Exception as exc:
        if db is not None and source_type and identifier and not dry_run:
            _record_source_failure(db, source_type, identifier, _source_error(exc))
        raise

    if db is not None and source_type and identifier and not dry_run:
        _record_source_success(db, source_type, identifier)
    return ingested


def _fetch_account_with_health(
    x_client,
    account,
    db,
    limit: int,
    failure_threshold: int,
    cooldown_hours: int,
) -> list[dict]:
    logger = logging.getLogger(__name__)
    identifier = account.identifier
    row = _curated_source_row(db, "x_account", identifier)
    remaining = _quarantine_remaining(row, failure_threshold, cooldown_hours)
    if remaining is not None:
        logger.warning(
            "Skipping @%s; source health cooldown active for %.1f more hours",
            identifier,
            max(0.0, remaining.total_seconds() / 3600),
        )
        _record_source_skipped(db, "x_account", identifier)
        return []

    try:
        tweets = fetch_user_tweets(x_client, identifier, limit=limit, db=db)
    except Exception as exc:
        _record_source_failure(db, "x_account", identifier, _source_error(exc))
        raise

    error = _last_x_error(x_client)
    if error:
        _record_source_failure(db, "x_account", identifier, error)
    else:
        _record_source_success(db, "x_account", identifier)
    return tweets


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch configured sources and report new entries without writing knowledge rows.",
    )
    args, _unknown = parser.parse_known_args(argv)
    return args


def main(argv: list[str] | None = None):
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)
    dry_run = bool(args.dry_run)

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
        elif should_skip_optional_api_call(
            config,
            db,
            "x",
            operation="curated X account fetch",
            logger=logger,
        ):
            pass
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
            failure_threshold = _int_config(
                getattr(config.curated_sources, "source_failure_threshold", DEFAULT_SOURCE_FAILURE_THRESHOLD),
                DEFAULT_SOURCE_FAILURE_THRESHOLD,
            )
            cooldown_hours = _nonnegative_int_config(
                getattr(config.curated_sources, "source_cooldown_hours", DEFAULT_SOURCE_COOLDOWN_HOURS),
                DEFAULT_SOURCE_COOLDOWN_HOURS,
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
                tweets = _fetch_account_with_health(
                    x_client,
                    account,
                    db,
                    tweets_per_account,
                    failure_threshold,
                    cooldown_hours,
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
                        if dry_run:
                            logger.info(
                                "[dry-run] Would ingest curated_x entry from @%s: %s (%s)",
                                account.identifier,
                                tweet["id"],
                                tweet["url"],
                            )
                        else:
                            ingest_curated_post(
                                store=store,
                                extractor=extractor,
                                post_id=tweet["id"],
                                content=tweet["text"],
                                url=tweet["url"],
                                author=account.identifier,
                                license_type=account.license,
                                published_at=tweet.get("created_at"),
                            )
                            logger.info(f"Ingested tweet {tweet['id']}")
                            time.sleep(1)  # Rate limiting
                    except Exception as e:
                        logger.error(f"Failed to ingest tweet {tweet['id']}: {e}")

        rss_entries_per_source = _int_config(
            getattr(config.curated_sources, "rss_entries_per_source", DEFAULT_RSS_ENTRIES_PER_SOURCE),
            DEFAULT_RSS_ENTRIES_PER_SOURCE,
        )
        failure_threshold = _int_config(
            getattr(config.curated_sources, "source_failure_threshold", DEFAULT_SOURCE_FAILURE_THRESHOLD),
            DEFAULT_SOURCE_FAILURE_THRESHOLD,
        )
        cooldown_hours = _nonnegative_int_config(
            getattr(config.curated_sources, "source_cooldown_hours", DEFAULT_SOURCE_COOLDOWN_HOURS),
            DEFAULT_SOURCE_COOLDOWN_HOURS,
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
                    db=db,
                    limit=rss_entries_per_source,
                    failure_threshold=failure_threshold,
                    cooldown_hours=cooldown_hours,
                    autodiscovery_enabled=getattr(
                        config.curated_sources,
                        "feed_autodiscovery_enabled",
                        DEFAULT_FEED_AUTODISCOVERY_ENABLED,
                    ),
                    autodiscovery_timeout=getattr(
                        config.curated_sources,
                        "feed_autodiscovery_timeout_seconds",
                        DEFAULT_FEED_AUTODISCOVERY_TIMEOUT_SECONDS,
                    ),
                    dry_run=dry_run,
                )
                if count:
                    action = "Would ingest" if dry_run else "Ingested"
                    logger.info("%s %d entries from %s", action, count, source.identifier)
            except Exception as e:
                logger.error("Failed to fetch feed for %s: %s", getattr(source, "identifier", "source"), e)

        logger.info("=== Done ===")


if __name__ == "__main__":
    main()
