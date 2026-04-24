#!/usr/bin/env python3
"""Poll GitHub releases without running publishing."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_releases import poll_new_releases
from runner import script_context, update_monitoring

logger = logging.getLogger(__name__)


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def determine_since(db, explicit_since: datetime | None, lookback_minutes: int) -> datetime:
    if explicit_since:
        return explicit_since
    last_poll = db.get_last_github_release_poll_time()
    if last_poll:
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        return last_poll
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


def ingest_github_releases(
    db,
    token: str,
    username: str,
    since: datetime,
    repositories: list[str | dict] | None = None,
    dry_run: bool = False,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
) -> list:
    return poll_new_releases(
        token=token,
        username=username,
        since=since,
        db=db,
        repositories=repositories,
        dry_run=dry_run,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll recently published GitHub releases.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print proposed releases without writing to the database.",
    )
    parser.add_argument(
        "--since",
        help="ISO timestamp to poll from. Defaults to the last release poll or lookback window.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=1440,
        help="Initial lookback window when no previous release poll exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        if not getattr(config.github, "include_releases", False):
            logger.info("GitHub release polling is disabled by github.include_releases")
            return 0

        since = determine_since(db, parse_since(args.since), args.lookback_minutes)
        current_poll_time = datetime.now(timezone.utc)
        repositories = getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub releases since %s", since.isoformat())
        if repositories:
            logger.info("Using %d configured repositories", len(repositories))

        releases = ingest_github_releases(
            db=db,
            token=config.github.token,
            username=config.github.username,
            since=since,
            repositories=repositories,
            dry_run=args.dry_run,
            timeout=config.timeouts.github_seconds,
            redaction_patterns=config.privacy.redaction_patterns,
        )

        for release in releases:
            line = (
                f"{'Would ingest' if args.dry_run else 'Ingested'} "
                f"{release.activity_id} {release.published_at.isoformat()} "
                f"{release.title} {release.url}"
            )
            if args.dry_run:
                print(line)
            logger.info(line)

        if args.dry_run:
            logger.info("Dry run complete. %d new releases found.", len(releases))
        else:
            db.set_last_github_release_poll_time(current_poll_time)
            update_monitoring("poll-github-releases")
            logger.info("Done. Ingested %d GitHub releases.", len(releases))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
