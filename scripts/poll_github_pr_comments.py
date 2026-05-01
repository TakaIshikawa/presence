#!/usr/bin/env python3
"""Poll GitHub pull request review and conversation comments."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_pr_comments import poll_new_pr_comments
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
    method = getattr(db, "get_last_github_pr_comment_poll_time", None)
    last_poll = method() if method else None
    if last_poll:
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        return last_poll
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


def ingest_github_pr_comments(
    db,
    token: str,
    username: str,
    since: datetime,
    repositories: list[str | dict] | None = None,
    dry_run: bool = False,
    limit: int = 100,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
):
    return poll_new_pr_comments(
        token=token,
        username=username,
        since=since,
        db=db,
        repositories=repositories,
        dry_run=dry_run,
        limit_per_repo=limit,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll recently updated GitHub pull request comments."
    )
    parser.add_argument(
        "--repo",
        action="append",
        help="Repository to poll as owner/name. May be passed more than once.",
    )
    parser.add_argument(
        "--since",
        help="ISO timestamp to poll from. Defaults to the last PR comment poll or lookback window.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum comments to inspect per repository and comment kind (default: 100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print proposed PR comments without writing to the database.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=1440,
        help="Initial lookback window when no previous PR comment poll exists.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        since = determine_since(db, parse_since(args.since), args.lookback_minutes)
        current_poll_time = datetime.now(timezone.utc)
        repositories = args.repo or getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub PR comments since %s", since.isoformat())
        if repositories:
            logger.info("Using %d repositories", len(repositories))

        result = ingest_github_pr_comments(
            db=db,
            token=config.github.token,
            username=config.github.username,
            since=since,
            repositories=repositories,
            dry_run=args.dry_run,
            limit=args.limit,
            timeout=config.timeouts.github_seconds,
            redaction_patterns=config.privacy.redaction_patterns,
        )

        for comment in result.comments:
            line = (
                f"{'Would ingest' if args.dry_run else 'Ingested'} "
                f"{comment.activity_id} pr=#{comment.pr_number} "
                f"{comment.updated_at.isoformat()} {comment.url}"
            )
            if args.dry_run:
                print(line)
            logger.info(line)

        summary = (
            f"fetched={result.fetched_count} skipped={result.skipped_count} "
            f"duplicates={result.duplicate_count} new={len(result.comments)}"
        )
        if args.dry_run:
            print(f"Dry run summary: {summary}")
            logger.info("Dry run complete. %s", summary)
        else:
            method = getattr(db, "set_last_github_pr_comment_poll_time", None)
            if method:
                method(current_poll_time)
            update_monitoring("poll-github-pr-comments")
            logger.info("Done. %s", summary)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
