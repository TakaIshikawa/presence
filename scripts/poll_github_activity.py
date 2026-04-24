#!/usr/bin/env python3
"""Poll GitHub issues, pull requests, releases, comments, and workflow runs."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_activity import GitHubActivityClient, poll_new_activity
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
    last_poll = db.get_last_github_activity_poll_time()
    if last_poll:
        if last_poll.tzinfo is None:
            last_poll = last_poll.replace(tzinfo=timezone.utc)
        return last_poll
    return datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)


def ingest_github_activity(
    db,
    token: str,
    username: str,
    since: datetime,
    repositories: list[str | dict] | None = None,
    include_issues: bool = True,
    include_discussions: bool = False,
    include_pull_requests: bool = False,
    include_comments: bool = False,
    include_workflow_runs: bool = False,
    include_releases: bool = False,
    dry_run: bool = False,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
) -> list:
    return poll_new_activity(
        token=token,
        username=username,
        since=since,
        db=db,
        repositories=repositories,
        include_issues=include_issues,
        include_discussions=include_discussions,
        include_pull_requests=include_pull_requests,
        include_comments=include_comments,
        include_workflow_runs=include_workflow_runs,
        include_releases=include_releases,
        dry_run=dry_run,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Poll recently updated GitHub issues, pull requests, releases, and workflow runs."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and report activity without writing to the database.",
    )
    parser.add_argument(
        "--since",
        help="ISO timestamp to poll from. Defaults to the last activity poll or lookback window.",
    )
    parser.add_argument(
        "--lookback-minutes",
        type=int,
        default=90,
        help="Initial lookback window when no previous activity poll exists.",
    )
    parser.add_argument(
        "--include-comments",
        action="store_true",
        help="Also ingest issue comments and pull request review comments.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        since = determine_since(db, parse_since(args.since), args.lookback_minutes)
        current_poll_time = datetime.now(timezone.utc)
        repositories = getattr(config.github, "repositories", None) or None
        include_issues = getattr(config.github, "include_issues", True)
        include_discussions = getattr(config.github, "include_discussions", False)
        include_pull_requests = getattr(config.github, "include_pull_requests", False)
        include_comments = args.include_comments or getattr(config.github, "include_comments", False)
        include_workflow_runs = getattr(config.github, "include_workflow_runs", False)
        include_releases = getattr(config.github, "include_releases", False)

        logger.info("Polling GitHub activity since %s", since.isoformat())
        include_comments = args.include_comments or getattr(config.github, "include_comments", False)
        include_workflow_runs = getattr(config.github, "include_workflow_runs", False)
        if repositories:
            logger.info("Using %d configured repositories", len(repositories))
        if not include_issues:
            logger.info("Skipping GitHub issues")
        if include_pull_requests:
            logger.info("Including GitHub pull requests")
        if include_releases:
            logger.info("Including GitHub releases")
        if include_discussions:
            logger.info("Including GitHub Discussions")
        if include_comments:
            logger.info("Including GitHub issue comments and PR review comments")
        if include_workflow_runs:
            logger.info("Including GitHub Actions workflow runs")

        activity = ingest_github_activity(
            db=db,
            token=config.github.token,
            username=config.github.username,
            since=since,
            repositories=repositories,
            include_issues=include_issues,
            include_discussions=include_discussions,
            include_pull_requests=include_pull_requests,
            include_comments=include_comments,
            include_workflow_runs=include_workflow_runs,
            include_releases=include_releases,
            dry_run=args.dry_run,
            timeout=config.timeouts.github_seconds,
            redaction_patterns=config.privacy.redaction_patterns,
        )

        for item in activity:
            logger.info(
                "%s: [%s] #%s %s",
                "Would ingest" if args.dry_run else "Ingested",
                item.repo_name,
                item.number,
                item.title[:80],
            )

        if args.dry_run:
            logger.info("Dry run complete. %d new/updated activity records found.", len(activity))
        else:
            db.set_last_github_activity_poll_time(current_poll_time)
            update_monitoring("poll-github-activity")
            logger.info("Done. Ingested %d GitHub activity records.", len(activity))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
