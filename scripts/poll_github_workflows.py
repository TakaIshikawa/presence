#!/usr/bin/env python3
"""Poll GitHub Actions workflow runs."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_workflows import poll_new_workflow_runs
from runner import script_context, update_monitoring

logger = logging.getLogger(__name__)

CURSOR_KEY = "github_workflows:last_poll_time"


def _parse_meta_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def determine_since(db, since_hours: int) -> datetime:
    last_poll = _parse_meta_datetime(db.get_meta(CURSOR_KEY) if hasattr(db, "get_meta") else None)
    if last_poll:
        return last_poll
    return datetime.now(timezone.utc) - timedelta(hours=since_hours)


def ingest_github_workflow_runs(
    db,
    token: str,
    username: str,
    since: datetime,
    repositories: list[str | dict] | None = None,
    dry_run: bool = False,
    limit: int = 100,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
) -> list:
    return poll_new_workflow_runs(
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
    parser = argparse.ArgumentParser(description="Poll recent GitHub Actions workflow runs.")
    parser.add_argument(
        "--since-hours",
        type=int,
        default=24,
        help="Initial lookback window when no previous workflow poll exists (default: 24).",
    )
    parser.add_argument(
        "--repo",
        action="append",
        help="Repository to poll as owner/name. May be passed more than once.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum workflow runs to inspect per repository (default: 100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print proposed workflow runs without writing to the database.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        since = determine_since(db, args.since_hours)
        current_poll_time = datetime.now(timezone.utc)
        repositories = args.repo or getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub workflow runs since %s", since.isoformat())
        if repositories:
            logger.info("Using %d repositories", len(repositories))

        runs = ingest_github_workflow_runs(
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

        for run in runs:
            line = (
                f"{'Would ingest' if args.dry_run else 'Ingested'} "
                f"{run.activity_id} state={run.state} branch={run.branch} "
                f"{run.updated_at.isoformat()} {run.url}"
            )
            if args.dry_run:
                print(line)
            logger.info(line)

        if args.dry_run:
            logger.info("Dry run complete. %d new/updated workflow runs found.", len(runs))
        else:
            if hasattr(db, "set_meta"):
                db.set_meta(CURSOR_KEY, current_poll_time.isoformat())
            update_monitoring("poll-github-workflows")
            logger.info("Done. Ingested %d GitHub workflow runs.", len(runs))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
