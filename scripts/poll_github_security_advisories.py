#!/usr/bin/env python3
"""Poll GitHub repository security advisories."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_security_advisories import poll_security_advisories  # noqa: E402
from runner import script_context, update_monitoring  # noqa: E402

logger = logging.getLogger(__name__)

CURSOR_KEY = "github_security_advisories:last_poll_time"


def parse_since(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_meta_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def determine_since(db, explicit_since: datetime | None, lookback_hours: int) -> datetime:
    if explicit_since:
        return explicit_since
    last_poll = _parse_meta_datetime(db.get_meta(CURSOR_KEY) if hasattr(db, "get_meta") else None)
    if last_poll:
        return last_poll
    return datetime.now(timezone.utc) - timedelta(hours=lookback_hours)


def ingest_github_security_advisories(
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
    return poll_security_advisories(
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
    parser = argparse.ArgumentParser(description="Poll GitHub repository security advisories.")
    parser.add_argument(
        "--since",
        help="ISO timestamp to poll from. Defaults to the last poll cursor or lookback window.",
    )
    parser.add_argument(
        "--lookback-hours",
        type=int,
        default=24,
        help="Initial lookback window when no advisory poll exists (default: 24).",
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
        help="Maximum advisories to ingest per repository (default: 100).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print candidate rows without writing to the database.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format for candidate rows (default: text).",
    )
    return parser


def _format_advisory_line(advisory, output_format: str) -> str:
    if output_format == "json":
        return json.dumps(advisory.to_activity_dict(), sort_keys=True)
    cves = ",".join(advisory.metadata.get("cves") or []) or "-"
    packages = ",".join(advisory.metadata.get("package_names") or []) or "-"
    return (
        f"{advisory.activity_id} severity={advisory.severity or '-'} "
        f"state={advisory.state or '-'} cves={cves} packages={packages} "
        f"updated_at={advisory.updated_at.isoformat()} {advisory.url}"
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        since = determine_since(db, parse_since(args.since), args.lookback_hours)
        current_poll_time = datetime.now(timezone.utc)
        repositories = args.repo or getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub security advisories since %s", since.isoformat())
        if repositories:
            logger.info("Using %d repositories", len(repositories))

        advisories = ingest_github_security_advisories(
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

        for advisory in advisories:
            prefix = "Would ingest" if args.dry_run else "Ingested"
            line = f"{prefix} {_format_advisory_line(advisory, args.format)}"
            if args.dry_run:
                print(line)
            logger.info(line)

        if args.dry_run:
            logger.info("Dry run complete. %d candidate security advisories found.", len(advisories))
        else:
            if hasattr(db, "set_meta"):
                db.set_meta(CURSOR_KEY, current_poll_time.isoformat())
            update_monitoring("poll-github-security-advisories")
            logger.info("Done. Ingested %d GitHub security advisories.", len(advisories))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
