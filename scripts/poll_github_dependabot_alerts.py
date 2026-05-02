#!/usr/bin/env python3
"""Poll GitHub Dependabot alerts."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from ingestion.github_dependabot_alerts import poll_dependabot_alerts  # noqa: E402
from runner import script_context, update_monitoring  # noqa: E402

logger = logging.getLogger(__name__)

VALID_STATES = ("auto_dismissed", "dismissed", "fixed", "open")


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def ingest_github_dependabot_alerts(
    db,
    token: str,
    username: str,
    repositories: list[str | dict] | None = None,
    state: str | None = None,
    dry_run: bool = False,
    limit: int = 100,
    timeout: int = 30,
    redaction_patterns: list[str | dict] | None = None,
) -> list:
    return poll_dependabot_alerts(
        token=token,
        username=username,
        db=db,
        repositories=repositories,
        state=state,
        dry_run=dry_run,
        limit_per_repo=limit,
        timeout=timeout,
        redaction_patterns=redaction_patterns,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll GitHub Dependabot alerts.")
    parser.add_argument(
        "--repo",
        action="append",
        help="Repository to poll as owner/name. May be passed more than once.",
    )
    parser.add_argument(
        "--state",
        choices=VALID_STATES,
        default=None,
        help="Only fetch alerts in this state.",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=100,
        help="Maximum alerts to ingest per repository (default: 100).",
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
        help="Output format for fetched alerts (default: text).",
    )
    return parser


def format_dependabot_alert_json(alert) -> str:
    return json.dumps(alert.to_activity_dict(), sort_keys=True)


def format_dependabot_alert_text(alert) -> str:
    return (
        f"{alert.external_id} severity={alert.severity or '-'} state={alert.state or '-'} "
        f"package={alert.package or '-'} ecosystem={alert.ecosystem or '-'} "
        f"ghsa={alert.ghsa_id or '-'} cve={alert.cve_id or '-'} "
        f"created_at={alert.created_at.isoformat()} {alert.url}"
    )


def _format_alert_line(alert, output_format: str) -> str:
    if output_format == "json":
        return format_dependabot_alert_json(alert)
    return format_dependabot_alert_text(alert)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)

    with script_context() as (config, db):
        repositories = args.repo or getattr(config.github, "repositories", None) or None

        logger.info("Polling GitHub Dependabot alerts")
        if repositories:
            logger.info("Using %d repositories", len(repositories))
        if args.state:
            logger.info("Filtering Dependabot alerts by state=%s", args.state)

        alerts = ingest_github_dependabot_alerts(
            db=db,
            token=config.github.token,
            username=config.github.username,
            repositories=repositories,
            state=args.state,
            dry_run=args.dry_run,
            limit=args.limit,
            timeout=config.timeouts.github_seconds,
            redaction_patterns=config.privacy.redaction_patterns,
        )

        for alert in alerts:
            prefix = "Would ingest" if args.dry_run else "Ingested"
            line = f"{prefix} {_format_alert_line(alert, args.format)}"
            if args.dry_run:
                print(line)
            logger.info(line)

        if args.dry_run:
            logger.info("Dry run complete. %d candidate Dependabot alerts found.", len(alerts))
        else:
            update_monitoring("poll-github-dependabot-alerts")
            logger.info("Done. Ingested %d GitHub Dependabot alerts.", len(alerts))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
