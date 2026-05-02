#!/usr/bin/env python3
"""Seed content ideas from unresolved Dependabot alert activity."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.dependabot_alert_idea_seeder import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    SEVERITY_ORDER,
    format_dependabot_alert_idea_results_json,
    format_dependabot_alert_idea_results_text,
    seed_dependabot_alert_ideas,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for Dependabot alerts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-severity",
        choices=tuple(SEVERITY_ORDER),
        default="medium",
        help="Minimum alert severity to seed (default: medium).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum alert groups to process (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Return candidates without inserting content_ideas rows.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            results = seed_dependabot_alert_ideas(
                db,
                days=args.days,
                min_severity=args.min_severity,
                limit=args.limit,
                dry_run=args.dry_run,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_dependabot_alert_idea_results_json(results))
    else:
        print(format_dependabot_alert_idea_results_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
