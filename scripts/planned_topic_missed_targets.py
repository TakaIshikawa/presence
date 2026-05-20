#!/usr/bin/env python3
"""Report planned topics whose target dates have passed without publication."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.planned_topic_missed_targets import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_DAYS_OVERDUE,
    build_planned_topic_missed_targets_report_from_db,
    format_planned_topic_missed_targets_json,
    format_planned_topic_missed_targets_text,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--as-of", help="As-of date in YYYY-MM-DD format.")
    parser.add_argument("--min-days-overdue", type=_non_negative_int, default=DEFAULT_MIN_DAYS_OVERDUE)
    parser.add_argument("--campaign-status", default="all", help="Comma-separated campaign status filter.")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "as_of": args.as_of,
            "min_days_overdue": args.min_days_overdue,
            "campaign_status": args.campaign_status,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_planned_topic_missed_targets_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_planned_topic_missed_targets_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_planned_topic_missed_targets_text(report) if args.format == "text" else format_planned_topic_missed_targets_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
