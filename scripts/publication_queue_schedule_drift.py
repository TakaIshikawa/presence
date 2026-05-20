#!/usr/bin/env python3
"""Report active publish queue rows whose schedule has drifted past due."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_queue_schedule_drift import (  # noqa: E402
    DEFAULT_GRACE_HOURS,
    DEFAULT_LIMIT,
    DEFAULT_PLATFORM,
    build_publication_queue_schedule_drift_report_from_db,
    format_publication_queue_schedule_drift_json,
    format_publication_queue_schedule_drift_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--grace-hours", type=_non_negative_int, default=DEFAULT_GRACE_HOURS)
    parser.add_argument("--status", default=None, help="Queue status or comma-separated statuses. Defaults to active statuses.")
    parser.add_argument("--platform", default=DEFAULT_PLATFORM)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {
            "grace_hours": args.grace_hours,
            "platform": args.platform,
            "limit": args.limit,
        }
        if args.status is not None:
            kwargs["status"] = args.status
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publication_queue_schedule_drift_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publication_queue_schedule_drift_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publication_queue_schedule_drift_text(report) if args.format == "text" else format_publication_queue_schedule_drift_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
