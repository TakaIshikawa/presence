#!/usr/bin/env python3
"""Report publish queue schedule and state integrity problems."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_queue_schedule_integrity import (  # noqa: E402
    DEFAULT_GRACE_HOURS,
    DEFAULT_LIMIT,
    build_publish_queue_schedule_integrity_report_from_db,
    format_publish_queue_schedule_integrity_json,
    format_publish_queue_schedule_integrity_text,
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
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"grace_hours": args.grace_hours, "limit": args.limit}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_publish_queue_schedule_integrity_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_schedule_integrity_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_publish_queue_schedule_integrity_text(report)
        if args.format == "text"
        else format_publish_queue_schedule_integrity_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
