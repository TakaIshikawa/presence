#!/usr/bin/env python3
"""Evaluate publish queue retry backoff effectiveness."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_queue_retry_backoff_effectiveness import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MAX_BACKOFF_HOURS,
    DEFAULT_MIN_BACKOFF_MINUTES,
    DEFAULT_STALE_FAILED_HOURS,
    build_publish_queue_retry_backoff_effectiveness_report_from_db,
    format_publish_queue_retry_backoff_effectiveness_json,
    format_publish_queue_retry_backoff_effectiveness_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _datetime(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("value must be an ISO datetime") from exc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--min-backoff-minutes", type=_non_negative_float, default=DEFAULT_MIN_BACKOFF_MINUTES)
    parser.add_argument("--max-backoff-hours", type=_positive_float, default=DEFAULT_MAX_BACKOFF_HOURS)
    parser.add_argument("--stale-failed-hours", type=_non_negative_float, default=DEFAULT_STALE_FAILED_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--now", type=_datetime)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {
        "min_backoff_minutes": args.min_backoff_minutes,
        "max_backoff_hours": args.max_backoff_hours,
        "stale_failed_hours": args.stale_failed_hours,
        "limit": args.limit,
        "now": args.now,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_publish_queue_retry_backoff_effectiveness_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_retry_backoff_effectiveness_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_publish_queue_retry_backoff_effectiveness_text(report) if args.format == "text" else format_publish_queue_retry_backoff_effectiveness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
