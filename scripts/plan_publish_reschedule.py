#!/usr/bin/env python3
"""Suggest dry-run scheduled_at updates for stale publish queue items."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_queue_reschedule import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_LIMIT,
    PLATFORMS,
    STATUSES,
    build_publish_queue_reschedule_report,
    format_publish_queue_reschedule_json,
    format_publish_queue_reschedule_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days-ahead",
        type=_positive_int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Scheduling horizon in days (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--platform",
        choices=("all", *PLATFORMS),
        default="all",
        help="Platform to plan for (default: all).",
    )
    parser.add_argument(
        "--status",
        choices=("all", *STATUSES),
        default="all",
        help="Queue status to plan for (default: all).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum suggestions to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_publish_queue_reschedule_report(
                    conn,
                    days_ahead=args.days_ahead,
                    platform=args.platform,
                    status=args.status,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_publish_queue_reschedule_report(
                    db,
                    days_ahead=args.days_ahead,
                    platform=args.platform,
                    status=args.status,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_queue_reschedule_json(report))
    else:
        print(format_publish_queue_reschedule_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
