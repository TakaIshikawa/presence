#!/usr/bin/env python3
"""Report queued content without a usable persona guard result."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_persona_guard_queue_gaps import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_AGE_HOURS,
    DEFAULT_STATUS,
    build_content_persona_guard_queue_gaps_report_from_db,
    format_content_persona_guard_queue_gaps_json,
    format_content_persona_guard_queue_gaps_text,
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
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--status", default=DEFAULT_STATUS, help="Queue status or comma-separated statuses. Defaults to queued.")
    parser.add_argument("--min-age-hours", type=_non_negative_int, default=DEFAULT_MIN_AGE_HOURS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        kwargs = {
            "limit": args.limit,
            "status": args.status,
            "min_age_hours": args.min_age_hours,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_content_persona_guard_queue_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_content_persona_guard_queue_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_content_persona_guard_queue_gaps_text(report)
        if args.format == "text"
        else format_content_persona_guard_queue_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
