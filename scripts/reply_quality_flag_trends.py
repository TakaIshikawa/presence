#!/usr/bin/env python3
"""Report reply_queue quality flag trend buckets."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_quality_flag_trends import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_COUNT,
    DEFAULT_MIN_RATE,
    DEFAULT_WINDOW_HOURS,
    build_reply_quality_flag_trends_report_from_db,
    format_reply_quality_flag_trends_json,
    format_reply_quality_flag_trends_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _rate(value: str) -> float:
    parsed = float(value)
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("value must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--window-hours", type=_positive_int, default=DEFAULT_WINDOW_HOURS)
    parser.add_argument("--min-count", type=_positive_int, default=DEFAULT_MIN_COUNT)
    parser.add_argument("--min-rate", type=_rate, default=DEFAULT_MIN_RATE)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {
            "window_hours": args.window_hours,
            "min_count": args.min_count,
            "min_rate": args.min_rate,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_quality_flag_trends_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_quality_flag_trends_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_reply_quality_flag_trends_text(report)
        if args.format == "text"
        else format_reply_quality_flag_trends_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
