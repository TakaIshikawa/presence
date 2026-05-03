#!/usr/bin/env python3
"""Report reply drafts with repeated review edits."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_edit_churn import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_EDITS,
    build_reply_edit_churn_report,
    format_reply_edit_churn_csv,
    format_reply_edit_churn_json,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--platform", help="Only include this reply_queue platform.")
    parser.add_argument("--status", help="Only include this final reply_queue status.")
    parser.add_argument("--intent", help="Only include this reply intent.")
    parser.add_argument("--priority", help="Only include this reply priority.")
    parser.add_argument("--start-date", help="Only include replies detected at or after this ISO date.")
    parser.add_argument("--end-date", help="Only include replies detected at or before this ISO date.")
    parser.add_argument(
        "--min-edits",
        type=_non_negative_int,
        default=DEFAULT_MIN_EDITS,
        help=f"Minimum edited events required to emit a row (default: {DEFAULT_MIN_EDITS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum rows to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("csv", "json"),
        default="csv",
        help="Output format (default: csv).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        kwargs = {
            "platform": args.platform,
            "status": args.status,
            "intent": args.intent,
            "priority": args.priority,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "min_edits": args.min_edits,
            "limit": args.limit,
        }
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_edit_churn_report(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_edit_churn_report(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_edit_churn_json(report))
    else:
        print(format_reply_edit_churn_csv(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
