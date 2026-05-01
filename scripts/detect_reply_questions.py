#!/usr/bin/env python3
"""Detect inbound mentions that likely ask direct unanswered questions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_question_detector import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_SCORE,
    build_reply_question_report,
    format_reply_question_report_json,
    format_reply_question_report_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help=f"Only include mentions detected in the last N days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum question score to report (default: {DEFAULT_MIN_SCORE:g}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--include-resolved",
        action="store_true",
        help="Include questions with approved, posted, dismissed, or otherwise resolved reply state.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_question_report(
                    conn,
                    days=args.days,
                    min_score=args.min_score,
                    include_resolved=args.include_resolved,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_question_report(
                    db,
                    days=args.days,
                    min_score=args.min_score,
                    include_resolved=args.include_resolved,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_question_report_json(report))
    else:
        print(format_reply_question_report_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
