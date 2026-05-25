#!/usr/bin/env python3
"""Export Reply Unanswered Question Gaps."""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.reply_unanswered_question_gaps import DEFAULT_MIN_AGE_HOURS, build_reply_unanswered_question_gaps_report_from_db, format_reply_unanswered_question_gaps_json, format_reply_unanswered_question_gaps_text  # noqa: E402
from runner import script_context  # noqa: E402


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
    parser.add_argument("--min-age-hours", type=_positive_int, default=DEFAULT_MIN_AGE_HOURS)
    parser.add_argument("--platform")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        kwargs = {"min_age_hours": args.min_age_hours, "platform": args.platform}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_unanswered_question_gaps_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_unanswered_question_gaps_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_unanswered_question_gaps_text(report) if args.format == "text" else format_reply_unanswered_question_gaps_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
