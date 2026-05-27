#!/usr/bin/env python3
"""Report reply drafts that appear to deflect question-like targets."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_question_deflection import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_QUESTION_SCORE,
    DEFAULT_STATUS,
    DEFAULT_WINDOW_DAYS,
    build_reply_draft_question_deflection_report_from_db,
    format_reply_draft_question_deflection_json,
    format_reply_draft_question_deflection_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--status", default=DEFAULT_STATUS)
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--min-question-score", type=_positive_int, default=DEFAULT_MIN_QUESTION_SCORE)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"status": args.status or None, "window_days": args.window_days, "min_question_score": args.min_question_score, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_reply_draft_question_deflection_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_draft_question_deflection_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_draft_question_deflection_text(report) if args.format == "text" else format_reply_draft_question_deflection_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
