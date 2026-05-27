#!/usr/bin/env python3
"""Report stale reply drafts or sent replies that promise follow-up."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_draft_followup_promise_staleness import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_HOURS,
    DEFAULT_STATUS,
    DEFAULT_WINDOW_DAYS,
    build_reply_draft_followup_promise_staleness_report_from_db,
    format_reply_draft_followup_promise_staleness_json,
    format_reply_draft_followup_promise_staleness_text,
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
    parser.add_argument("--stale-hours", type=_positive_int, default=DEFAULT_STALE_HOURS)
    parser.add_argument("--window-days", type=_positive_int, default=DEFAULT_WINDOW_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"status": args.status or None, "stale_hours": args.stale_hours, "window_days": args.window_days, "limit": args.limit}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                report = build_reply_draft_followup_promise_staleness_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_draft_followup_promise_staleness_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_draft_followup_promise_staleness_text(report) if args.format == "text" else format_reply_draft_followup_promise_staleness_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
