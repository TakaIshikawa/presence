#!/usr/bin/env python3
"""Report reply follow-up reminder completion timeliness."""

from __future__ import annotations

import argparse
from datetime import datetime
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_completion_sla import (  # noqa: E402
    DEFAULT_GRACE_HOURS,
    build_reply_followup_completion_sla_report_from_db,
    format_reply_followup_completion_sla_json,
    format_reply_followup_completion_sla_text,
)
from runner import script_context  # noqa: E402


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
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
    parser.add_argument("--now", type=_datetime, help="ISO datetime to evaluate overdue reminders against.")
    parser.add_argument("--grace-hours", type=_non_negative_float, default=DEFAULT_GRACE_HOURS)
    parser.add_argument("--target-handle", help="Only include one target handle.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    kwargs = {"now": args.now, "grace_hours": args.grace_hours, "target_handle": args.target_handle}
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_followup_completion_sla_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_followup_completion_sla_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_reply_followup_completion_sla_text(report)
        if args.format == "text"
        else format_reply_followup_completion_sla_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
