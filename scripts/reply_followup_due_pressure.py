#!/usr/bin/env python3
"""Report pressure from pending reply follow-up reminders."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_due_pressure import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_LIMIT,
    DEFAULT_OVERDUE_GRACE_HOURS,
    build_reply_followup_due_pressure_report,
    format_reply_followup_due_pressure_json,
    format_reply_followup_due_pressure_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _nonnegative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-ahead", type=_positive_int, default=DEFAULT_DAYS_AHEAD)
    parser.add_argument("--overdue-grace-hours", type=_nonnegative_float, default=DEFAULT_OVERDUE_GRACE_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_followup_due_pressure_report(db, days_ahead=args.days_ahead, overdue_grace_hours=args.overdue_grace_hours, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_reply_followup_due_pressure_text(report) if as_text else format_reply_followup_due_pressure_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
