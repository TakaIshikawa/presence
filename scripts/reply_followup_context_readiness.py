#!/usr/bin/env python3
"""Report reply follow-ups due soon without enough drafting context."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_context_readiness import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_LIMIT,
    DEFAULT_MAX_CONTEXT_AGE_HOURS,
    build_reply_followup_context_readiness_report_from_db,
    format_reply_followup_context_readiness_json,
    format_reply_followup_context_readiness_text,
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


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid datetime: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days-ahead", type=_non_negative_int, default=DEFAULT_DAYS_AHEAD)
    parser.add_argument("--max-context-age-hours", type=_positive_float, default=DEFAULT_MAX_CONTEXT_AGE_HOURS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--now", type=_datetime, help="Current time for deterministic readiness calculations.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    kwargs = {
        "days_ahead": args.days_ahead,
        "max_context_age_hours": args.max_context_age_hours,
        "limit": args.limit,
        "now": args.now,
    }
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_followup_context_readiness_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_reply_followup_context_readiness_report_from_db(db, **kwargs)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_reply_followup_context_readiness_text(report)
        if args.format == "text"
        else format_reply_followup_context_readiness_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
