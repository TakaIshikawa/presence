#!/usr/bin/env python3
"""Report overdue and upcoming reply follow-up reminders."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_digest import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    build_reply_followup_digest_report,
    format_reply_followup_digest_json,
    format_reply_followup_digest_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-ahead",
        type=_non_negative_int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Number of days ahead to include upcoming reminders (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--include-completed",
        action="store_true",
        help="Include completed follow-up reminders.",
    )
    parser.add_argument("--platform", help="Only include reminders for this platform.")
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_reply_followup_digest_report(
                db,
                days_ahead=args.days_ahead,
                include_completed=args.include_completed,
                platform=args.platform,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_followup_digest_json(report))
    else:
        print(format_reply_followup_digest_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
