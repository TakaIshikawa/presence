#!/usr/bin/env python3
"""Report snoozed content ideas that should wake up soon."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.content_idea_snooze_wakeup import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    build_content_idea_snooze_wakeup_report,
    format_content_idea_snooze_wakeup_json,
    format_content_idea_snooze_wakeup_text,
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
        help=(
            "Include snoozes ending within this many days "
            f"(default: {DEFAULT_DAYS_AHEAD})."
        ),
    )
    parser.add_argument(
        "--include-overdue",
        dest="include_overdue",
        action="store_true",
        default=True,
        help="Include snoozes that ended before the report time (default).",
    )
    parser.add_argument(
        "--no-include-overdue",
        dest="include_overdue",
        action="store_false",
        help="Exclude snoozes that ended before the report time.",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_content_idea_snooze_wakeup_report(
                db,
                days_ahead=args.days_ahead,
                include_overdue=args.include_overdue,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_content_idea_snooze_wakeup_json(report))
    else:
        print(format_content_idea_snooze_wakeup_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
