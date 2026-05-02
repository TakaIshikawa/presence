#!/usr/bin/env python3
"""Report reply follow-up reminders by due window."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_followup_due_windows import (  # noqa: E402
    DEFAULT_HORIZON_HOURS,
    build_reply_followup_due_windows_report,
    format_reply_followup_due_windows_json,
    format_reply_followup_due_windows_text,
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--horizon-hours",
        type=_positive_int,
        default=DEFAULT_HORIZON_HOURS,
        help=(
            "Include pending reminders due within this many hours "
            f"(default: {DEFAULT_HORIZON_HOURS})."
        ),
    )
    parser.add_argument("--target-handle", help="Only include one target handle.")
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
            report = build_reply_followup_due_windows_report(
                db,
                horizon_hours=args.horizon_hours,
                target_handle=args.target_handle,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_reply_followup_due_windows_json(report))
    else:
        print(format_reply_followup_due_windows_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
