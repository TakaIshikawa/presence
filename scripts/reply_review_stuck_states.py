#!/usr/bin/env python3
"""Find reply drafts stuck in review workflow states."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_review_stuck_states import (  # noqa: E402
    DEFAULT_MAX_AGE_HOURS,
    build_reply_review_stuck_states_report_from_db,
    format_reply_review_stuck_states_json,
    format_reply_review_stuck_states_text,
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
    parser.add_argument("--max-age-hours", type=_non_negative_int, default=DEFAULT_MAX_AGE_HOURS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_review_stuck_states_report_from_db(db, max_age_hours=args.max_age_hours)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_reply_review_stuck_states_text(report) if args.format == "text" else format_reply_review_stuck_states_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
