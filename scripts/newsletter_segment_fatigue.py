#!/usr/bin/env python3
"""Report repeated newsletter segment fatigue."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_segment_fatigue import (  # noqa: E402
    DEFAULT_LOOKBACK,
    DEFAULT_MIN_REPEAT,
    build_newsletter_segment_fatigue_report_from_db,
    format_newsletter_segment_fatigue_json,
    format_newsletter_segment_fatigue_text,
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


def _repeat(value: str) -> int:
    parsed = _positive_int(value)
    if parsed <= 1:
        raise argparse.ArgumentTypeError("value must be greater than 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--lookback", type=_positive_int, default=DEFAULT_LOOKBACK)
    parser.add_argument("--min-repeat", type=_repeat, default=DEFAULT_MIN_REPEAT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_newsletter_segment_fatigue_report_from_db(
                    conn,
                    lookback=args.lookback,
                    min_repeat=args.min_repeat,
                )
        else:
            with script_context() as (_config, db):
                report = build_newsletter_segment_fatigue_report_from_db(
                    db,
                    lookback=args.lookback,
                    min_repeat=args.min_repeat,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_newsletter_segment_fatigue_text(report)
        if args.format == "text"
        else format_newsletter_segment_fatigue_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
