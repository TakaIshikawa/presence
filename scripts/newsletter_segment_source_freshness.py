#!/usr/bin/env python3
"""Report newsletter segment source freshness."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_segment_source_freshness import (  # noqa: E402
    DEFAULT_AGING_HOURS,
    DEFAULT_FRESH_HOURS,
    build_newsletter_segment_source_freshness_report_from_db,
    format_newsletter_segment_source_freshness_json,
    format_newsletter_segment_source_freshness_table,
)
from runner import script_context  # noqa: E402


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = _non_negative_float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fresh-hours", type=_non_negative_float, default=DEFAULT_FRESH_HOURS)
    parser.add_argument("--aging-hours", type=_positive_float, default=DEFAULT_AGING_HOURS)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true", help="Print table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_newsletter_segment_source_freshness_report_from_db(
                db,
                fresh_hours=args.fresh_hours,
                aging_hours=args.aging_hours,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_newsletter_segment_source_freshness_table(report)
        if args.table or args.format == "table"
        else format_newsletter_segment_source_freshness_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
