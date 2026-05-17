#!/usr/bin/env python3
"""Report blog/newsletter timing alignment."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_newsletter_timing_alignment import (  # noqa: E402
    DEFAULT_EARLY_TOLERANCE_DAYS,
    DEFAULT_LATE_AFTER_DAYS,
    DEFAULT_LIMIT,
    build_blog_newsletter_timing_alignment_report_from_db,
    format_blog_newsletter_timing_alignment_json,
    format_blog_newsletter_timing_alignment_text,
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


def _positive_int(value: str) -> int:
    parsed = _non_negative_int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--early-tolerance-days", type=_non_negative_int, default=DEFAULT_EARLY_TOLERANCE_DAYS)
    parser.add_argument("--late-after-days", type=_non_negative_int, default=DEFAULT_LATE_AFTER_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_newsletter_timing_alignment_report_from_db(
                db,
                early_tolerance_days=args.early_tolerance_days,
                late_after_days=args.late_after_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_blog_newsletter_timing_alignment_text(report)
        if args.table or args.format == "text"
        else format_blog_newsletter_timing_alignment_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
