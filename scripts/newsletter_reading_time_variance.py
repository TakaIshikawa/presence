#!/usr/bin/env python3
"""Report newsletter reading-time variance for recent sent issues."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.newsletter_reading_time_variance import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_WORDS_PER_MINUTE,
    build_newsletter_reading_time_variance_report,
    format_newsletter_reading_time_variance_json,
    format_newsletter_reading_time_variance_text,
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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by newsletter send timestamp (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum recent sent newsletters to inspect (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--min-words-per-minute",
        type=_positive_int,
        default=DEFAULT_MIN_WORDS_PER_MINUTE,
        help=(
            "Words-per-minute rate used for estimated reading time "
            f"(default: {DEFAULT_MIN_WORDS_PER_MINUTE})."
        ),
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
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_newsletter_reading_time_variance_report(
                db,
                days=args.days,
                limit=args.limit,
                min_words_per_minute=args.min_words_per_minute,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_newsletter_reading_time_variance_json(report))
    else:
        print(format_newsletter_reading_time_variance_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
