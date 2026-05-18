#!/usr/bin/env python3
"""Report blog series continuation opportunities."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.blog_series_continuation_opportunities import (  # noqa: E402
    DEFAULT_HIGH_ENGAGEMENT_SCORE,
    DEFAULT_LIMIT,
    DEFAULT_STALE_SERIES_GAP_DAYS,
    build_blog_series_continuation_opportunities_report_from_db,
    format_blog_series_continuation_opportunities_json,
    format_blog_series_continuation_opportunities_text,
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


def _non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--high-engagement-score", type=_non_negative_float, default=DEFAULT_HIGH_ENGAGEMENT_SCORE)
    parser.add_argument("--stale-series-gap-days", type=_positive_int, default=DEFAULT_STALE_SERIES_GAP_DAYS)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_blog_series_continuation_opportunities_report_from_db(
                db,
                high_engagement_score=args.high_engagement_score,
                stale_series_gap_days=args.stale_series_gap_days,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_blog_series_continuation_opportunities_text(report) if args.table or args.format == "text" else format_blog_series_continuation_opportunities_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
