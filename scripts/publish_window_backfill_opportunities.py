#!/usr/bin/env python3
"""Report upcoming publish windows that can be backfilled."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publish_window_backfill_opportunities import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    build_publish_window_backfill_opportunity_report,
    format_publish_window_backfill_opportunities_json,
    format_publish_window_backfill_opportunities_text,
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
        raise argparse.ArgumentTypeError(f"invalid score: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days-ahead",
        type=_positive_int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Upcoming window horizon in days (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--min-score",
        type=_non_negative_float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum generated_content.eval_score for candidates (default: {DEFAULT_MIN_SCORE}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum opportunities and recommendations to emit (default: {DEFAULT_LIMIT}).",
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
            report = build_publish_window_backfill_opportunity_report(
                db,
                days_ahead=args.days_ahead,
                min_score=args.min_score,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publish_window_backfill_opportunities_json(report))
    else:
        print(format_publish_window_backfill_opportunities_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
