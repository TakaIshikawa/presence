#!/usr/bin/env python3
"""Report uncovered calendar gaps for active campaigns."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_date_gaps import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_MIN_GAP_DAYS,
    format_campaign_date_gaps_json,
    format_campaign_date_gaps_text,
    plan_campaign_date_gaps,
)


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
        "--days-ahead",
        type=_positive_int,
        default=DEFAULT_DAYS_AHEAD,
        help=f"Calendar horizon in days from today (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--min-gap-days",
        type=_positive_int,
        default=DEFAULT_MIN_GAP_DAYS,
        help=f"Minimum uncovered range length to report (default: {DEFAULT_MIN_GAP_DAYS}).",
    )
    parser.add_argument(
        "--campaign-id",
        type=_positive_int,
        help="Only inspect one active campaign ID.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = plan_campaign_date_gaps(
                db,
                days_ahead=args.days_ahead,
                min_gap_days=args.min_gap_days,
                campaign_id=args.campaign_id,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_date_gaps_json(report))
    else:
        print(format_campaign_date_gaps_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
