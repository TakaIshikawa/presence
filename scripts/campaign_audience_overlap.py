#!/usr/bin/env python3
"""Report active campaign audience overlap in the planning window."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.campaign_audience_overlap import (  # noqa: E402
    DEFAULT_DAYS_AHEAD,
    DEFAULT_THRESHOLD,
    build_campaign_audience_overlap_report,
    format_campaign_audience_overlap_json,
    format_campaign_audience_overlap_text,
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
        help=f"Lookahead window in days from today (default: {DEFAULT_DAYS_AHEAD}).",
    )
    parser.add_argument(
        "--threshold",
        type=_positive_int,
        default=DEFAULT_THRESHOLD,
        help=(
            "Flag an audience tag only when active planned item count exceeds this "
            f"value (default: {DEFAULT_THRESHOLD})."
        ),
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
            report = build_campaign_audience_overlap_report(
                db,
                days_ahead=args.days_ahead,
                threshold=args.threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_campaign_audience_overlap_json(report))
    else:
        print(format_campaign_audience_overlap_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
