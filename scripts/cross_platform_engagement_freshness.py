#!/usr/bin/env python3
"""Report stale or missing engagement metrics for published content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.cross_platform_engagement_freshness import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MAX_AGE_HOURS,
    SUPPORTED_PLATFORMS,
    build_cross_platform_engagement_freshness_report,
    format_cross_platform_engagement_freshness_json,
    format_cross_platform_engagement_freshness_text,
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
        "--platform",
        choices=("all", *SUPPORTED_PLATFORMS),
        default="all",
        help="Restrict report to one platform (default: all).",
    )
    parser.add_argument(
        "--max-age-hours",
        type=_positive_int,
        default=DEFAULT_MAX_AGE_HOURS,
        help=f"Maximum metric age before stale (default: {DEFAULT_MAX_AGE_HOURS}).",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by publication timestamp (default: {DEFAULT_DAYS}).",
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
            report = build_cross_platform_engagement_freshness_report(
                db,
                platform=args.platform,
                max_age_hours=args.max_age_hours,
                days=args.days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_cross_platform_engagement_freshness_json(report))
    else:
        print(format_cross_platform_engagement_freshness_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
