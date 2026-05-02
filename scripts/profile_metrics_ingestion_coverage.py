#!/usr/bin/env python3
"""Report profile metrics ingestion coverage by platform."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.profile_metrics_ingestion_coverage import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_EXPECTED_INTERVAL_HOURS,
    DEFAULT_MAX_STALE_HOURS,
    build_profile_metrics_ingestion_coverage_report,
    format_profile_metrics_ingestion_coverage_json,
    format_profile_metrics_ingestion_coverage_text,
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


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Profile metrics lookback in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        help="Limit report to one platform.",
    )
    parser.add_argument(
        "--expected-interval-hours",
        type=_positive_float,
        default=DEFAULT_EXPECTED_INTERVAL_HOURS,
        help=(
            "Expected maximum hours between samples "
            f"(default: {DEFAULT_EXPECTED_INTERVAL_HOURS:g})."
        ),
    )
    parser.add_argument(
        "--max-stale-hours",
        type=_positive_float,
        default=DEFAULT_MAX_STALE_HOURS,
        help=(
            "Maximum latest sample age before status is stale "
            f"(default: {DEFAULT_MAX_STALE_HOURS:g})."
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
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_profile_metrics_ingestion_coverage_report(
                db,
                days=args.days,
                platform=args.platform,
                expected_interval_hours=args.expected_interval_hours,
                max_stale_hours=args.max_stale_hours,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_profile_metrics_ingestion_coverage_json(report))
    else:
        print(format_profile_metrics_ingestion_coverage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
