#!/usr/bin/env python3
"""Report suspicious engagement metric changes for published posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.engagement_anomaly_report import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_JUMP_THRESHOLDS,
    DEFAULT_LIMIT,
    DEFAULT_RATE_THRESHOLDS_PER_HOUR,
    METRIC_NAMES,
    build_engagement_anomaly_report,
    format_engagement_anomaly_report_json,
    format_engagement_anomaly_report_text,
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


def _non_negative_number(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid number: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for engagement snapshots (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum anomalies to print (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with status 1 when engagement anomalies are found.",
    )
    for metric in METRIC_NAMES:
        parser.add_argument(
            f"--{metric}-jump-threshold",
            type=_non_negative_number,
            default=DEFAULT_JUMP_THRESHOLDS[metric],
            help=(
                f"Flag one-fetch {metric} increases above this value "
                f"(default: {DEFAULT_JUMP_THRESHOLDS[metric]:g})."
            ),
        )
        parser.add_argument(
            f"--{metric}-rate-threshold",
            type=_non_negative_number,
            default=DEFAULT_RATE_THRESHOLDS_PER_HOUR[metric],
            help=(
                f"Flag {metric} growth above this per-hour rate "
                f"(default: {DEFAULT_RATE_THRESHOLDS_PER_HOUR[metric]:g})."
            ),
        )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    jump_thresholds = {
        metric: getattr(args, f"{metric}_jump_threshold")
        for metric in METRIC_NAMES
    }
    rate_thresholds = {
        metric: getattr(args, f"{metric}_rate_threshold")
        for metric in METRIC_NAMES
    }
    try:
        with script_context() as (_config, db):
            report = build_engagement_anomaly_report(
                db,
                days=args.days,
                limit=args.limit,
                jump_thresholds=jump_thresholds,
                rate_thresholds_per_hour=rate_thresholds,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_engagement_anomaly_report_json(report))
    else:
        print(format_engagement_anomaly_report_text(report))
    if args.fail_on_issues and report.has_issues:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
