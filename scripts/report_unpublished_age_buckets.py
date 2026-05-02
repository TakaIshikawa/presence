#!/usr/bin/env python3
"""Report generated content stuck before publication attempts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.unpublished_age_buckets import (  # noqa: E402
    DEFAULT_MIN_AGE_HOURS,
    DEFAULT_THRESHOLDS_HOURS,
    build_unpublished_age_bucket_report,
    format_unpublished_age_bucket_json,
    format_unpublished_age_bucket_markdown,
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
    parser.add_argument(
        "--min-age-hours",
        type=_non_negative_float,
        default=DEFAULT_MIN_AGE_HOURS,
        help=f"Minimum content age to include (default: {DEFAULT_MIN_AGE_HOURS:g}).",
    )
    parser.add_argument(
        "--threshold-hours",
        action="append",
        type=_positive_float,
        dest="thresholds_hours",
        help=(
            "Age bucket threshold in hours. Repeat for multiple thresholds "
            f"(default: {', '.join(f'{value:g}' for value in DEFAULT_THRESHOLDS_HOURS)})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_unpublished_age_bucket_report(
                db,
                min_age_hours=args.min_age_hours,
                thresholds_hours=args.thresholds_hours or DEFAULT_THRESHOLDS_HOURS,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "markdown":
        print(format_unpublished_age_bucket_markdown(report))
    else:
        print(format_unpublished_age_bucket_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
