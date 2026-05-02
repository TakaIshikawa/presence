#!/usr/bin/env python3
"""Recommend future publish windows from historical engagement."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.publish_window_recommendations import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SAMPLES,
    build_publish_window_recommendation_report,
    format_publish_window_recommendations_json,
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
        help=f"Published-post lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-samples",
        type=_positive_int,
        default=DEFAULT_MIN_SAMPLES,
        help=(
            "Minimum published samples required for a weekday/hour recommendation "
            f"(default: {DEFAULT_MIN_SAMPLES})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum recommendations to emit (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_publish_window_recommendation_report(
                db,
                days=args.days,
                min_samples=args.min_samples,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_publish_window_recommendations_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
