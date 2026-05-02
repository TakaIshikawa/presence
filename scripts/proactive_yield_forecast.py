#!/usr/bin/env python3
"""Forecast expected engagement yield for pending proactive actions."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.proactive_yield_forecast import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MIN_SCORE,
    build_proactive_yield_forecast_report,
    format_proactive_yield_forecast_json,
    format_proactive_yield_forecast_text,
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
        raise argparse.ArgumentTypeError(f"invalid float: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by proactive action creation time (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum pending proactive actions to emit (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--min-score",
        type=_non_negative_float,
        default=DEFAULT_MIN_SCORE,
        help=f"Minimum forecast score to include (default: {DEFAULT_MIN_SCORE:g}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        with script_context() as (_config, db):
            report = build_proactive_yield_forecast_report(
                db,
                days=args.days,
                limit=args.limit,
                min_score=args.min_score,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_proactive_yield_forecast_json(report))
    else:
        print(format_proactive_yield_forecast_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
