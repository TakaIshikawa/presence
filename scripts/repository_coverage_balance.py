#!/usr/bin/env python3
"""Report repository coverage balance for published content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.repository_coverage_balance import (  # noqa: E402
    DEFAULT_DELTA_THRESHOLD,
    DEFAULT_LOOKBACK_DAYS,
    build_repository_coverage_balance_report,
    format_repository_coverage_balance_json,
    format_repository_coverage_balance_table,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lookback-days", type=_positive_int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--delta-threshold", type=_non_negative_float, default=DEFAULT_DELTA_THRESHOLD)
    parser.add_argument("--format", choices=("json", "table"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_repository_coverage_balance_report(
                db,
                lookback_days=args.lookback_days,
                delta_threshold=args.delta_threshold,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_table = args.table or args.format == "table"
    print(format_repository_coverage_balance_table(report) if as_table else format_repository_coverage_balance_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
