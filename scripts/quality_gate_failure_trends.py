#!/usr/bin/env python3
"""Report recent quality-gate failure trends for generated content."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.quality_gate_failure_trends import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_COUNT,
    build_quality_gate_failure_trends_report,
    format_quality_gate_failure_trends_json,
    format_quality_gate_failure_trends_text,
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
        help=f"Lookback window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-count",
        type=_positive_int,
        default=DEFAULT_MIN_COUNT,
        help=f"Minimum grouped failures to include (default: {DEFAULT_MIN_COUNT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit deterministic JSON. Equivalent to --format json.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        with script_context() as (_config, db):
            report = build_quality_gate_failure_trends_report(
                db,
                days=args.days,
                min_count=args.min_count,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json or args.format == "json":
        print(format_quality_gate_failure_trends_json(report))
    else:
        print(format_quality_gate_failure_trends_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
