#!/usr/bin/env python3
"""Report whether reply drafts used available relationship and action context."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_context_usage_coverage import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_LOW_COVERAGE_THRESHOLD,
    build_reply_context_usage_coverage_report_from_db,
    format_reply_context_usage_coverage_json,
    format_reply_context_usage_coverage_text,
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


def _ratio(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ratio: {value}") from exc
    if not 0 <= parsed <= 1:
        raise argparse.ArgumentTypeError("ratio must be between 0 and 1")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--low-coverage-threshold", type=_ratio, default=DEFAULT_LOW_COVERAGE_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true", help="Print the human-readable table output.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_reply_context_usage_coverage_report_from_db(
                db,
                low_coverage_threshold=args.low_coverage_threshold,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    as_text = args.table or args.format == "text"
    print(format_reply_context_usage_coverage_text(report) if as_text else format_reply_context_usage_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
