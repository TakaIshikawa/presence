#!/usr/bin/env python3
"""Audit generated visual posts for missing or weak alt text."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.alt_text_quality_report import (  # noqa: E402
    DEFAULT_DAYS,
    STATUSES,
    build_alt_text_quality_report,
    format_alt_text_quality_json,
    format_alt_text_quality_text,
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
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for generated visual posts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--status",
        choices=STATUSES,
        help="Only include rows with this status.",
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
            rows = build_alt_text_quality_report(db, days=args.days, status=args.status)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_alt_text_quality_json(rows))
    else:
        print(format_alt_text_quality_text(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
