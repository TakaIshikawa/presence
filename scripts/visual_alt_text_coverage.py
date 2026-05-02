#!/usr/bin/env python3
"""Audit generated visual content for missing or weak alt text."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.visual_alt_text_coverage import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_CHARS,
    build_visual_alt_text_coverage_report,
    format_visual_alt_text_coverage_json,
    format_visual_alt_text_coverage_text,
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
        help=f"Lookback window in days for generated visual content (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-chars",
        type=_positive_int,
        default=DEFAULT_MIN_CHARS,
        help=f"Minimum alt text characters before reporting too-short text (default: {DEFAULT_MIN_CHARS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_visual_alt_text_coverage_report(
                db,
                days=args.days,
                min_chars=args.min_chars,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_visual_alt_text_coverage_json(report))
    else:
        print(format_visual_alt_text_coverage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
