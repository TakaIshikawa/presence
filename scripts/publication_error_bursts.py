#!/usr/bin/env python3
"""Detect short-window bursts of publication failures."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.publication_error_bursts import (  # noqa: E402
    DEFAULT_HOURS,
    DEFAULT_MIN_CONSECUTIVE,
    DEFAULT_MIN_FAILURES,
    PLATFORMS,
    build_publication_error_burst_report,
    format_publication_error_burst_json,
    format_publication_error_burst_text,
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
        "--hours",
        type=_positive_int,
        default=DEFAULT_HOURS,
        help=f"Number of hours to look back (default: {DEFAULT_HOURS}).",
    )
    parser.add_argument(
        "--min-failures",
        type=_positive_int,
        default=DEFAULT_MIN_FAILURES,
        help=(
            "Minimum failed attempts in a platform/category group "
            f"(default: {DEFAULT_MIN_FAILURES})."
        ),
    )
    parser.add_argument(
        "--min-consecutive",
        type=_positive_int,
        default=DEFAULT_MIN_CONSECUTIVE,
        help=(
            "Minimum consecutive failures before a success interrupts the streak "
            f"(default: {DEFAULT_MIN_CONSECUTIVE})."
        ),
    )
    parser.add_argument(
        "--platform",
        choices=("all", *PLATFORMS),
        default="all",
        help="Platform to include (default: all).",
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
            report = build_publication_error_burst_report(
                db,
                hours=args.hours,
                min_failures=args.min_failures,
                min_consecutive=args.min_consecutive,
                platform=args.platform,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_publication_error_burst_json(report))
    else:
        print(format_publication_error_burst_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
