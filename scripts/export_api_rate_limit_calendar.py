#!/usr/bin/env python3
"""Export an operational API rate-limit reset calendar."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.api_rate_limit_calendar import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STALE_AFTER_MINUTES,
    build_api_rate_limit_calendar,
    format_api_rate_limit_calendar_json,
    format_api_rate_limit_calendar_text,
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
        "--provider",
        help="Filter to one API provider.",
    )
    parser.add_argument(
        "--endpoint",
        help="Filter to one endpoint/resource.",
    )
    parser.add_argument(
        "--stale-after-minutes",
        type=_positive_int,
        default=DEFAULT_STALE_AFTER_MINUTES,
        help=(
            "Mark snapshots older than this many minutes as stale "
            f"(default: {DEFAULT_STALE_AFTER_MINUTES})."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum calendar rows to output (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        with script_context() as (_config, db):
            report = build_api_rate_limit_calendar(
                db,
                provider=args.provider,
                endpoint=args.endpoint,
                stale_after_minutes=args.stale_after_minutes,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_api_rate_limit_calendar_json(report))
    else:
        print(format_api_rate_limit_calendar_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
