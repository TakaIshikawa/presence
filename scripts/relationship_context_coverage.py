#!/usr/bin/env python3
"""Report Cultivate relationship context coverage for queued replies."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.relationship_context_coverage import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_MIN_STRENGTH,
    DEFAULT_STATUS,
    build_relationship_context_coverage_report,
    format_relationship_context_coverage_json,
    format_relationship_context_coverage_text,
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
        "--status",
        action="append",
        help=(
            "Reply status to include. Repeat for multiple statuses. "
            f"Defaults to: {', '.join(DEFAULT_STATUS)}."
        ),
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to include. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days by detected_at (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-strength",
        type=_non_negative_float,
        default=DEFAULT_MIN_STRENGTH,
        help=(
            "Flag relationship_strength below this value "
            f"(default: {DEFAULT_MIN_STRENGTH})."
        ),
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
            report = build_relationship_context_coverage_report(
                db,
                status=tuple(args.status or DEFAULT_STATUS),
                platform=tuple(args.platform or ()),
                days=args.days,
                min_strength=args.min_strength,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_relationship_context_coverage_json(report))
    else:
        print(format_relationship_context_coverage_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
