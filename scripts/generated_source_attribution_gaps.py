#!/usr/bin/env python3
"""Report generated content rows without durable source attribution."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.generated_source_attribution_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    build_generated_source_attribution_gaps_report,
    format_generated_source_attribution_gaps_json,
    format_generated_source_attribution_gaps_text,
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
        help=f"Classify unpublished source-less content stale after this many days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument("--content-type", help="Only include this generated_content.content_type.")
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
            report = build_generated_source_attribution_gaps_report(
                db,
                days=args.days,
                content_type=args.content_type,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_generated_source_attribution_gaps_json(report))
    else:
        print(format_generated_source_attribution_gaps_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
