#!/usr/bin/env python3
"""Report generated blog posts missing publication metadata."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_publication_metadata_gaps import (  # noqa: E402
    DEFAULT_DAYS,
    build_blog_publication_metadata_gap_report,
    format_blog_publication_metadata_gaps_json,
    format_blog_publication_metadata_gaps_text,
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
        help=f"Only inspect published blog posts from this many days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--fail-on-issues",
        action="store_true",
        help="Exit with status 1 when metadata gaps are found.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_blog_publication_metadata_gap_report(db, days=args.days)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.format == "json":
        print(format_blog_publication_metadata_gaps_json(report))
    else:
        print(format_blog_publication_metadata_gaps_text(report))
    if args.fail_on_issues and report.totals["gaps_found"] > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
