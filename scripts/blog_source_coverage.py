#!/usr/bin/env python3
"""Report source artifact coverage for generated blog drafts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_source_coverage import (  # noqa: E402
    DEFAULT_MIN_SOURCES,
    build_blog_source_coverage_report,
    format_blog_source_coverage_json,
    format_blog_source_coverage_markdown,
)
from runner import script_context  # noqa: E402


def _non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--min-sources",
        type=_non_negative_int,
        default=DEFAULT_MIN_SOURCES,
        help=f"Minimum backing source artifacts per draft (default: {DEFAULT_MIN_SOURCES}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)

    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_blog_source_coverage_report(
                    conn,
                    min_sources=args.min_sources,
                )
        else:
            with script_context() as (_config, db):
                report = build_blog_source_coverage_report(
                    db,
                    min_sources=args.min_sources,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "markdown":
        print(format_blog_source_coverage_markdown(report))
    else:
        print(format_blog_source_coverage_json(report))
    return 1 if report.blocking_issue_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
