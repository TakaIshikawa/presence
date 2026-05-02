#!/usr/bin/env python3
"""Export reusable pull quotes from generated blog posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context  # noqa: E402
from synthesis.blog_pull_quote_export import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_CHARS,
    DEFAULT_MIN_CHARS,
    export_blog_pull_quotes,
    format_blog_pull_quotes_json,
    format_blog_pull_quotes_text,
)


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


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
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for generated blog posts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--min-chars",
        type=_positive_int,
        default=DEFAULT_MIN_CHARS,
        help=f"Minimum quote length in characters (default: {DEFAULT_MIN_CHARS}).",
    )
    parser.add_argument(
        "--max-chars",
        type=_positive_int,
        default=DEFAULT_MAX_CHARS,
        help=f"Maximum quote length in characters (default: {DEFAULT_MAX_CHARS}).",
    )
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum pull quotes to export (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--create-ideas",
        action="store_true",
        help="Write deduplicated content_ideas rows. Default mode only reports candidates.",
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
            results = export_blog_pull_quotes(
                db,
                days=args.days,
                min_chars=args.min_chars,
                max_chars=args.max_chars,
                limit=args.limit,
                create_ideas=args.create_ideas,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_blog_pull_quotes_json(results))
    else:
        print(format_blog_pull_quotes_text(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
