#!/usr/bin/env python3
"""Export suggested meta descriptions for generated blog posts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_meta_description_export import (  # noqa: E402
    DEFAULT_MAX_CHARS,
    DEFAULT_MIN_CHARS,
    export_blog_meta_descriptions,
    export_blog_meta_descriptions_from_markdown,
    format_blog_meta_descriptions_csv,
    format_blog_meta_descriptions_json,
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
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    source.add_argument(
        "--markdown",
        action="append",
        type=Path,
        dest="markdown_paths",
        help="Markdown draft file to export. Repeat for multiple files.",
    )
    parser.add_argument(
        "--min-chars",
        type=_positive_int,
        default=DEFAULT_MIN_CHARS,
        help=f"Minimum target description length (default: {DEFAULT_MIN_CHARS}).",
    )
    parser.add_argument(
        "--max-chars",
        type=_positive_int,
        default=DEFAULT_MAX_CHARS,
        help=f"Maximum description length (default: {DEFAULT_MAX_CHARS}).",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.markdown_paths:
            rows = export_blog_meta_descriptions_from_markdown(
                args.markdown_paths,
                min_chars=args.min_chars,
                max_chars=args.max_chars,
            )
        elif args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                rows = export_blog_meta_descriptions(
                    conn,
                    min_chars=args.min_chars,
                    max_chars=args.max_chars,
                )
        else:
            with script_context() as (_config, db):
                rows = export_blog_meta_descriptions(
                    db,
                    min_chars=args.min_chars,
                    max_chars=args.max_chars,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_blog_meta_descriptions_csv(rows))
    else:
        print(format_blog_meta_descriptions_json(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
