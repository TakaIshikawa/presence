#!/usr/bin/env python3
"""Export short attributable quote candidates from curated knowledge sources."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.curated_quote_extract import (  # noqa: E402
    DEFAULT_MAX_CHARS,
    DEFAULT_MIN_CHARS,
    extract_quote_candidates,
    format_curated_quotes_csv,
    format_curated_quotes_jsonl,
    load_curated_source_records,
    load_fixture_records_from_paths,
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
        "fixtures",
        nargs="*",
        type=Path,
        help="Optional JSON or JSONL fixture files with source_id/title/url/text fields.",
    )
    parser.add_argument(
        "--format",
        choices=("jsonl", "csv"),
        default="jsonl",
        help="Output format (default: jsonl).",
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
        "--include-unapproved",
        action="store_true",
        help="Include unapproved knowledge rows when reading from the database.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.fixtures:
            records = load_fixture_records_from_paths(args.fixtures)
        else:
            with script_context() as (_config, db):
                records = load_curated_source_records(
                    db,
                    include_unapproved=args.include_unapproved,
                )
        candidates = extract_quote_candidates(
            records,
            min_chars=args.min_chars,
            max_chars=args.max_chars,
        )
    except (OSError, TypeError, ValueError, json.JSONDecodeError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "csv":
        print(format_curated_quotes_csv(candidates))
    else:
        output = format_curated_quotes_jsonl(candidates)
        if output:
            print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
