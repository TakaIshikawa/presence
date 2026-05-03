#!/usr/bin/env python3
"""Report likely duplicate knowledge source URLs after local canonicalization."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.source_duplicate_urls import (  # noqa: E402
    DEFAULT_LIMIT,
    build_knowledge_duplicate_url_report,
    build_knowledge_duplicate_url_report_from_fixture,
    format_knowledge_duplicate_url_json,
    format_knowledge_duplicate_url_text,
)
from runner import script_context  # noqa: E402


def _nonnegative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid integer: {value}") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be nonnegative")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--fixture",
        type=Path,
        help="Read knowledge source records from JSON array or JSONL fixture.",
    )
    parser.add_argument("--source-type", help="Only include one knowledge source_type.")
    parser.add_argument(
        "--limit",
        type=_nonnegative_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum duplicate clusters to emit; 0 means no limit (default: {DEFAULT_LIMIT}).",
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
        try:
            args = parse_args(argv)
        except SystemExit as exc:
            return int(exc.code or 0)
        limit = None if args.limit == 0 else args.limit
        if args.fixture:
            report = build_knowledge_duplicate_url_report_from_fixture(
                args.fixture,
                source_type=args.source_type,
                limit=limit,
            )
        elif args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_knowledge_duplicate_url_report(
                    conn,
                    source_type=args.source_type,
                    limit=limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_knowledge_duplicate_url_report(
                    db,
                    source_type=args.source_type,
                    limit=limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_knowledge_duplicate_url_json(report))
    else:
        print(format_knowledge_duplicate_url_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
