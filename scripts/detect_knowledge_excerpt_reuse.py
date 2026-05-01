#!/usr/bin/env python3
"""Detect generated content that reuses long knowledge excerpts."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.excerpt_reuse import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_MIN_TOKENS,
    DEFAULT_SIMILARITY_THRESHOLD,
    build_knowledge_excerpt_reuse_report,
    format_knowledge_excerpt_reuse_json,
    format_knowledge_excerpt_reuse_text,
)
from runner import script_context  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument(
        "--min-tokens",
        type=int,
        default=DEFAULT_MIN_TOKENS,
        help=f"Minimum contiguous overlapping tokens to flag (default: {DEFAULT_MIN_TOKENS}).",
    )
    parser.add_argument(
        "--similarity-threshold",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=(
            "Minimum longest-span similarity to flag "
            f"(default: {DEFAULT_SIMILARITY_THRESHOLD:g})."
        ),
    )
    parser.add_argument(
        "--format",
        choices=("text", "json"),
        default="text",
        help="Output format (default: text).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum findings to include (default: {DEFAULT_LIMIT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_knowledge_excerpt_reuse_report(
                    conn,
                    min_tokens=args.min_tokens,
                    similarity_threshold=args.similarity_threshold,
                    limit=args.limit,
                )
        else:
            with script_context() as (_config, db):
                report = build_knowledge_excerpt_reuse_report(
                    db,
                    min_tokens=args.min_tokens,
                    similarity_threshold=args.similarity_threshold,
                    limit=args.limit,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "json":
        print(format_knowledge_excerpt_reuse_json(report))
    else:
        print(format_knowledge_excerpt_reuse_text(report))

    return 1 if report.findings else 0


if __name__ == "__main__":
    raise SystemExit(main())
