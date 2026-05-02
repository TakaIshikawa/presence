#!/usr/bin/env python3
"""Report missing or stale knowledge embeddings for backfill planning."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.embedding_coverage_report import (  # noqa: E402
    build_knowledge_embedding_coverage_report,
    format_knowledge_embedding_coverage_json,
    format_knowledge_embedding_coverage_text,
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
    parser.add_argument(
        "--limit",
        type=_non_negative_int,
        help="Include up to this many representative missing and stale item IDs.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            report = build_knowledge_embedding_coverage_report(
                db,
                limit=args.limit,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_knowledge_embedding_coverage_text(report))
    else:
        print(format_knowledge_embedding_coverage_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
