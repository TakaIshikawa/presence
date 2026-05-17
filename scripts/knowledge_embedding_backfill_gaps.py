#!/usr/bin/env python3
"""Report knowledge embedding backfill gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.knowledge_embedding_backfill_gaps import (  # noqa: E402
    DEFAULT_EXPECTED_MODEL,
    DEFAULT_LIMIT,
    build_knowledge_embedding_backfill_gaps_report_from_db,
    format_knowledge_embedding_backfill_gaps_json,
    format_knowledge_embedding_backfill_gaps_text,
)
from runner import script_context  # noqa: E402


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-model", default=DEFAULT_EXPECTED_MODEL)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--table", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        with script_context() as (_config, db):
            report = build_knowledge_embedding_backfill_gaps_report_from_db(
                db, expected_model=args.expected_model, limit=args.limit
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(
        format_knowledge_embedding_backfill_gaps_text(report)
        if args.table or args.format == "text"
        else format_knowledge_embedding_backfill_gaps_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
