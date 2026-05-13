#!/usr/bin/env python3
"""Report knowledge ingest recovery gaps."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.ingest_gap_recovery import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_FAILURE_THRESHOLD,
    DEFAULT_LIMIT,
    build_knowledge_ingest_gap_recovery_report,
    format_knowledge_ingest_gap_recovery_json,
    format_knowledge_ingest_gap_recovery_text,
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
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--days", type=_positive_int, default=DEFAULT_DAYS)
    parser.add_argument("--failure-threshold", type=_positive_int, default=DEFAULT_FAILURE_THRESHOLD)
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--json", action="store_true")
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
                report = build_knowledge_ingest_gap_recovery_report(conn, days=args.days, failure_threshold=args.failure_threshold, limit=args.limit)
        else:
            with script_context() as (_config, db):
                report = build_knowledge_ingest_gap_recovery_report(db, days=args.days, failure_threshold=args.failure_threshold, limit=args.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_knowledge_ingest_gap_recovery_json(report) if args.json else format_knowledge_ingest_gap_recovery_text(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
