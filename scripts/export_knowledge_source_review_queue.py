#!/usr/bin/env python3
"""Export knowledge source review queue."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.knowledge_source_review_queue_export import DEFAULT_LIMIT, build_knowledge_source_review_queue_export_from_db, format_knowledge_source_review_queue_export_csv, format_knowledge_source_review_queue_export_json  # noqa: E402
from runner import script_context  # noqa: E402


def _positive_int(v: str) -> int:
    parsed = int(v)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db")
    p.add_argument("--format", choices=("json", "csv"), default="json")
    p.add_argument("--min-priority", type=int, default=1)
    p.add_argument("--reason")
    p.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    return p.parse_args(argv)


def main(argv=None) -> int:
    try:
        a = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        if a.db:
            with sqlite3.connect(a.db) as conn:
                conn.row_factory = sqlite3.Row
                export = build_knowledge_source_review_queue_export_from_db(conn, min_priority=a.min_priority, reason=a.reason, limit=a.limit)
        else:
            with script_context() as (_config, db):
                export = build_knowledge_source_review_queue_export_from_db(db, min_priority=a.min_priority, reason=a.reason, limit=a.limit)
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_knowledge_source_review_queue_export_csv(export) if a.format == "csv" else format_knowledge_source_review_queue_export_json(export), end="" if a.format == "csv" else "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
