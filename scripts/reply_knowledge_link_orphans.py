#!/usr/bin/env python3
"""Report orphaned and invalid reply knowledge links."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_knowledge_link_orphans import (  # noqa: E402
    DEFAULT_LIMIT,
    DEFAULT_STATUS,
    build_reply_knowledge_link_orphans_report_from_db,
    format_reply_knowledge_link_orphans_json,
    format_reply_knowledge_link_orphans_text,
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
    parser.add_argument("--limit", type=_positive_int, default=DEFAULT_LIMIT)
    parser.add_argument("--status", default=DEFAULT_STATUS)
    parser.add_argument("--format", choices=("json", "text"), default="json")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_reply_knowledge_link_orphans_report_from_db(
                    conn,
                    limit=args.limit,
                    status=args.status,
                )
        else:
            with script_context() as (_config, db):
                report = build_reply_knowledge_link_orphans_report_from_db(
                    db,
                    limit=args.limit,
                    status=args.status,
                )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(
        format_reply_knowledge_link_orphans_text(report)
        if args.format == "text"
        else format_reply_knowledge_link_orphans_json(report)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
