#!/usr/bin/env python3
"""Report pending reply drafts with stale relationship context."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_stale_context_report import (  # noqa: E402
    DEFAULT_MAX_AGE_DAYS,
    build_reply_stale_context_report,
    format_reply_stale_context_json,
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
        "--max-age-days",
        type=_positive_int,
        default=DEFAULT_MAX_AGE_DAYS,
        help=f"Maximum trusted context age in days (default: {DEFAULT_MAX_AGE_DAYS}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        with script_context() as (_config, db):
            rows = list_pending_reply_drafts(db)
            report = build_reply_stale_context_report(
                rows,
                max_age_days=args.max_age_days,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    print(format_reply_stale_context_json(report))
    return 0


def list_pending_reply_drafts(db_or_conn: Any) -> list[dict[str, Any]]:
    """Load pending reply_queue rows for stale context reporting."""

    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return []

    query = "SELECT * FROM reply_queue"
    params: list[Any] = []
    if "status" in columns:
        query += " WHERE LOWER(COALESCE(status, 'pending')) = 'pending'"
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "platform" in columns:
        parts.append("platform ASC")
    if "id" in columns:
        parts.append("id ASC")
    else:
        parts.append("rowid ASC")
    return ", ".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
