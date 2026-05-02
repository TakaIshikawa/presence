#!/usr/bin/env python3
"""Report reply drafts that need relationship context enrichment."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any, Sequence

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.reply_context_gap_report import (  # noqa: E402
    DEFAULT_DAYS,
    DEFAULT_LIMIT,
    DEFAULT_MAX_INTERACTION_AGE_DAYS,
    DEFAULT_STATUS,
    SEVERITY_RANK,
    build_reply_context_gap_report,
    format_reply_context_gap_report_json,
    format_reply_context_gap_report_text,
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


def _severity(value: str) -> str:
    parsed = value.strip().casefold()
    if parsed not in SEVERITY_RANK:
        raise argparse.ArgumentTypeError(
            "severity must be one of: " + ", ".join(sorted(SEVERITY_RANK))
        )
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=_positive_int,
        default=DEFAULT_DAYS,
        help=f"Lookback window in days for inbound mentions and drafts (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Platform to report. Repeat for multiple platforms. Defaults to all platforms.",
    )
    parser.add_argument(
        "--status",
        action="append",
        help=(
            "Reply status to include. Repeat for multiple statuses. "
            f"Defaults to: {', '.join(DEFAULT_STATUS)}."
        ),
    )
    parser.add_argument(
        "--limit",
        type=_positive_int,
        default=DEFAULT_LIMIT,
        help=f"Maximum reply records to scan (default: {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--max-interaction-age-days",
        type=_positive_int,
        default=DEFAULT_MAX_INTERACTION_AGE_DAYS,
        help=(
            "Flag last interactions older than this many days "
            f"(default: {DEFAULT_MAX_INTERACTION_AGE_DAYS})."
        ),
    )
    parser.add_argument(
        "--min-severity",
        type=_severity,
        help="Only emit findings at this severity or higher.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "text"),
        default="json",
        help="Output format (default: json).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        statuses = tuple(args.status or DEFAULT_STATUS)
        platforms = _normalise(args.platform or ())
        with script_context() as (_config, db):
            rows, missing_tables = list_reply_context_gap_records(
                db,
                days=args.days,
                platforms=platforms,
                statuses=statuses,
                limit=args.limit,
            )
            report = build_reply_context_gap_report(
                rows,
                max_interaction_age_days=args.max_interaction_age_days,
                min_severity=args.min_severity,
                filters={
                    "days": args.days,
                    "platform": list(platforms),
                    "status": list(statuses),
                    "limit": args.limit,
                },
                missing_tables=missing_tables,
            )
    except (OSError, sqlite3.Error, TypeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.format == "text":
        print(format_reply_context_gap_report_text(report))
    else:
        print(format_reply_context_gap_report_json(report))
    return 1 if report.blocking_issue_count else 0


def list_reply_context_gap_records(
    db_or_conn: Any,
    *,
    days: int,
    platforms: Sequence[str],
    statuses: Sequence[str],
    limit: int,
) -> tuple[list[dict[str, Any]], tuple[str, ...]]:
    """Load reply_queue rows for relationship context gap reporting."""
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return [], ("reply_queue",)

    where: list[str] = []
    params: list[Any] = []
    if "status" in columns and statuses:
        normalised_statuses = _normalise(statuses)
        placeholders = ",".join("?" for _ in normalised_statuses)
        where.append(f"LOWER(COALESCE(status, 'pending')) IN ({placeholders})")
        params.extend(normalised_statuses)
    if "platform" in columns and platforms:
        placeholders = ",".join("?" for _ in platforms)
        where.append(f"LOWER(COALESCE(platform, '')) IN ({placeholders})")
        params.extend(platforms)
    if "detected_at" in columns:
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime('now', ?))")
        params.append(f"-{days} days")

    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    query += " LIMIT ?"
    params.append(limit)
    return [dict(row) for row in conn.execute(query, params).fetchall()], ()


def _normalise(values: Sequence[str]) -> tuple[str, ...]:
    return tuple(sorted({value.strip().casefold() for value in values if value.strip()}))


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row[1])
        for row in conn.execute(
            f"PRAGMA table_info({_quote_identifier(table)})"
        ).fetchall()
    }


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    if "id" in columns:
        parts.append("id ASC")
    else:
        parts.append("rowid ASC")
    return ", ".join(parts)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


if __name__ == "__main__":
    raise SystemExit(main())
