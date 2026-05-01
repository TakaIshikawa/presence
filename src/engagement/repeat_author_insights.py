"""Aggregate repeat audience members across reply and proactive queues."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_COUNT = 2
DEFAULT_STALE_DAYS = 14


def build_repeat_author_insights_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return repeat author insights without mutating queue tables."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    stale_cutoff = now - timedelta(days=stale_days)

    rows = _reply_rows(conn, cutoff) + _proactive_rows(conn, cutoff)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        handle = normalize_handle(row.get("raw_handle"))
        if handle:
            grouped[handle].append(row)

    authors = [
        _author_summary(handle, matches, min_count=min_count, stale_cutoff=stale_cutoff)
        for handle, matches in grouped.items()
    ]
    authors.sort(key=_author_sort_key)

    return {
        "generated_at": now.isoformat(),
        "lookback_days": days,
        "thresholds": {
            "min_count": min_count,
            "stale_days": stale_days,
        },
        "totals": {
            "authors": len(authors),
            "interactions": sum(item["total_count"] for item in authors),
            "reply_queue": sum(item["source_counts"].get("reply_queue", 0) for item in authors),
            "proactive_actions": sum(
                item["source_counts"].get("proactive_actions", 0) for item in authors
            ),
        },
        "classification_counts": dict(
            sorted(Counter(item["classification"] for item in authors).items())
        ),
        "authors": authors,
    }


def format_repeat_author_insights_json(report: dict[str, Any]) -> str:
    """Format a repeat-author report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_repeat_author_insights_text(report: dict[str, Any]) -> str:
    """Format repeat-author insights for lightweight terminal review."""
    lines = [
        "Repeat reply author insights",
        (
            f"Authors: {report['totals']['authors']} "
            f"interactions={report['totals']['interactions']} "
            f"reply_queue={report['totals']['reply_queue']} "
            f"proactive_actions={report['totals']['proactive_actions']}"
        ),
        (
            f"Lookback: {report['lookback_days']}d "
            f"min_count={report['thresholds']['min_count']} "
            f"stale_days={report['thresholds']['stale_days']}"
        ),
        "",
    ]
    if not report["authors"]:
        lines.append("No repeat authors matched.")
        return "\n".join(lines).rstrip()

    for author in report["authors"]:
        handles = ", ".join(f"@{handle.lstrip('@')}" for handle in author["raw_handles"])
        lines.append(
            f"@{author['normalized_handle']} {author['classification']} "
            f"count={author['total_count']} latest={author['latest_seen_at'] or 'unknown'}"
        )
        lines.append(f"  handles: {handles or 'unknown'}")
        lines.append(
            "  sources: "
            + ", ".join(
                f"{source}={count}"
                for source, count in sorted(author["source_counts"].items())
            )
        )
        lines.append(
            "  statuses: "
            + ", ".join(
                f"{status}={count}"
                for status, count in sorted(author["status_counts"].items())
            )
        )
    return "\n".join(lines).rstrip()


def normalize_handle(handle: Any) -> str | None:
    """Normalize handles for case-insensitive aggregation."""
    normalized = str(handle or "").strip().lstrip("@").casefold()
    return normalized or None


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _reply_rows(conn: sqlite3.Connection, cutoff: datetime) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "reply_queue")
    if not columns or "inbound_author_handle" not in columns:
        return []

    rows = _fetch_all(conn, "reply_queue", columns, cutoff, ("posted_at", "reviewed_at", "detected_at"))
    return [
        {
            "source": "reply_queue",
            "platform": _value(row, columns, "platform") or "x",
            "status": _value(row, columns, "status") or "unknown",
            "raw_handle": _value(row, columns, "inbound_author_handle"),
            "seen_at": _latest_timestamp(
                _value(row, columns, "posted_at"),
                _value(row, columns, "reviewed_at"),
                _value(row, columns, "detected_at"),
            ),
        }
        for row in rows
    ]


def _proactive_rows(conn: sqlite3.Connection, cutoff: datetime) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "proactive_actions")
    if not columns or "target_author_handle" not in columns:
        return []

    rows = _fetch_all(
        conn,
        "proactive_actions",
        columns,
        cutoff,
        ("posted_at", "reviewed_at", "created_at"),
    )
    return [
        {
            "source": "proactive_actions",
            "platform": _platform_from_metadata(_value(row, columns, "platform_metadata")),
            "status": _value(row, columns, "status") or "unknown",
            "raw_handle": _value(row, columns, "target_author_handle"),
            "seen_at": _latest_timestamp(
                _value(row, columns, "posted_at"),
                _value(row, columns, "reviewed_at"),
                _value(row, columns, "created_at"),
            ),
        }
        for row in rows
    ]


def _fetch_all(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    cutoff: datetime,
    timestamp_columns: tuple[str, ...],
) -> list[dict[str, Any]]:
    available = [column for column in timestamp_columns if column in columns]
    if available:
        timestamp_expr = "COALESCE(" + ", ".join(available) + ")"
        query = (
            f"SELECT * FROM {table} "
            f"WHERE datetime({timestamp_expr}) >= datetime(?) "
            f"ORDER BY datetime({timestamp_expr}) DESC"
        )
        params: tuple[Any, ...] = (cutoff.isoformat(),)
    else:
        query = f"SELECT * FROM {table}"
        params = ()
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _author_summary(
    handle: str,
    matches: list[dict[str, Any]],
    *,
    min_count: int,
    stale_cutoff: datetime,
) -> dict[str, Any]:
    latest = _latest_timestamp(*(match.get("seen_at") for match in matches))
    latest_dt = _parse_datetime(latest)
    classification = "active"
    if latest_dt is not None and latest_dt < stale_cutoff:
        classification = "stale"
    elif len(matches) < min_count:
        classification = "emerging"

    return {
        "normalized_handle": handle,
        "raw_handles": _raw_handles(matches),
        "total_count": len(matches),
        "platform_counts": dict(sorted(Counter(match["platform"] for match in matches).items())),
        "source_counts": dict(sorted(Counter(match["source"] for match in matches).items())),
        "status_counts": dict(sorted(Counter(match["status"] for match in matches).items())),
        "latest_seen_at": latest,
        "classification": classification,
    }


def _raw_handles(matches: list[dict[str, Any]]) -> list[str]:
    handles = {
        str(match.get("raw_handle")).strip()
        for match in matches
        if str(match.get("raw_handle") or "").strip()
    }
    return sorted(handles, key=lambda value: value.casefold())


def _author_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    classification_rank = {"active": 0, "emerging": 1, "stale": 2}
    latest_dt = _parse_datetime(item.get("latest_seen_at"))
    latest_ts = latest_dt.timestamp() if latest_dt else 0.0
    return (
        classification_rank.get(item["classification"], 9),
        -int(item["total_count"]),
        -latest_ts,
        item["normalized_handle"],
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _value(row: dict[str, Any], columns: set[str], column: str) -> Any:
    if column not in columns:
        return None
    return row.get(column)


def _latest_timestamp(*values: Any) -> str | None:
    parsed = [
        (dt, str(value))
        for value in values
        if value
        for dt in [_parse_datetime(value)]
        if dt is not None
    ]
    if not parsed:
        return None
    return max(parsed, key=lambda item: item[0])[0].isoformat()


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for parser in (
        lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        return _as_utc(parsed)
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _platform_from_metadata(value: Any) -> str:
    if not value:
        return "x"
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return "x"
    if isinstance(parsed, dict):
        platform = str(parsed.get("platform") or "").strip()
        return platform or "x"
    return "x"
