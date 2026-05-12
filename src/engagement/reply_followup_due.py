"""Summarize reply follow-up reminders by due status."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
DEFAULT_LIMIT = 10
BUCKETS = ("overdue", "due_today", "upcoming", "done", "dismissed")
DONE_STATUSES = {"done", "completed", "sent", "resolved"}
DISMISSED_STATUSES = {"dismissed", "cancelled", "canceled", "expired"}
REQUIRED_REMINDER_COLUMNS = {
    "id",
    "target_handle",
    "source_type",
    "source_id",
    "due_at",
    "status",
    "reason",
}


def build_reply_followup_due_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return due-status counts and the oldest actionable follow-up reminders."""

    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days_ahead)
    filters = {
        "days_ahead": days_ahead,
        "limit": limit,
        "window_start": generated_at.isoformat(),
        "window_end": horizon_end.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "reply_followup_reminders" not in schema:
        return _empty_report(generated_at, filters, ["reply_followup_reminders"], {}, [])

    missing = sorted(REQUIRED_REMINDER_COLUMNS - schema["reply_followup_reminders"])
    if missing:
        return _empty_report(generated_at, filters, [], {"reply_followup_reminders": missing}, [])

    source_missing = [] if "reply_queue" in schema else ["reply_queue"]
    rows = _reminder_rows(conn, schema)
    items = [
        item
        for row in rows
        if (item := _build_item(row, now=generated_at, horizon_end=horizon_end)) is not None
    ]
    items.sort(key=_item_sort_key)
    representatives = [
        item["id"]
        for item in items
        if item["status_bucket"] in {"overdue", "due_today", "upcoming"}
    ][:limit]

    return {
        "artifact_type": "reply_followup_due",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "representative_ids": representatives,
        "items": items[:limit],
        "missing_tables": [],
        "missing_source_tables": source_missing,
        "missing_columns": {},
    }


def format_reply_followup_due_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_due_text(report: dict[str, Any]) -> str:
    """Render a compact operator view."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Reply Follow-up Due",
        f"Generated: {report['generated_at']}",
        f"Filters: days_ahead={filters['days_ahead']} limit={filters['limit']}",
        (
            "Status counts: "
            + " ".join(f"{bucket}={totals['by_status'][bucket]}" for bucket in BUCKETS)
            + f" total={totals['total']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_source_tables"):
        lines.append("Missing source tables: " + ", ".join(report["missing_source_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(formatted))
    if not report["items"]:
        lines.append("No reply follow-up reminders matched the due window.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Actionable reminders:")
    for item in report["items"]:
        source = f"{item['source_type']}:{item['source_id']}"
        target = item["target_handle"] or "-"
        due = item["due_at"] or "-"
        lines.append(
            f"- #{item['id']} {item['status_bucket']} @{target} due={due} "
            f"source={source} platform={item['platform']}"
        )
        if item.get("reason"):
            lines.append(f"  reason={item['reason']}")
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
    missing_source_tables: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_followup_due",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"total": 0, "by_status": {bucket: 0 for bucket in BUCKETS}},
        "representative_ids": [],
        "items": [],
        "missing_tables": missing_tables,
        "missing_source_tables": missing_source_tables,
        "missing_columns": missing_columns,
    }


def _reminder_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    reminder_columns = schema["reply_followup_reminders"]
    select_columns = [
        _column_expr("r", reminder_columns, "id"),
        _column_expr("r", reminder_columns, "target_handle"),
        _column_expr("r", reminder_columns, "source_type"),
        _column_expr("r", reminder_columns, "source_id"),
        _column_expr("r", reminder_columns, "source_reply_id"),
        _column_expr("r", reminder_columns, "due_at"),
        _column_expr("r", reminder_columns, "status"),
        _column_expr("r", reminder_columns, "reason"),
        _column_expr("r", reminder_columns, "notes"),
        _column_expr("r", reminder_columns, "created_at"),
    ]
    reply_columns = schema.get("reply_queue", set())
    join = ""
    if reply_columns:
        if "source_reply_id" in reminder_columns:
            join = "LEFT JOIN reply_queue q ON q.id = r.source_reply_id"
        else:
            join = (
                "LEFT JOIN reply_queue q "
                "ON q.id = r.source_id AND r.source_type = 'reply_queue'"
            )
        select_columns.extend(
            [
                _column_expr("q", reply_columns, "platform", "'unknown'", "reply_platform"),
                _column_expr("q", reply_columns, "inbound_author_handle"),
                _column_expr("q", reply_columns, "inbound_author_id"),
                _column_expr("q", reply_columns, "inbound_tweet_id"),
                _column_expr("q", reply_columns, "inbound_url"),
                _column_expr("q", reply_columns, "status", "NULL", "source_status"),
            ]
        )
    else:
        select_columns.extend(
            [
                "'unknown' AS reply_platform",
                "NULL AS inbound_author_handle",
                "NULL AS inbound_author_id",
                "NULL AS inbound_tweet_id",
                "NULL AS inbound_url",
                "NULL AS source_status",
            ]
        )
    query = (
        f"SELECT {', '.join(select_columns)} "
        f"FROM reply_followup_reminders r {join} "
        "ORDER BY datetime(r.due_at) ASC, r.id ASC"
    )
    cursor = conn.execute(query)
    names = [description[0] for description in cursor.description]
    return [_row_to_dict(row, names) for row in cursor.fetchall()]


def _build_item(
    row: dict[str, Any],
    *,
    now: datetime,
    horizon_end: datetime,
) -> dict[str, Any] | None:
    due_at = _parse_timestamp(row.get("due_at"))
    status = _clean_status(row.get("status")) or "pending"
    bucket = _bucket(status, due_at, now)
    if bucket is None:
        return None
    if bucket == "upcoming" and due_at and due_at > horizon_end:
        return None

    source_type = str(row.get("source_type") or "unknown")
    source_id = _int_or_none(row.get("source_id"))
    source_reply_id = _int_or_none(row.get("source_reply_id"))
    if source_reply_id is None and source_type == "reply_queue":
        source_reply_id = source_id
    target_handle = _clean_handle(row.get("inbound_author_handle") or row.get("target_handle"))

    return {
        "id": _int_or_none(row.get("id")),
        "status_bucket": bucket,
        "status": status,
        "target_handle": target_handle,
        "stored_target_handle": _clean_handle(row.get("target_handle")),
        "due_at": due_at.isoformat() if due_at else None,
        "created_at": row.get("created_at"),
        "source_type": source_type,
        "source_id": source_id,
        "source_reply_id": source_reply_id,
        "source_status": _clean_status(row.get("source_status")),
        "platform": str(row.get("reply_platform") or "unknown").strip().lower() or "unknown",
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "source_context": _source_context(row, source_reply_id),
    }


def _bucket(status: str, due_at: datetime | None, now: datetime) -> str | None:
    if status in DONE_STATUSES:
        return "done"
    if status in DISMISSED_STATUSES:
        return "dismissed"
    if status != "pending":
        return None
    if due_at is None:
        return None
    if due_at < now:
        return "overdue"
    if due_at.date() == now.date():
        return "due_today"
    return "upcoming"


def _source_context(row: dict[str, Any], source_reply_id: int | None) -> dict[str, Any] | None:
    if row.get("inbound_tweet_id") is None:
        return None
    return {
        "reply_queue_id": source_reply_id,
        "author_handle": _clean_handle(row.get("inbound_author_handle")),
        "author_id": row.get("inbound_author_id"),
        "inbound_id": row.get("inbound_tweet_id"),
        "inbound_url": row.get("inbound_url"),
        "status": _clean_status(row.get("source_status")),
    }


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(items),
        "by_status": {
            bucket: sum(1 for item in items if item["status_bucket"] == bucket)
            for bucket in BUCKETS
        },
    }


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    rank = {bucket: index for index, bucket in enumerate(BUCKETS)}
    return (
        rank.get(item["status_bucket"], 99),
        item["due_at"] or "",
        item["target_handle"] or "",
        item["id"] or 0,
    )


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    default: str = "NULL",
    output: str | None = None,
) -> str:
    output = output or column
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _row_to_dict(row: Any, names: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(names, row))


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_handle(value: Any) -> str | None:
    if value is None:
        return None
    handle = str(value).strip().lstrip("@").lower()
    return handle or None


def _clean_status(value: Any) -> str | None:
    if value is None:
        return None
    status = str(value).strip().lower()
    return status or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
