"""Audit reply queue status gaps against durable review events."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_STALE_PENDING_HOURS = 48


def build_reply_review_audit_gaps_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stale_pending_hours: int = DEFAULT_STALE_PENDING_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare reply_queue statuses with latest reply_review_events."""
    if days <= 0 or stale_pending_hours <= 0 or limit <= 0:
        raise ValueError("days, stale_pending_hours, and limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    stale_cutoff = generated_at - timedelta(hours=stale_pending_hours)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "stale_pending_hours": stale_pending_hours,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    missing_tables = [table for table in ("reply_queue", "reply_review_events") if table not in schema]
    missing_columns = _missing_columns(schema)
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = [_normalize_reply(dict(row)) for row in _load_replies(conn, schema["reply_queue"], cutoff, generated_at)]
    events = _load_latest_events(conn, schema.get("reply_review_events", set()), [row["reply_id"] for row in rows])
    findings: list[dict[str, Any]] = []
    for row in rows:
        latest = events.get(row["reply_id"])
        status = row["status"]
        if status in {"approved", "posted"} and latest is None:
            findings.append(_finding("approved_without_event" if status == "approved" else "posted_without_event", row, latest))
        if status == "pending" and latest is None and _parse_timestamp(row["detected_at"]) and _parse_timestamp(row["detected_at"]) < stale_cutoff:
            findings.append(_finding("stale_pending_without_event", row, latest))
        if latest and latest.get("new_status") and latest["new_status"] != status:
            findings.append(_finding("status_event_mismatch", row, latest))

    findings.sort(key=lambda item: (item["finding_type"], item["reply_id"]))
    status_counts = dict(sorted(Counter(row["status"] for row in rows).items()))
    event_counts = _event_counts(conn, schema.get("reply_review_events", set()), cutoff, generated_at)
    return {
        "artifact_type": "reply_review_audit_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "reply_count": len(rows),
            "finding_count": len(findings),
            "representative_reply_count": min(len({f["reply_id"] for f in findings}), limit),
        },
        "findings": findings[:limit],
        "status_counts": status_counts,
        "event_counts": event_counts,
        "representative_reply_ids": sorted({finding["reply_id"] for finding in findings})[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_reply_review_audit_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_audit_gaps_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Review Audit Gaps",
        f"Generated: {report['generated_at']}",
        f"Totals: replies={totals['reply_count']} findings={totals['finding_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    return "\n".join(lines)


def _load_replies(conn: sqlite3.Connection, columns: set[str], cutoff: datetime, now: datetime) -> list[sqlite3.Row]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "reviewed_at"),
        _column_expr(columns, "posted_at"),
    ]
    return conn.execute(
        f"""SELECT {', '.join(select_columns)} FROM reply_queue
            WHERE datetime(COALESCE(posted_at, reviewed_at, detected_at)) >= datetime(?)
              AND datetime(COALESCE(posted_at, reviewed_at, detected_at)) <= datetime(?)
            ORDER BY id ASC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()


def _load_latest_events(conn: sqlite3.Connection, columns: set[str], reply_ids: list[int]) -> dict[int, dict[str, Any]]:
    required = {"reply_queue_id"}
    if not reply_ids or not required.issubset(columns):
        return {}
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "reply_queue_id"),
        _column_expr(columns, "event_type"),
        _column_expr(columns, "new_status"),
        _column_expr(columns, "created_at"),
    ]
    placeholders = ",".join("?" for _ in reply_ids)
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)} FROM reply_review_events
            WHERE reply_queue_id IN ({placeholders})
            ORDER BY reply_queue_id ASC, datetime(created_at) ASC, id ASC""",
        reply_ids,
    ).fetchall()
    latest: dict[int, dict[str, Any]] = {}
    for raw in rows:
        event = _normalize_event(dict(raw))
        latest[event["reply_id"]] = event
    return latest


def _event_counts(conn: sqlite3.Connection, columns: set[str], cutoff: datetime, now: datetime) -> dict[str, int]:
    if "reply_review_events" not in _schema(conn) or "event_type" not in columns:
        return {}
    created_filter = ""
    params: tuple[Any, ...] = ()
    if "created_at" in columns:
        created_filter = "WHERE datetime(created_at) >= datetime(?) AND datetime(created_at) <= datetime(?)"
        params = (cutoff.isoformat(), now.isoformat())
    rows = conn.execute(
        f"SELECT event_type, COUNT(*) AS count FROM reply_review_events {created_filter} GROUP BY event_type",
        params,
    ).fetchall()
    return dict(sorted((str(row["event_type"]), int(row["count"])) for row in rows))


def _normalize_reply(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reply_id": int(row["id"]),
        "status": (row.get("status") or "pending").lower(),
        "detected_at": row.get("detected_at"),
        "reviewed_at": row.get("reviewed_at"),
        "posted_at": row.get("posted_at"),
    }


def _normalize_event(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": row.get("id"),
        "reply_id": int(row.get("reply_queue_id") or 0),
        "event_type": (row.get("event_type") or "").lower() or None,
        "new_status": (row.get("new_status") or "").lower() or None,
        "created_at": row.get("created_at"),
    }


def _finding(kind: str, row: dict[str, Any], latest: dict[str, Any] | None) -> dict[str, Any]:
    return {
        "finding_type": kind,
        "reply_id": row["reply_id"],
        "status": row["status"],
        "detected_at": row["detected_at"],
        "reviewed_at": row["reviewed_at"],
        "posted_at": row["posted_at"],
        "latest_event": latest,
    }


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "reply_review_audit_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"reply_count": 0, "finding_count": 0, "representative_reply_count": 0},
        "findings": [],
        "status_counts": {},
        "event_counts": {},
        "representative_reply_ids": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    expected = {
        "reply_queue": {"id", "status", "detected_at", "reviewed_at", "posted_at"},
        "reply_review_events": {"id", "reply_queue_id", "event_type", "new_status", "created_at"},
    }
    return {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema and columns - schema.get(table, set())
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _parse_timestamp(raw: Any) -> datetime | None:
    if not raw:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(raw).replace("Z", "+00:00")))
    except ValueError:
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
