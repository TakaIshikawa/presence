"""Report generated content that has been pending review for too long."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MAX_AGE_HOURS = 48
PENDING_STATUSES = {"pending", "pending_review", "review", "in_review", "queued", "drafted"}


def build_generation_review_queue_aging_report(
    rows: list[dict[str, Any]],
    *,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if max_age_hours < 0:
        raise ValueError("max_age_hours must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    pending = [_normalize_row(row, generated_at) for row in rows if _is_pending(row)]
    aged = [row for row in pending if row["age_hours"] is not None and row["age_hours"] > max_age_hours]
    aged.sort(key=lambda row: (-1 if row["age_hours"] is None else -row["age_hours"], row["content_id"]))
    oldest = max((row["age_hours"] for row in pending if row["age_hours"] is not None), default=None)
    return {
        "artifact_type": "generation_review_queue_aging",
        "generated_at": generated_at.isoformat(),
        "filters": {"max_age_hours": max_age_hours},
        "totals": {
            "rows_scanned": len(rows),
            "total_pending_items": len(pending),
            "aged_pending_count": len(aged),
            "oldest_pending_age_hours": oldest,
        },
        "aged_items": aged,
        "empty_state": {
            "is_empty": not pending,
            "message": "No pending generation review items found." if not pending else None,
        },
    }


def build_generation_review_queue_aging_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_generation_review_queue_aging_report(_load_rows(conn, schema), **kwargs)


def format_generation_review_queue_aging_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_generation_review_queue_aging_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Generation Review Queue Aging",
        f"Generated: {report['generated_at']}",
        f"Max age hours: {report['filters']['max_age_hours']}",
        (
            "Totals: "
            f"pending={totals['total_pending_items']} "
            f"aged={totals['aged_pending_count']} "
            f"oldest_hours={totals['oldest_pending_age_hours']}"
        ),
    ]
    if not report["aged_items"]:
        lines.append(report["empty_state"]["message"] or "No pending review items exceed the threshold.")
        return "\n".join(lines)
    lines.extend(["", "Aged items:"])
    for item in report["aged_items"]:
        lines.append(
            f"- {item['content_id']} status={item['status']} age_hours={item['age_hours']} "
            f"type={item['content_type']} reason={item['reason']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = [
        "id AS content_id" if "id" in columns else "NULL AS content_id",
        "content_type" if "content_type" in columns else "NULL AS content_type",
        "content_format" if "content_format" in columns else "NULL AS content_format",
        _column_expr(columns, ("review_status", "status", "state"), "pending") + " AS status",
        _column_expr(columns, ("review_requested_at", "queued_at", "created_at", "generated_at"), "NULL") + " AS pending_since",
        "eval_score" if "eval_score" in columns else "NULL AS eval_score",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]


def _normalize_row(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    pending_since = _parse_datetime(_first(row, "pending_since", "review_requested_at", "queued_at", "created_at", "generated_at"))
    age_hours = None if pending_since is None else int((now - pending_since).total_seconds() // 3600)
    status = _text(_first(row, "status", "review_status", "state")) or "pending"
    content_type = _text(_first(row, "content_type", "content_format")) or "unknown"
    return {
        "content_id": _text(_first(row, "content_id", "id")) or "unknown",
        "content_type": content_type,
        "status": status,
        "pending_since": pending_since.isoformat() if pending_since else None,
        "age_hours": age_hours,
        "reason": _reason(age_hours, content_type),
    }


def _reason(age_hours: int | None, content_type: str) -> str:
    if age_hours is None:
        return f"{content_type} has no review queue timestamp."
    return f"{content_type} has been pending review for {age_hours} hours."


def _is_pending(row: dict[str, Any]) -> bool:
    status = (_text(_first(row, "status", "review_status", "state")) or "pending").lower()
    return status in PENDING_STATUSES or "pending" in status


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(columns: set[str], names: tuple[str, ...], default: str) -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
