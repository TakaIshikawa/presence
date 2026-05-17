"""Find reply drafts stuck in review workflow states."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MAX_AGE_HOURS = 24
REVIEW_STATES = {"drafted", "reviewed", "revised", "approved", "pending_review"}


def build_reply_review_stuck_states_report(
    rows: list[dict[str, Any]],
    *,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if max_age_hours < 0:
        raise ValueError("max_age_hours must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    candidates = [_stuck_item(row, generated_at) for row in rows if _state(row) in REVIEW_STATES]
    stuck = [item for item in candidates if item["age_hours"] is not None and item["age_hours"] > max_age_hours]
    stuck.sort(key=lambda item: (-item["age_hours"], item["draft_id"]))
    return {
        "artifact_type": "reply_review_stuck_states",
        "generated_at": generated_at.isoformat(),
        "filters": {"max_age_hours": max_age_hours},
        "totals": {
            "rows_scanned": len(rows),
            "stuck_count": len(stuck),
            "oldest_stuck_age_hours": max((item["age_hours"] for item in stuck), default=None),
            "stuck_by_state": dict(Counter(item["current_state"] for item in stuck)),
        },
        "oldest_stuck_items": stuck[:25],
        "empty_state": {"is_empty": not stuck, "message": "No stuck reply review states found." if not stuck else None},
    }


def build_reply_review_stuck_states_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_reply_review_stuck_states_report(_load_rows(conn, schema), **kwargs)


def format_reply_review_stuck_states_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_stuck_states_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Review Stuck States",
        f"Generated: {report['generated_at']}",
        f"Max age hours: {report['filters']['max_age_hours']}",
        f"Totals: stuck={totals['stuck_count']} oldest_hours={totals['oldest_stuck_age_hours']}",
    ]
    if not report["oldest_stuck_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "Oldest stuck items:"])
    for item in report["oldest_stuck_items"]:
        lines.append(
            f"- {item['draft_id']} state={item['current_state']} age_hours={item['age_hours']} "
            f"last_transition={item['last_transition_at']} action={item['recommended_next_action']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "reply_drafts" if "reply_drafts" in schema else "reply_reviews" if "reply_reviews" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "id", "draft_id", "reply_id") + " AS draft_id",
        _col(columns, "state", "status", "review_status", default="'drafted'") + " AS current_state",
        _col(columns, "last_transition_at", "reviewed_at", "updated_at", "created_at", default="NULL") + " AS last_transition_at",
        _col(columns, "created_at", default="NULL") + " AS created_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _stuck_item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    transitioned_at = _parse_datetime(_first(row, "last_transition_at", "updated_at", "created_at"))
    age_hours = None if transitioned_at is None else int((now - transitioned_at).total_seconds() // 3600)
    state = _state(row)
    return {
        "draft_id": _text(_first(row, "draft_id", "id", "reply_id")) or "unknown",
        "current_state": state,
        "age_hours": age_hours,
        "last_transition_at": transitioned_at.isoformat() if transitioned_at else None,
        "recommended_next_action": _next_action(state),
    }


def _next_action(state: str) -> str:
    return {
        "drafted": "send to reviewer",
        "pending_review": "complete review",
        "reviewed": "apply review decision",
        "revised": "request final approval",
        "approved": "send approved reply",
    }.get(state, "inspect reply state")


def _state(row: dict[str, Any]) -> str:
    return _text(_first(row, "current_state", "state", "status", "review_status")).lower() or "drafted"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
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
