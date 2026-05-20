"""Audit proactive action relationship context gaps."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
PRE_APPROVAL_STATUSES = {"draft", "pending", "proposed", "queued", "needs_approval", "awaiting_approval"}
RELATIONSHIP_SIGNAL_KEYS = ("stage", "tier", "strength", "last_interaction_at", "notes")
REQUIRED_COLUMNS = {"id", "status", "relationship_context"}
OPTIONAL_COLUMNS = ("action_type", "target_author_handle", "created_at")


def build_proactive_action_relationship_context_gaps_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a proactive action relationship context gaps report."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get("proactive_actions")
    if columns is None:
        return _report(generated_at, limit, [], 0, ["proactive_actions"], {})
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return _report(generated_at, limit, [], 0, [], {"proactive_actions": missing})

    rows = _load_rows(conn, columns)
    items = [item for row in rows if (item := _row_gap(row)) is not None]
    items.sort(key=lambda item: (_gap_rank(item["gap_type"]), _clean(item["status"]), _int_or_text(item["action_id"])))
    return _report(generated_at, limit, items, len(rows), [], {})


def format_proactive_action_relationship_context_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_relationship_context_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Proactive Action Relationship Context Gaps",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: scanned={summary['actions_scanned']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "action_id | status | action_type | target_author_handle | created_at | gap_type"])
    for item in report["items"]:
        lines.append(
            f"{item['action_id']} | {item['status']} | {item['action_type'] or '-'} | "
            f"{item['target_author_handle'] or '-'} | {item['created_at'] or '-'} | {item['gap_type']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select_parts = ["id AS action_id", "status", "relationship_context"]
    for column in OPTIONAL_COLUMNS:
        select_parts.append(f"{column}" if column in columns else f"NULL AS {column}")
    rows = conn.execute(
        f"""SELECT {', '.join(select_parts)}
            FROM proactive_actions
            WHERE lower(COALESCE(status, '')) IN ({', '.join('?' for _ in PRE_APPROVAL_STATUSES)})
            ORDER BY id ASC""",
        tuple(sorted(PRE_APPROVAL_STATUSES)),
    ).fetchall()
    return [dict(row) for row in rows]


def _row_gap(row: dict[str, Any]) -> dict[str, Any] | None:
    raw = row.get("relationship_context")
    if raw is None or _clean(raw) == "":
        return _item(row, "missing_context")
    payload = _parse_context(raw)
    if payload is None:
        return _item(row, "malformed_context")
    if not payload:
        return _item(row, "empty_context")
    if not any(_has_signal(payload, key) for key in RELATIONSHIP_SIGNAL_KEYS):
        return _item(row, "low_signal_context")
    return None


def _parse_context(raw: Any) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return raw
    try:
        parsed = json.loads(str(raw))
    except (TypeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _has_signal(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, list):
        return any(_clean(item) for item in value)
    if isinstance(value, dict):
        return any(_clean(item) for item in value.values())
    return _clean(value) != ""


def _item(row: dict[str, Any], gap_type: str) -> dict[str, Any]:
    return {
        "action_id": row.get("action_id"),
        "status": _clean(row.get("status")).lower(),
        "action_type": row.get("action_type"),
        "target_author_handle": row.get("target_author_handle"),
        "created_at": row.get("created_at"),
        "gap_type": gap_type,
    }


def _report(
    generated_at: datetime,
    limit: int,
    items: list[dict[str, Any]],
    actions_scanned: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = items[:limit]
    return {
        "artifact_type": "proactive_action_relationship_context_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "statuses": sorted(PRE_APPROVAL_STATUSES)},
        "summary": {
            "actions_scanned": actions_scanned,
            "gap_count": len(items),
            "shown_count": len(shown),
            "by_gap_type": _counts(items, "gap_type"),
        },
        "items": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items())},
        "empty_state": {
            "is_empty": not items,
            "message": "No proactive action relationship context gaps found." if not items else None,
        },
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = _clean(item.get(key))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _gap_rank(gap_type: str) -> int:
    return {
        "missing_context": 0,
        "malformed_context": 1,
        "empty_context": 2,
        "low_signal_context": 3,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
