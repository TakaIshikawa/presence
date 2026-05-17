"""Report pressure from pending reply follow-up reminders."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
DEFAULT_OVERDUE_GRACE_HOURS = 0.0
DEFAULT_LIMIT = 20


def build_reply_followup_due_pressure_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    overdue_grace_hours: float = DEFAULT_OVERDUE_GRACE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if overdue_grace_hours < 0:
        raise ValueError("overdue_grace_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    due_until = generated_at + timedelta(days=days_ahead)
    overdue_cutoff = generated_at - timedelta(hours=overdue_grace_hours)
    filters = {"days_ahead": days_ahead, "overdue_grace_hours": overdue_grace_hours, "limit": limit, "due_until": due_until.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns)
    rows = conn.execute(
        """SELECT id, target_handle, source_type, source_id, due_at, status
           FROM reply_followup_reminders
           ORDER BY datetime(due_at) ASC, id ASC"""
    ).fetchall()
    buckets = Counter({"overdue": 0, "due_soon": 0, "later": 0, "done": 0, "dismissed": 0})
    overdue_examples = []
    target_counts: Counter[str] = Counter()
    for row in rows:
        status = str(row["status"] or "pending").lower()
        due_at = _parse(row["due_at"])
        if status in {"done", "completed"}:
            bucket = "done"
        elif status in {"dismissed", "cancelled", "canceled"}:
            bucket = "dismissed"
        elif due_at is not None and due_at < overdue_cutoff:
            bucket = "overdue"
        elif due_at is not None and due_at <= due_until:
            bucket = "due_soon"
        else:
            bucket = "later"
        buckets[bucket] += 1
        if bucket == "overdue":
            handle = row["target_handle"] or "unknown"
            target_counts[handle] += 1
            overdue_examples.append(
                {
                    "reminder_id": int(row["id"]),
                    "target_handle": handle,
                    "source_type": row["source_type"],
                    "source_id": row["source_id"],
                    "due_at": due_at.isoformat() if due_at else None,
                    "overdue_hours": round((generated_at - due_at).total_seconds() / 3600, 2) if due_at else None,
                    "status": status,
                }
            )
    overdue_examples.sort(key=lambda item: (-(item["overdue_hours"] or 0), item["reminder_id"]))
    return {
        "artifact_type": "reply_followup_due_pressure",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"reminder_count": len(rows), **dict(buckets)},
        "due_buckets": dict(buckets),
        "overdue_examples": overdue_examples[:limit],
        "target_handle_breakdowns": dict(sorted(target_counts.items(), key=lambda item: (-item[1], item[0]))),
        "missing_tables": [],
        "missing_columns": {},
    }


def format_reply_followup_due_pressure_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_due_pressure_text(report: dict[str, Any]) -> str:
    b = report["due_buckets"]
    lines = [
        "Reply Followup Due Pressure",
        f"Generated: {report['generated_at']}",
        f"Filters: days_ahead={report['filters']['days_ahead']} overdue_grace_hours={report['filters']['overdue_grace_hours']} limit={report['filters']['limit']}",
        f"Buckets: overdue={b['overdue']} due_soon={b['due_soon']} later={b['later']} done={b['done']} dismissed={b['dismissed']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report["overdue_examples"]:
        lines.extend(["", "Overdue examples:"])
        for item in report["overdue_examples"]:
            lines.append(f"- reminder={item['reminder_id']} target={item['target_handle']} due={item['due_at']} overdue_h={item['overdue_hours']}")
    else:
        lines.append("No overdue follow-up reminders found.")
    return "\n".join(lines)


format_reply_followup_due_pressure_table = format_reply_followup_due_pressure_text


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {"reply_followup_reminders": {"id", "target_handle", "source_type", "source_id", "due_at", "status"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {table: sorted(cols - schema[table]) for table, cols in required.items() if table in schema and cols - schema[table]}
    return missing_tables, missing_columns


def _empty(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "reply_followup_due_pressure",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"reminder_count": 0, "overdue": 0, "due_soon": 0, "later": 0, "done": 0, "dismissed": 0},
        "due_buckets": {"overdue": 0, "due_soon": 0, "later": 0, "done": 0, "dismissed": 0},
        "overdue_examples": [],
        "target_handle_breakdowns": {},
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
