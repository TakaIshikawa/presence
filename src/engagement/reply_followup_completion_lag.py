"""Report reply follow-up reminder completion lag by source and target."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_LAG_HOURS = 0.0
LATE_BUCKETS = ("overdue_pending", "completed_late", "dismissed_late")


def build_reply_followup_completion_lag_report(
    rows: list[dict[str, Any]],
    *,
    min_lag_hours: float = DEFAULT_MIN_LAG_HOURS,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if min_lag_hours < 0:
        raise ValueError("min_lag_hours must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    summaries: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    examples: list[dict[str, Any]] = []
    total = 0

    for row in rows:
        total += 1
        source_type = _norm(row.get("source_type"), "unknown")
        target_handle = _norm(row.get("target_handle"), "unknown")
        status = _norm(row.get("status"), "pending").lower()
        due_at = _parse_dt(row.get("due_at"))
        completed_at = _parse_dt(row.get("completed_at"))
        dismissed_at = _parse_dt(row.get("dismissed_at"))
        bucket = _bucket(status, due_at, completed_at, dismissed_at, generated_at)
        lag_hours = _lag_hours(bucket, due_at, completed_at, dismissed_at, generated_at)
        key = (source_type, target_handle)
        summaries[key]["total_count"] += 1
        summaries[key][bucket] += 1
        if bucket in LATE_BUCKETS and lag_hours is not None and lag_hours >= min_lag_hours:
            examples.append(
                {
                    "reminder_id": row.get("reminder_id"),
                    "source_type": source_type,
                    "target_handle": target_handle,
                    "status": status,
                    "bucket": bucket,
                    "due_at": due_at.isoformat() if due_at else None,
                    "completed_at": completed_at.isoformat() if completed_at else None,
                    "dismissed_at": dismissed_at.isoformat() if dismissed_at else None,
                    "lag_hours": lag_hours,
                }
            )

    grouped = []
    for (source_type, target_handle), counts in summaries.items():
        late_count = sum(counts[bucket] for bucket in LATE_BUCKETS)
        grouped.append(
            {
                "source_type": source_type,
                "target_handle": target_handle,
                "total_count": counts["total_count"],
                "pending_count": counts["pending"],
                "done_count": counts["done"],
                "dismissed_count": counts["dismissed"],
                "on_time_count": counts["on_time"],
                "overdue_pending_count": counts["overdue_pending"],
                "completed_late_count": counts["completed_late"],
                "dismissed_late_count": counts["dismissed_late"],
                "late_count": late_count,
            }
        )
    grouped.sort(key=lambda item: (-item["late_count"], item["source_type"], item["target_handle"]))
    examples.sort(key=lambda item: (-item["lag_hours"], item["source_type"], item["target_handle"], item["reminder_id"] or 0))
    issue_counts = Counter(item["bucket"] for item in examples)
    return {
        "artifact_type": "reply_followup_completion_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"now": generated_at.isoformat(), "min_lag_hours": min_lag_hours},
        "summary": {
            "reminder_count": total,
            "group_count": len(grouped),
            "late_example_count": len(examples),
            "overdue_pending_count": issue_counts["overdue_pending"],
            "completed_late_count": issue_counts["completed_late"],
            "dismissed_late_count": issue_counts["dismissed_late"],
        },
        "grouped_summaries": grouped,
        "late_reminders": examples,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
    }


def build_reply_followup_completion_lag_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"reply_followup_reminders": {"id", "due_at"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    rows = [] if missing_tables or missing_columns else _load_rows(conn, schema["reply_followup_reminders"])
    return build_reply_followup_completion_lag_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_reply_followup_completion_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_completion_lag_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Reply Follow-up Completion Lag",
        f"Generated: {report['generated_at']}",
        f"Min lag hours: {report['filters']['min_lag_hours']:g}",
        (
            f"Totals: reminders={summary['reminder_count']} groups={summary['group_count']} "
            f"overdue_pending={summary['overdue_pending_count']} completed_late={summary['completed_late_count']} "
            f"dismissed_late={summary['dismissed_late_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{table}.{col}" for table, cols in report["missing_columns"].items() for col in cols))
    if report["grouped_summaries"]:
        lines.extend(["", "source_type | target_handle | total | overdue_pending | completed_late | dismissed_late | on_time"])
        for row in report["grouped_summaries"]:
            lines.append(
                f"{row['source_type']} | {row['target_handle']} | {row['total_count']} | "
                f"{row['overdue_pending_count']} | {row['completed_late_count']} | {row['dismissed_late_count']} | {row['on_time_count']}"
            )
    if report["late_reminders"]:
        lines.extend(["", "Late reminders:"])
        for item in report["late_reminders"]:
            lines.append(f"- {item['bucket']} reminder_id={item['reminder_id']} target={item['target_handle']} lag_hours={item['lag_hours']}")
    if not report["late_reminders"] and not report["missing_tables"] and not report["missing_columns"]:
        lines.append("No reply follow-up completion lag found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "reminder_id", "rowid"),
        _expr(columns, "source_type", "source_type", "'unknown'"),
        _expr(columns, "target_handle", "target_handle", _expr(columns, "handle", "target_handle", "'unknown'").split(" AS ")[0]),
        _expr(columns, "status", "status", "'pending'"),
        "due_at",
        _expr(columns, "completed_at", "completed_at", "NULL"),
        _expr(columns, "dismissed_at", "dismissed_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM reply_followup_reminders ORDER BY due_at, rowid")]


def _bucket(status: str, due_at: datetime | None, completed_at: datetime | None, dismissed_at: datetime | None, now: datetime) -> str:
    if status in {"done", "completed", "complete"}:
        return "completed_late" if due_at and completed_at and completed_at > due_at else "on_time"
    if status == "dismissed":
        return "dismissed_late" if due_at and dismissed_at and dismissed_at > due_at else "on_time"
    if due_at and now > due_at:
        return "overdue_pending"
    return "pending"


def _lag_hours(bucket: str, due_at: datetime | None, completed_at: datetime | None, dismissed_at: datetime | None, now: datetime) -> float | None:
    if not due_at:
        return None
    end = now if bucket == "overdue_pending" else completed_at if bucket == "completed_late" else dismissed_at if bucket == "dismissed_late" else None
    if not end:
        return None
    return round((end - due_at).total_seconds() / 3600, 2)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _norm(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or default
