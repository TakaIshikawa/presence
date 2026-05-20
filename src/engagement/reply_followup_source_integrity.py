"""Audit source references and terminal state consistency for reply follow-ups."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
BLOCKED_SOURCE_STATUSES = {"dismissed", "cancelled", "canceled", "rejected", "expired"}
TERMINAL_STATUSES = {"completed", "done", "dismissed", "cancelled", "canceled"}


def build_reply_followup_source_integrity_report(
    rows: list[dict[str, Any]],
    *,
    source_type: str = "all",
    status: str = "all",
    due_before: str | datetime | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    source_filter = _clean(source_type).lower() or "all"
    status_filter = _clean(status).lower() or "all"
    due_before_dt = _parse_dt(due_before)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        row_source_type = _clean(row.get("source_type"), "unknown").lower()
        row_status = _clean(row.get("status"), "unknown").lower()
        due_at = _parse_dt(row.get("due_at"))
        if source_filter != "all" and row_source_type != source_filter:
            continue
        if status_filter != "all" and row_status != status_filter:
            continue
        if due_before_dt is not None and (due_at is None or due_at > due_before_dt):
            continue
        scanned += 1
        for issue in _issues(row):
            findings.append(
                {
                    "reminder_id": _int_or_none(row.get("id") or row.get("reminder_id")),
                    "source_type": row_source_type,
                    "source_id": _int_or_none(row.get("source_id")),
                    "source_reply_id": _int_or_none(row.get("source_reply_id")),
                    "source_action_id": _int_or_none(row.get("source_action_id")),
                    "status": row_status,
                    "source_status": _clean(row.get("source_status")).lower() or None,
                    "due_at": due_at.isoformat() if due_at else None,
                    "completed_at": _iso(row.get("completed_at")),
                    "dismissed_at": _iso(row.get("dismissed_at")),
                    "issue_type": issue,
                }
            )
    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "reply_followup_source_integrity",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "source_type": source_filter,
            "status": status_filter,
            "due_before": due_before_dt.isoformat() if due_before_dt else None,
            "limit": limit,
        },
        "findings": shown,
        "grouped_counts": _grouped_counts(findings),
        "summary": {
            "rows_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in findings).items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_reply_followup_source_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, schema, missing_tables, missing_columns)
    return build_reply_followup_source_integrity_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_reply_followup_source_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_source_integrity_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Reply Follow-up Source Integrity",
        f"Generated: {report['generated_at']}",
        f"Source type: {filters['source_type']}",
        f"Status: {filters['status']}",
        f"Due before: {filters['due_before'] or 'all'}",
        f"Totals: scanned={summary['rows_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No reply follow-up source integrity gaps found.")
        return "\n".join(lines)
    lines.extend(["", "source_type | status | issue_type | count"])
    for group in report["grouped_counts"]:
        lines.append(f"{group['source_type']} | {group['status']} | {group['issue_type']} | {group['count']}")
    lines.extend(["", "reminder_id | source_type | source_id | status | source_status | issue"])
    for item in report["findings"]:
        lines.append(
            f"{item['reminder_id'] or '-'} | {item['source_type']} | {item['source_id'] or '-'} | "
            f"{item['status']} | {item['source_status'] or '-'} | {item['issue_type']}"
        )
    return "\n".join(lines)


def _issues(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    source_type = _clean(row.get("source_type")).lower()
    source_id = _int_or_none(row.get("source_id"))
    source_reply_id = _int_or_none(row.get("source_reply_id"))
    source_action_id = _int_or_none(row.get("source_action_id"))
    source_exists = True if "source_exists" not in row else _truthy(row.get("source_exists"))
    status = _clean(row.get("status")).lower()
    source_status = _clean(row.get("source_status")).lower()
    completed_at = _parse_dt(row.get("completed_at"))
    dismissed_at = _parse_dt(row.get("dismissed_at"))
    if source_type == "reply_queue":
        if source_reply_id is not None and source_id != source_reply_id:
            issues.append("source_field_mismatch")
        if not source_exists:
            issues.append("missing_source")
    elif source_type == "proactive_action":
        if source_action_id is not None and source_id != source_action_id:
            issues.append("source_field_mismatch")
        if not source_exists:
            issues.append("missing_source")
    elif source_type:
        issues.append("unsupported_source_type")
    if status == "pending" and source_status in BLOCKED_SOURCE_STATUSES:
        issues.append("stale_pending_source")
    if status in {"completed", "done"} and completed_at is None:
        issues.append("terminal_timestamp_conflict")
    if status not in {"completed", "done"} and completed_at is not None:
        issues.append("terminal_timestamp_conflict")
    if status in {"dismissed", "cancelled", "canceled"} and dismissed_at is None:
        issues.append("terminal_timestamp_conflict")
    if status not in {"dismissed", "cancelled", "canceled"} and dismissed_at is not None:
        issues.append("terminal_timestamp_conflict")
    if status == "pending" and (completed_at is not None or dismissed_at is not None):
        issues.append("terminal_timestamp_conflict")
    return sorted(set(issues))


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "reply_followup_reminders" not in schema:
        missing_tables.append("reply_followup_reminders")
        return []
    cols = schema["reply_followup_reminders"]
    if "id" not in cols:
        missing_columns["reply_followup_reminders"] = ["id"]
        return []
    rq = schema.get("reply_queue", set())
    pa = schema.get("proactive_actions", set())
    rq_join = "LEFT JOIN reply_queue rq ON rq.id = r.source_id AND lower(r.source_type) = 'reply_queue'" if rq and "id" in rq else ""
    pa_join = "LEFT JOIN proactive_actions pa ON pa.id = r.source_id AND lower(r.source_type) = 'proactive_action'" if pa and "id" in pa else ""
    rq_exists = "rq.id IS NOT NULL" if rq_join else "0"
    pa_exists = "pa.id IS NOT NULL" if pa_join else "0"
    rq_status = _qcol(rq, "status", "rq", "NULL")
    pa_status = _qcol(pa, "status", "pa", "NULL")
    source_type_expr = _qcol(cols, "source_type", "r", "''")
    select = [
        "r.id AS id",
        _qcol(cols, "source_type", "r", "'unknown'") + " AS source_type",
        _qcol(cols, "source_id", "r", "NULL") + " AS source_id",
        _qcol(cols, "source_reply_id", "r", "NULL") + " AS source_reply_id",
        _qcol(cols, "source_action_id", "r", "NULL") + " AS source_action_id",
        _qcol(cols, "status", "r", "'unknown'") + " AS status",
        _qcol(cols, "due_at", "r", "NULL") + " AS due_at",
        _qcol(cols, "completed_at", "r", "NULL") + " AS completed_at",
        _qcol(cols, "dismissed_at", "r", "NULL") + " AS dismissed_at",
        f"CASE WHEN lower({source_type_expr}) = 'reply_queue' THEN {rq_exists} WHEN lower({source_type_expr}) = 'proactive_action' THEN {pa_exists} ELSE 0 END AS source_exists",
        f"CASE WHEN lower({source_type_expr}) = 'reply_queue' THEN {rq_status} WHEN lower({source_type_expr}) = 'proactive_action' THEN {pa_status} ELSE NULL END AS source_status",
    ]
    query = f"SELECT {', '.join(select)} FROM reply_followup_reminders r {rq_join} {pa_join} ORDER BY r.rowid ASC"
    return [dict(row) for row in conn.execute(query)]


def _grouped_counts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["source_type"], item["status"], item["issue_type"]) for item in items)
    return [
        {"source_type": source_type, "status": status, "issue_type": issue, "count": count}
        for (source_type, status, issue), count in sorted(counts.items())
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["source_type"], item["status"], item["issue_type"], item["reminder_id"] or 0)


def _qcol(columns: set[str], column: str, alias: str, default: str) -> str:
    return f"{alias}.{column}" if column in columns else default


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    try:
        return _utc(datetime.fromisoformat(str(value).strip().replace("Z", "+00:00")))
    except ValueError:
        return None


def _iso(value: Any) -> str | None:
    parsed = _parse_dt(value)
    return parsed.isoformat() if parsed else None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _truthy(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
