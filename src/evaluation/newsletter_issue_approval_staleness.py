"""Report newsletter issues stuck in approval or review states."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WARNING_HOURS = 24.0
DEFAULT_CRITICAL_HOURS = 72.0
DEFAULT_LIMIT = 100
REVIEW_STATUSES = {"review", "in_review", "pending_review", "approval", "pending_approval", "awaiting_approval"}
TIME_COLUMNS = ("review_started_at", "submitted_for_review_at", "updated_at", "created_at")


def build_newsletter_issue_approval_staleness_report(
    rows: list[dict[str, Any]],
    *,
    warning_hours: float = DEFAULT_WARNING_HOURS,
    critical_hours: float = DEFAULT_CRITICAL_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if warning_hours <= 0 or critical_hours <= 0 or limit <= 0:
        raise ValueError("warning_hours, critical_hours, and limit must be positive")
    if critical_hours < warning_hours:
        raise ValueError("critical_hours must be greater than or equal to warning_hours")
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = []
    for row in rows:
        status = _norm(row.get("status")) or "unknown"
        started = _parse_timestamp(row.get("started_at"))
        if status in REVIEW_STATUSES and started:
            age = (generated_at - started).total_seconds() / 3600
            severity = "critical" if age >= critical_hours else "warning" if age >= warning_hours else None
            if severity:
                findings.append(_finding(row, started, age, severity))
    findings.sort(key=lambda f: ({"critical": 0, "warning": 1}[f["severity"]], -f["age_hours"], str(f["issue_id"])))
    shown = findings[:limit]
    return {
        "artifact_type": "newsletter_issue_approval_staleness",
        "generated_at": generated_at.isoformat(),
        "filters": {"warning_hours": warning_hours, "critical_hours": critical_hours, "limit": limit},
        "summary": {
            "issue_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_status_severity": [
                {"status": status, "severity": severity, "count": count}
                for (status, severity), count in sorted(Counter((f["status"], f["severity"]) for f in findings).items())
            ],
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {t: sorted(c) for t, c in sorted((missing_columns or {}).items()) if c},
        "empty_state": {"is_empty": not findings, "message": "No stale newsletter approvals found." if not findings else None},
    }


def build_newsletter_issue_approval_staleness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in ("newsletter_issues", "newsletter_drafts", "newsletter_issue_queue") if name in schema), None)
    if table is None:
        return build_newsletter_issue_approval_staleness_report([], missing_tables=["newsletter_issues|newsletter_drafts|newsletter_issue_queue"], **kwargs)
    columns = schema[table]
    missing = []
    if "id" not in columns and "issue_id" not in columns:
        missing.append("id|issue_id")
    if "status" not in columns:
        missing.append("status")
    if not set(TIME_COLUMNS) & columns:
        missing.append("|".join(TIME_COLUMNS))
    if missing:
        return build_newsletter_issue_approval_staleness_report([], missing_columns={table: missing}, **kwargs)
    return build_newsletter_issue_approval_staleness_report(_load_rows(conn, table, columns), **kwargs)


def format_newsletter_issue_approval_staleness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_issue_approval_staleness_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Newsletter Issue Approval Staleness",
        f"Generated: {report['generated_at']}",
        f"Filters: warning_hours={report['filters']['warning_hours']} critical_hours={report['filters']['critical_hours']} limit={report['filters']['limit']}",
        f"Totals: issues={s['issue_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(f"  - issue={f['issue_id']} status={f['status']} severity={f['severity']} age_hours={f['age_hours']} title={f['subject_or_title'] or '-'}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    aliases = {
        "issue_id": ("issue_id", "id"),
        "subject_or_title": ("subject", "title"),
        "status": ("status",),
        "started_at": TIME_COLUMNS,
        "scheduled_for": ("scheduled_for", "scheduled_at", "send_at"),
        "reviewer": ("reviewer", "reviewer_id", "reviewer_handle"),
        "approver": ("approver", "approver_id", "approved_by"),
    }
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases.items())
    rows = conn.execute(f"SELECT {select} FROM {table} ORDER BY datetime({_coalesce(columns, TIME_COLUMNS)}) ASC").fetchall()
    return [dict(row) for row in rows]


def _finding(row: dict[str, Any], started: datetime, age_hours: float, severity: str) -> dict[str, Any]:
    return {
        "issue_id": row.get("issue_id"),
        "subject_or_title": row.get("subject_or_title"),
        "status": _norm(row.get("status")) or "unknown",
        "age_hours": round(age_hours, 2),
        "severity": severity,
        "scheduled_for": row.get("scheduled_for"),
        "reviewer": row.get("reviewer"),
        "approver": row.get("approver"),
        "approval_started_at": started.isoformat(),
    }


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _norm(value: Any) -> str | None:
    text = "" if value is None else str(value).strip().lower()
    return text or None


def _parse_timestamp(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
