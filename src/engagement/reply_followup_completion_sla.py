"""Report reply follow-up reminder completion timeliness."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_GRACE_HOURS = 0.0
ISSUE_TYPES = ("overdue_pending", "completed_after_due", "dismissed_after_due")


def build_reply_followup_completion_sla_report(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    grace_hours: float = DEFAULT_GRACE_HOURS,
    target_handle: str | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if grace_hours < 0:
        raise ValueError("grace_hours must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    target_filter = _clean(target_handle).lower() or None
    grace = timedelta(hours=grace_hours)
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    backlog: Counter[str] = Counter()

    for row in rows:
        target = _clean(row.get("target_handle")) or "unknown"
        if target_filter and target.lower() != target_filter:
            continue
        status = _clean(row.get("status")).lower() or "pending"
        due_at = _parse_time(row.get("due_at"))
        if status == "pending":
            backlog[target] += 1
        if due_at is None:
            continue
        base = {
            "reminder_id": row.get("reminder_id"),
            "target_handle": target,
            "status": status,
            "due_at": row.get("due_at"),
            "completed_at": row.get("completed_at"),
            "dismissed_at": row.get("dismissed_at"),
        }
        deadline = due_at + grace
        if status == "pending" and generated_at > deadline:
            _record(findings, counts, {**base, "issue_type": "overdue_pending", "lateness_hours": _hours(generated_at - due_at), "detail": "pending reminder is past due"})
        elif status == "completed":
            completed_at = _parse_time(row.get("completed_at"))
            if completed_at is not None and completed_at > deadline:
                _record(findings, counts, {**base, "issue_type": "completed_after_due", "lateness_hours": _hours(completed_at - due_at), "detail": "reminder was completed after due_at"})
        elif status == "dismissed":
            dismissed_at = _parse_time(row.get("dismissed_at"))
            if dismissed_at is not None and dismissed_at > deadline:
                _record(findings, counts, {**base, "issue_type": "dismissed_after_due", "lateness_hours": _hours(dismissed_at - due_at), "detail": "reminder was dismissed after due_at"})

    findings.sort(key=_finding_sort_key)
    return {
        "artifact_type": "reply_followup_completion_sla",
        "generated_at": generated_at.isoformat(),
        "filters": {"now": generated_at.isoformat(), "grace_hours": grace_hours, "target_handle": target_filter or "all"},
        "overdue_count": counts["overdue_pending"],
        "late_completed_count": counts["completed_after_due"],
        "late_dismissed_count": counts["dismissed_after_due"],
        "target_backlog": dict(sorted(backlog.items())),
        "totals": {
            "reminder_count": sum(1 for row in rows if not target_filter or (_clean(row.get("target_handle")).lower() == target_filter)),
            "finding_count": len(findings),
            "issue_counts": {issue: counts[issue] for issue in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": findings,
    }


def build_reply_followup_completion_sla_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"reply_followup_reminders": {"id", "due_at"}}
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_reply_followup_completion_sla_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    return build_reply_followup_completion_sla_report(
        _load_rows(conn, schema["reply_followup_reminders"]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_reply_followup_completion_sla_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_completion_sla_text(report: dict[str, Any]) -> str:
    lines = [
        "Reply Follow-up Completion SLA",
        f"Generated: {report['generated_at']}",
        f"Grace hours: {report['filters']['grace_hours']:g}",
        f"Target handle: {report['filters']['target_handle']}",
        (
            "Totals: "
            f"overdue={report['overdue_count']} "
            f"late_completed={report['late_completed_count']} "
            f"late_dismissed={report['late_dismissed_count']} "
            f"findings={report['totals']['finding_count']}"
        ),
        "Target backlog: " + (", ".join(f"{target}={count}" for target, count in report["target_backlog"].items()) or "none"),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(f"{table}.{column}" for table, columns in report["missing_columns"].items() for column in columns)
        )
    if not report["findings"]:
        lines.append("No reply follow-up completion SLA breaches found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(
            f"- {finding['issue_type']} reminder_id={finding.get('reminder_id')} "
            f"target={finding.get('target_handle')} lateness_hours={finding.get('lateness_hours')} "
            f"detail={finding['detail']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    status = _col(columns, "status", "state", fallback="'pending'")
    target = _col(columns, "target_handle", "handle", "author_handle", fallback="'unknown'")
    completed = _col(columns, "completed_at", "completed_time", fallback="NULL")
    dismissed = _col(columns, "dismissed_at", "dismissed_time", fallback="NULL")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT id AS reminder_id, {target} AS target_handle, {status} AS status,
                       due_at, {completed} AS completed_at, {dismissed} AS dismissed_at
                FROM reply_followup_reminders
                ORDER BY due_at ASC, id ASC"""
        ).fetchall()
    ]


def _record(findings: list[dict[str, Any]], counts: Counter[str], finding: dict[str, Any]) -> None:
    findings.append(finding)
    counts[finding["issue_type"]] += 1


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _col(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _hours(delta: timedelta) -> float:
    return round(delta.total_seconds() / 3600, 2)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(finding["issue_type"]), finding.get("target_handle") or "", finding.get("due_at") or "", finding.get("reminder_id") or 0)
