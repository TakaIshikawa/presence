"""Report proactive action follow-up readiness gaps."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, parse_time, schema, text_format, utc


ARTIFACT_TYPE = "proactive_action_followup_readiness"
DEFAULT_WINDOW_HOURS = 48
DEFAULT_LIMIT = 100
REASONS = ("missing_followup_reminder", "orphan_followup_reminder", "premature_followup_due", "duplicate_target_window")


def build_proactive_action_followup_readiness_report(rows: list[dict[str, Any]], *, window_hours: int = DEFAULT_WINDOW_HOURS, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if window_hours < 0:
        raise ValueError("window_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    actions_by_handle: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        status = clean(row.get("action_status")).lower()
        handle = clean(row.get("target_author_handle")).lower()
        if row.get("action_id") is not None and handle:
            actions_by_handle[handle].append(row)
        if status in {"approved", "posted"} and row.get("reminder_id") is None:
            findings.append({"reason": "missing_followup_reminder", "id": row.get("action_id"), "target_handle": handle, "detail": "approved or posted proactive action has no follow-up reminder"})
        if row.get("reminder_id") is not None and row.get("action_id") is None:
            findings.append({"reason": "orphan_followup_reminder", "id": row.get("reminder_id"), "target_handle": clean(row.get("reminder_target_handle")), "detail": "follow-up reminder points at missing action"})
        due_at = parse_time(row.get("due_at"))
        approved_at = parse_time(row.get("reviewed_at") or row.get("posted_at"))
        if due_at and (row.get("action_id") is None or approved_at is None or due_at < approved_at):
            findings.append({"reason": "premature_followup_due", "id": row.get("reminder_id") or row.get("action_id"), "target_handle": handle, "detail": "reminder due before action approval/posting"})
    for handle, actions in actions_by_handle.items():
        dated = [(parse_time(row.get("posted_at") or row.get("reviewed_at") or row.get("created_at")), row) for row in actions]
        dated = [(dt, row) for dt, row in dated if dt is not None]
        dated.sort(key=lambda item: item[0])
        for idx in range(1, len(dated)):
            if (dated[idx][0] - dated[idx - 1][0]).total_seconds() <= window_hours * 3600:
                findings.append({"reason": "duplicate_target_window", "id": dated[idx][1].get("action_id"), "target_handle": handle, "detail": "duplicate target handle inside follow-up window"})
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("target_handle")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"window_hours": window_hours, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_proactive_action_followup_readiness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"proactive_actions": {"id"}, "reply_followup_reminders": {"id"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_proactive_action_followup_readiness_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    pa, rr = db_schema["proactive_actions"], db_schema["reply_followup_reminders"]
    action_select = ["pa.id AS action_id", "NULL AS reminder_id", col(pa, "status", "pa") + " AS action_status", col(pa, "target_author_handle", "pa") + " AS target_author_handle", "NULL AS reminder_target_handle", "NULL AS due_at", col(pa, "reviewed_at", "pa") + " AS reviewed_at", col(pa, "posted_at", "pa") + " AS posted_at", col(pa, "created_at", "pa") + " AS created_at"]
    reminder_select = ["pa.id AS action_id", "rr.id AS reminder_id", col(pa, "status", "pa") + " AS action_status", col(pa, "target_author_handle", "pa") + " AS target_author_handle", col(rr, "target_handle", "rr") + " AS reminder_target_handle", col(rr, "due_at", "rr") + " AS due_at", col(pa, "reviewed_at", "pa") + " AS reviewed_at", col(pa, "posted_at", "pa") + " AS posted_at", col(pa, "created_at", "pa") + " AS created_at"]
    source_join = "rr.source_action_id = pa.id OR (rr.source_type = 'proactive_actions' AND rr.source_id = pa.id)"
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(action_select)} FROM proactive_actions pa WHERE NOT EXISTS (SELECT 1 FROM reply_followup_reminders rr WHERE {source_join}) UNION ALL SELECT {', '.join(reminder_select)} FROM reply_followup_reminders rr LEFT JOIN proactive_actions pa ON {source_join} ORDER BY action_id, reminder_id").fetchall()]
    return build_proactive_action_followup_readiness_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_proactive_action_followup_readiness_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_proactive_action_followup_readiness_text(report: dict[str, Any]) -> str:
    return text_format("Proactive Action Followup Readiness", report)
