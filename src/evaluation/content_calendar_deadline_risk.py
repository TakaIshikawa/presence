"""Flag scheduled content that is near deadline but not ready."""
from __future__ import annotations

from typing import Any

from ._batch_report_common import clean, dt, empty_state, json_dumps, now_value, positive

ARTIFACT_TYPE = "content_calendar_deadline_risk"
DEFAULT_LIMIT = 50
DEFAULT_WINDOW_HOURS = 48
READY_STATUSES = {"draft-ready", "draft_ready", "ready", "approved", "scheduled"}


def build_content_calendar_deadline_risk_report(rows: list[dict[str, Any]], *, window_hours: int = DEFAULT_WINDOW_HOURS, limit: int = DEFAULT_LIMIT, now: Any = None) -> dict[str, Any]:
    positive("window_hours", window_hours)
    positive("limit", limit)
    gen = now_value(now)
    findings = []
    for row in rows:
        planned = dt(row.get("planned_at") or row.get("planned_publish_at") or row.get("publish_at"))
        status = clean(row.get("status")).lower()
        if planned is None or status in READY_STATUSES:
            continue
        hours = round((planned - gen).total_seconds() / 3600, 2)
        if hours <= window_hours:
            risk = "overdue" if hours < 0 else "high" if hours <= 12 else "medium"
            findings.append({"idea_id": clean(row.get("idea_id") or row.get("id")), "planned_at": planned.isoformat(), "status": status, "hours_until_deadline": hours, "risk_level": risk})
    findings.sort(key=lambda f: (f["hours_until_deadline"], f["idea_id"]))
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"window_hours": window_hours, "limit": limit}, "summary": {"idea_count": len(rows), "finding_count": len(findings)}, "findings": findings[:limit], "empty_state": empty_state(findings, "No content calendar deadline risks found.")}


def format_content_calendar_deadline_risk_json(report: dict[str, Any]) -> str:
    return json_dumps(report)
