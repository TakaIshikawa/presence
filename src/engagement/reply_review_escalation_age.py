"""Report reply drafts awaiting escalation or human review beyond an SLA."""
from __future__ import annotations
from typing import Any
from evaluation._batch_report_common import *

ARTIFACT_TYPE = "reply_review_escalation_age"
DEFAULT_STATUS = "escalated,human_review,needs_review,review"
DEFAULT_SLA_HOURS = 24
DEFAULT_LIMIT = 100


def build_reply_review_escalation_age_report(rows: list[dict[str, Any]], events: list[dict[str, Any]] | None = None, *, sla_hours: int = DEFAULT_SLA_HOURS, status: str | None = DEFAULT_STATUS, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    positive("sla_hours", sla_hours); positive("limit", limit)
    gen = now_value(now); wanted = _set(status); resolved = {"resolved", "sent", "posted", "closed", "approved"}
    latest = {}
    for e in events or []:
        rid = e.get("reply_draft_id") or e.get("draft_id"); ts = dt(e.get("created_at") or e.get("event_at"))
        if rid is not None and (rid not in latest or (ts or gen) > (latest[rid][0] or gen)): latest[rid] = (ts, e)
    findings = []
    for r in rows:
        st = lower(r.get("status") or r.get("review_status"), "unknown")
        if st in resolved or (wanted and st not in wanted): continue
        ts = dt(r.get("review_requested_at") or r.get("escalated_at") or r.get("created_at"))
        if (r.get("id") in latest) and latest[r.get("id")][0]: ts = latest[r.get("id")][0]
        age = round((gen - (ts or gen)).total_seconds() / 3600, 2)
        if age > sla_hours:
            ev = latest.get(r.get("id"), (None, {}))[1]
            reason = clean(r.get("reason") or ev.get("reason") or r.get("review_reason") or "review_sla_exceeded")
            findings.append({"reply_draft_id": r.get("id") or r.get("reply_draft_id"), "platform": clean(r.get("platform"), "unknown"), "status": st, "reviewer": r.get("reviewer") or r.get("assignee") or ev.get("reviewer") or ev.get("assignee"), "assignee": r.get("assignee") or ev.get("assignee"), "reason": reason, "age_hours": age, "recommended_action": "escalate reviewer follow-up" if "escalat" in st else "prioritize human review"})
    findings.sort(key=lambda f: (-f["age_hours"], _sid(f["reply_draft_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"sla_hours": sla_hours, "status": status, "limit": limit}, "summary": {"draft_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v}, "empty_state": empty_state(findings, "No reply review escalation SLA breaches found.", schema_gap=bool(missing_tables or missing_columns))}


def build_reply_review_escalation_age_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []; events = []
    if "reply_drafts" not in s: mt.append("reply_drafts")
    else:
        c = s["reply_drafts"]
        if "id" not in c: mc["reply_drafts"] = ["id"]
        else: rows = load_table(conn, "reply_drafts", c, {"id": ("id", "reply_draft_id"), "platform": ("platform",), "status": ("review_status", "status"), "reviewer": ("reviewer", "reviewer_id"), "assignee": ("assignee", "assignee_id"), "reason": ("reason", "review_reason", "escalation_reason"), "review_requested_at": ("review_requested_at", "escalated_at", "created_at"), "created_at": ("created_at",)})
    if "reply_review_events" in s:
        c = s["reply_review_events"]; events = load_table(conn, "reply_review_events", c, {"reply_draft_id": ("reply_draft_id", "draft_id"), "reason": ("reason",), "reviewer": ("reviewer", "reviewer_id"), "assignee": ("assignee",), "created_at": ("event_at", "created_at")})
    return build_reply_review_escalation_age_report(rows, events, missing_tables=mt, missing_columns=mc, **kw)


def format_reply_review_escalation_age_json(r): return json_dumps(r)
def format_reply_review_escalation_age_text(r):
    lines = ["Reply Review Escalation Age", f"Generated: {r['generated_at']}", f"Totals: drafts={r['summary']['draft_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - draft={f['reply_draft_id']} status={f['status']} age_hours={f['age_hours']} action={f['recommended_action']}")
    return "\n".join(lines)

def _set(v): return {lower(p) for p in clean(v).split(",") if lower(p)}
def _sid(v):
    try: return (0, int(v))
    except (TypeError, ValueError): return (1, clean(v))
