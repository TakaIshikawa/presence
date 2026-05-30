"""Find proactive actions whose due dates slipped without completion."""
from __future__ import annotations
from datetime import datetime
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "proactive_action_due_date_slippage"
DEFAULT_LIMIT = 50
DONE = {"completed", "complete", "dismissed", "sent", "resolved", "archived", "archive", "published", "reviewed"}


def build_proactive_action_due_date_slippage_report(rows: list[dict[str, Any]], *, now: datetime | None = None, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None):
    positive("limit", limit); gen = now_value(now)
    findings = []
    for r in rows:
        status = lower(r.get("status"), "open")
        if status in DONE: continue
        due = dt(r.get("due_at") or r.get("scheduled_for") or r.get("review_by"))
        if not due or due >= gen: continue
        days = (gen - due).days
        findings.append({"action_id": r.get("action_id") or r.get("id"), "target": clean(r.get("target") or r.get("relationship") or r.get("recipient")), "platform": clean(r.get("platform")), "due_at": due.isoformat(), "days_overdue": days, "status": status, "recommended_action": "reschedule, complete, dismiss, publish, or review the overdue action"})
    findings.sort(key=lambda f: (-f["days_overdue"], str(f["action_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit}, "summary": {"action_count": len(rows), "slipped_count": len(findings), "shown": len(shown)}, "slipped_actions": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No proactive action due date slippage found.", schema_gap=bool(missing_tables or missing_columns))}


def build_proactive_action_due_date_slippage_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    table = next((t for t in ("proactive_actions", "strategic_actions", "resolved_actions") if t in s), None)
    if not table: mt.append("proactive_actions|strategic_actions|resolved_actions")
    else:
        c = s[table]
        if not ({"due_at", "scheduled_for", "review_by"} & c): mc[table] = ["due_at|scheduled_for|review_by"]
        rows = load_table(conn, table, c, {"action_id": ("id", "action_id"), "target": ("target", "relationship", "recipient"), "platform": ("platform",), "due_at": ("due_at", "scheduled_for", "review_by"), "scheduled_for": ("scheduled_for",), "review_by": ("review_by",), "status": ("status", "state")})
    return build_proactive_action_due_date_slippage_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_proactive_action_due_date_slippage_json(r): return json_dumps(r)
def format_proactive_action_due_date_slippage_text(r):
    s = r["summary"]; lines = ["Proactive Action Due Date Slippage", f"Generated: {r['generated_at']}", f"Totals: actions={s['action_count']} slipped={s['slipped_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["slipped_actions"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "action_id | target | platform | due_at | days_overdue | status"]
    for f in r["slipped_actions"]: lines.append(f"{f['action_id']} | {f['target']} | {f['platform']} | {f['due_at']} | {f['days_overdue']} | {f['status']}")
    return "\n".join(lines)
