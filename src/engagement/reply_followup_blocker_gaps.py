"""Find blocked reply follow-ups with missing or generic blocker reasons."""
from __future__ import annotations
from typing import Any
from evaluation._batch_report_common import *

ARTIFACT_TYPE = "reply_followup_blocker_gaps"
DEFAULT_STATUS = "blocked,deferred"
DEFAULT_MIN_REASON_LENGTH = 12
DEFAULT_LIMIT = 100
PLACEHOLDERS = {"tbd", "todo", "none", "n/a", "na", "blocked", "unknown", "later", "waiting"}


def build_reply_followup_blocker_gaps_report(rows: list[dict[str, Any]], *, status: str | None = DEFAULT_STATUS, min_reason_length: int = DEFAULT_MIN_REASON_LENGTH, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    positive("min_reason_length", min_reason_length); positive("limit", limit)
    gen = now_value(now); wanted = _set(status); findings = []; seen = Counter()
    for r in rows:
        reason = clean(r.get("blocker_reason") or r.get("hold_reason") or r.get("reason")); key = lower(reason); seen[key] += 1
    for r in rows:
        st = lower(r.get("status") or r.get("state"), "unknown")
        if wanted and st not in wanted: continue
        reason = clean(r.get("blocker_reason") or r.get("hold_reason") or r.get("reason"))
        gap = None
        if not reason: gap = "missing_blocker_reason"
        elif lower(reason) in PLACEHOLDERS: gap = "placeholder_blocker_reason"
        elif len(reason) < min_reason_length: gap = "short_blocker_reason"
        elif seen[lower(reason)] > 1 and tokens(reason) <= {"waiting", "blocked", "need", "needs", "info", "input", "follow", "up"}: gap = "repeated_generic_blocker_reason"
        if gap:
            created = dt(r.get("created_at") or r.get("updated_at")); age = round((gen - (created or gen)).total_seconds() / 3600, 2)
            findings.append({"followup_id": r.get("id") or r.get("followup_id") or r.get("action_id"), "status": st, "blocker_reason": reason or None, "gap_reason": gap, "age_hours": age})
    findings.sort(key=lambda f: (f["gap_reason"], -f["age_hours"], _sid(f["followup_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"status": status, "min_reason_length": min_reason_length, "limit": limit}, "summary": {"items_scanned": len(rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v}, "empty_state": empty_state(findings, "No reply follow-up blocker gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_reply_followup_blocker_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); table = "reply_followups" if "reply_followups" in s else ("proactive_actions" if "proactive_actions" in s else None); mt = []; mc = {}; rows = []
    if not table: mt.append("reply_followups|proactive_actions")
    else:
        c = s[table]
        if "id" not in c: mc[table] = ["id"]
        else: rows = load_table(conn, table, c, {"id": ("id", "followup_id", "action_id"), "status": ("status", "state"), "blocker_reason": ("blocker_reason", "hold_reason", "reason"), "created_at": ("created_at", "updated_at")})
    return build_reply_followup_blocker_gaps_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_reply_followup_blocker_gaps_json(r): return json_dumps(r)
def format_reply_followup_blocker_gaps_text(r):
    lines = ["Reply Followup Blocker Gaps", f"Generated: {r['generated_at']}", f"Totals: items={r['summary']['items_scanned']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - followup={f['followup_id']} gap={f['gap_reason']} age_hours={f['age_hours']}")
    return "\n".join(lines)

def _set(v): return {lower(p) for p in clean(v).split(",") if lower(p)}
def _sid(v):
    try: return (0, int(v))
    except (TypeError, ValueError): return (1, clean(v))
