"""Find lower-priority proactive actions overtaking older high-priority actions."""
from __future__ import annotations
from typing import Any
from evaluation._batch_report_common import *

ARTIFACT_TYPE = "proactive_action_priority_inversion"
DEFAULT_OPEN_STATUS = "open,pending,queued,review"
DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_LIMIT = 100
TABLES = ("proactive_actions", "action_queue")
HIGH_MAX_RANK = 2


def build_proactive_action_priority_inversion_report(rows: list[dict[str, Any]], *, open_status: str | None = DEFAULT_OPEN_STATUS, lookback_days: int = DEFAULT_LOOKBACK_DAYS, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    positive("lookback_days", lookback_days); positive("limit", limit)
    gen = now_value(now); cutoff = gen - timedelta(days=lookback_days); wanted = _set(open_status)
    candidates = []
    for r in rows:
        status = lower(r.get("status") or r.get("state"), "open")
        if wanted and status not in wanted: continue
        created = dt(r.get("created_at")) or gen
        if created < cutoff: continue
        due = dt(r.get("due_at")) or created
        reviewed = dt(r.get("reviewed_at")) or due
        candidates.append({**r, "status": status, "_created": created, "_scheduled": reviewed, "_rank": priority_rank(r)})
    findings = []
    highs = [r for r in candidates if r["_rank"] <= HIGH_MAX_RANK]
    lows = [r for r in candidates if r["_rank"] > HIGH_MAX_RANK]
    for high in highs:
        for low in lows:
            if high is low: continue
            if high["_created"] < low["_created"] and low["_scheduled"] < high["_scheduled"]:
                findings.append({"blocked_action_id": high.get("id") or high.get("action_id"), "blocked_priority": priority_label(high), "blocked_priority_rank": high["_rank"], "blocked_created_at": high["_created"].isoformat(), "blocked_scheduled_at": high["_scheduled"].isoformat(), "overtaking_action_id": low.get("id") or low.get("action_id"), "overtaking_priority": priority_label(low), "overtaking_priority_rank": low["_rank"], "overtaking_created_at": low["_created"].isoformat(), "overtaking_scheduled_at": low["_scheduled"].isoformat(), "status": high["status"], "gap_hours": round((high["_scheduled"] - low["_scheduled"]).total_seconds() / 3600, 2)})
    findings.sort(key=lambda f: (-f["gap_hours"], f["blocked_priority_rank"], _sid(f["blocked_action_id"]), _sid(f["overtaking_action_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"open_status": open_status, "lookback_days": lookback_days, "limit": limit}, "summary": {"actions_scanned": len(candidates), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v}, "empty_state": empty_state(findings, "No proactive action priority inversions found.", schema_gap=bool(missing_tables or missing_columns))}


def build_proactive_action_priority_inversion_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); table = next((t for t in TABLES if t in s), None); mt = []; mc = {}; rows = []
    if not table: mt.append("proactive_actions|action_queue")
    else:
        c = s[table]
        if "id" not in c: mc[table] = ["id"]
        else: rows = load_table(conn, table, c, {"id": ("id", "action_id"), "status": ("status", "state"), "priority": ("priority", "priority_label"), "severity": ("severity",), "priority_score": ("priority_score", "priority_rank", "score"), "due_at": ("due_at", "scheduled_at", "next_review_at"), "reviewed_at": ("reviewed_at", "last_reviewed_at"), "created_at": ("created_at", "inserted_at")})
    return build_proactive_action_priority_inversion_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def priority_rank(row: dict[str, Any]) -> int:
    n = to_int(row.get("priority_score") or row.get("priority_rank") or (row.get("priority") if str(row.get("priority", "")).strip().isdigit() else None) or (row.get("severity") if str(row.get("severity", "")).strip().isdigit() else None), 0)
    if n:
        return max(1, min(5, n))
    label = lower(row.get("priority") or row.get("severity"), "normal")
    if label in {"p0", "critical", "urgent", "blocker"}: return 1
    if label in {"p1", "high", "major"}: return 2
    if label in {"p2", "medium", "normal"}: return 3
    if label in {"p3", "low", "minor"}: return 4
    return 5


def priority_label(row: dict[str, Any]) -> str:
    return clean(row.get("priority") or row.get("severity") or row.get("priority_score") or "normal")


def format_proactive_action_priority_inversion_json(r): return json_dumps(r)
def format_proactive_action_priority_inversion_text(r):
    lines = ["Proactive Action Priority Inversion", f"Generated: {r['generated_at']}", f"Totals: actions={r['summary']['actions_scanned']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines.append("Findings:")
    for f in r["findings"]: lines.append(f"  - blocked={f['blocked_action_id']} overtaken_by={f['overtaking_action_id']} gap_hours={f['gap_hours']}")
    return "\n".join(lines)


def _set(v): return {lower(p) for p in clean(v).split(",") if lower(p)}
def _sid(v):
    try: return (0, int(v))
    except (TypeError, ValueError): return (1, clean(v))
