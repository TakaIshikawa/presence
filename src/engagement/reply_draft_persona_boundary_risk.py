"""Flag reply drafts with first-person claims beyond the persona boundary."""
from __future__ import annotations
from typing import Any
from evaluation._batch_report_common import *

ARTIFACT_TYPE = "reply_draft_persona_boundary_risk"
DEFAULT_STATUS = "draft,pending,queued"
DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 100
TEXT_COLUMNS = ("body", "content", "text", "draft_text", "reply_text")
PATTERNS = [
    ("employment_claim", 3, re.compile(r"\bI\s+(?:work|worked|am employed|was employed)\s+(?:at|for|with)\b", re.I)),
    ("private_access_claim", 3, re.compile(r"\bI\s+(?:have|had|can access|accessed)\s+(?:internal|private|confidential|non[- ]public)\b", re.I)),
    ("guaranteed_outcome", 2, re.compile(r"\bI\s+(?:guarantee|promise|will ensure|can guarantee)\b", re.I)),
    ("unverifiable_experience", 2, re.compile(r"\bI\s+(?:personally used|personally saw|was in the room|spoke directly with)\b", re.I)),
    ("commitment", 2, re.compile(r"\bI\s+will\s+(?:ship|fix|refund|approve|hire|pay|deliver)\b", re.I)),
]


def build_reply_draft_persona_boundary_risk_report(rows: list[dict[str, Any]], *, status: str | None = DEFAULT_STATUS, window_days: int = DEFAULT_WINDOW_DAYS, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    positive("window_days", window_days); positive("limit", limit)
    gen = now_value(now); cutoff = gen - timedelta(days=window_days); wanted = _set(status); findings = []; scanned = 0
    for r in rows:
        st = lower(r.get("status") or r.get("draft_status"), "draft")
        if wanted and st not in wanted: continue
        created = dt(r.get("created_at") or r.get("updated_at"))
        if created and created < cutoff: continue
        scanned += 1; body = clean(r.get("body") or r.get("content") or r.get("text"))
        for risk_type, severity, pat in PATTERNS:
            m = pat.search(body)
            if m:
                findings.append({"reply_draft_id": r.get("id") or r.get("reply_draft_id"), "platform": clean(r.get("platform"), "unknown"), "status": st, "matched_phrase": m.group(0), "risk_type": risk_type, "severity": severity})
    findings.sort(key=lambda f: (-f["severity"], f["risk_type"], _sid(f["reply_draft_id"])))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"status": status, "window_days": window_days, "limit": limit}, "summary": {"drafts_scanned": scanned, "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v}, "empty_state": empty_state(findings, "No reply draft persona boundary risks found.", schema_gap=bool(missing_tables or missing_columns))}


def build_reply_draft_persona_boundary_risk_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    if "reply_drafts" not in s: mt.append("reply_drafts")
    else:
        c = s["reply_drafts"]
        miss = []
        if "id" not in c: miss.append("id")
        if not set(TEXT_COLUMNS) & c: miss.append("body|content|text|draft_text|reply_text")
        if miss: mc["reply_drafts"] = miss
        else: rows = load_table(conn, "reply_drafts", c, {"id": ("id", "reply_draft_id"), "platform": ("platform",), "status": ("draft_status", "status"), "body": TEXT_COLUMNS, "created_at": ("created_at", "updated_at")})
    return build_reply_draft_persona_boundary_risk_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_reply_draft_persona_boundary_risk_json(r): return json_dumps(r)
def format_reply_draft_persona_boundary_risk_text(r):
    lines = ["Reply Draft Persona Boundary Risk", f"Generated: {r['generated_at']}", f"Totals: drafts={r['summary']['drafts_scanned']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - draft={f['reply_draft_id']} risk={f['risk_type']} severity={f['severity']} phrase={f['matched_phrase']}")
    return "\n".join(lines)

def _set(v): return {lower(p) for p in clean(v).split(",") if lower(p)}
def _sid(v):
    try: return (0, int(v))
    except (TypeError, ValueError): return (1, clean(v))
