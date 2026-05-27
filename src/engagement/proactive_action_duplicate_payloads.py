"""Find proactive actions with duplicate draft or review payload text."""
from __future__ import annotations
from typing import Any
from evaluation._batch_report_common import *

ARTIFACT_TYPE = "proactive_action_duplicate_payloads"
DEFAULT_STATUS = "pending,queued,review"
DEFAULT_MIN_LENGTH = 40
DEFAULT_LIMIT = 100


def build_proactive_action_duplicate_payloads_report(rows: list[dict[str, Any]], *, status: str | None = DEFAULT_STATUS, min_length: int = DEFAULT_MIN_LENGTH, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None):
    positive("min_length", min_length); positive("limit", limit)
    gen = now_value(now); wanted = _set(status); grouped = defaultdict(list); scanned = 0
    for r in rows:
        st = lower(r.get("status") or r.get("state"), "unknown")
        if wanted and st not in wanted: continue
        text = _payload(r)
        norm = _norm_payload(text)
        if len(norm) < min_length: continue
        scanned += 1; grouped[norm].append({**r, "_payload": text, "_norm": norm})
    for r in rows:
        pass
    findings = []
    for key, items in grouped.items():
        targets = {clean(i.get("target_url") or i.get("url")) for i in items}
        if len(items) > 1 and len(targets) > 1:
            findings.append({"duplicate_key": key[:80], "action_ids": sorted([i.get("id") or i.get("action_id") for i in items], key=_sid), "target_urls": sorted(targets), "duplicate_reason": "exact_normalized_payload", "payload_length": len(key)})
    # lightweight duplicate opening clause
    openings = defaultdict(list)
    for items in grouped.values():
        for i in items:
            opening = " ".join(i["_norm"].split()[:12])
            if len(opening) >= min_length // 2: openings[opening].append(i)
    for key, items in openings.items():
        ids = sorted({i.get("id") or i.get("action_id") for i in items}, key=_sid); targets = sorted({clean(i.get("target_url") or i.get("url")) for i in items})
        if len(ids) > 1 and len(targets) > 1 and not any(set(ids) == set(f["action_ids"]) for f in findings):
            findings.append({"duplicate_key": key[:80], "action_ids": ids, "target_urls": targets, "duplicate_reason": "similar_opening_clause", "payload_length": len(key)})
    findings.sort(key=lambda f: (-len(f["action_ids"]), f["duplicate_reason"], f["duplicate_key"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"status": status, "min_length": min_length, "limit": limit}, "summary": {"actions_scanned": scanned, "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v}, "empty_state": empty_state(findings, "No duplicate proactive action payloads found.", schema_gap=bool(missing_tables or missing_columns))}


def build_proactive_action_duplicate_payloads_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); table = "proactive_actions" if "proactive_actions" in s else ("action_queue" if "action_queue" in s else None); mt = []; mc = {}; rows = []
    if not table: mt.append("proactive_actions|action_queue")
    else:
        c = s[table]
        if "id" not in c: mc[table] = ["id"]
        else: rows = load_table(conn, table, c, {"id": ("id", "action_id"), "status": ("status", "state"), "target_url": ("target_url", "url", "permalink"), "payload": ("payload", "draft_payload", "review_payload", "draft", "content", "body"), "metadata": ("metadata", "draft_metadata", "review_metadata")})
    return build_proactive_action_duplicate_payloads_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def _payload(row):
    text = clean(row.get("payload") or row.get("draft") or row.get("content") or row.get("body"))
    if text: return text
    try: meta = json.loads(clean(row.get("metadata")))
    except json.JSONDecodeError: return clean(row.get("metadata"))
    vals = []
    def walk(v):
        if isinstance(v, dict):
            for k, child in v.items():
                if k in {"payload", "draft", "content", "body", "text", "review_payload"}: vals.append(clean(child))
                walk(child)
        elif isinstance(v, list):
            for child in v: walk(child)
    walk(meta); return " ".join(v for v in vals if v)

def _norm_payload(v): return re.sub(r"\s+", " ", clean(v).lower()).strip()
def format_proactive_action_duplicate_payloads_json(r): return json_dumps(r)
def format_proactive_action_duplicate_payloads_text(r):
    lines = ["Proactive Action Duplicate Payloads", f"Generated: {r['generated_at']}", f"Totals: actions={r['summary']['actions_scanned']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - reason={f['duplicate_reason']} actions={','.join(map(str, f['action_ids']))}")
    return "\n".join(lines)
def _set(v): return {lower(p) for p in clean(v).split(",") if lower(p)}
def _sid(v):
    try: return (0, int(v))
    except (TypeError, ValueError): return (1, clean(v))
