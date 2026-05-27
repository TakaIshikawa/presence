"""Find generated content numeric claims without numeric evidence."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "content_claim_numeric_evidence_gaps"
DEFAULT_LIMIT = 50
NUMERIC_RE = re.compile(r"(\$[\d,.]+[kmb]?|\d+(?:\.\d+)?%|\d+\s*[:/]\s*\d+|\b\d{4}\b|\b\d+(?:,\d{3})+(?:\.\d+)?\b|\b\d+(?:\.\d+)?\s*(?:days?|weeks?|months?|years?|hours?|users?|customers?|people|times|x)\b)", re.I)


def _frag(text_value: str, start: int, end: int) -> str:
    lo = max(0, start - 60); hi = min(len(text_value), end + 60)
    return clean(text_value[lo:hi])


def build_content_claim_numeric_evidence_gaps_report(contents: list[dict[str, Any]], evidence: list[dict[str, Any]] | None = None, *, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit)
    by_content = defaultdict(str)
    for e in evidence or []:
        cid = clean(e.get("content_id") or e.get("generated_content_id"))
        by_content[cid] += " " + clean(e.get("evidence_text") or e.get("body") or e.get("url") or e.get("source_url") or e.get("claim"))
    findings = []
    for c in contents:
        cid = clean(c.get("content_id") or c.get("id"))
        body = clean(c.get("body") or c.get("text") or c.get("content"))
        ev = by_content.get(cid, "") + " " + clean(c.get("citations") or c.get("evidence_urls") or c.get("metadata"))
        ev_nums = set(m.group(1).lower() for m in NUMERIC_RE.finditer(ev))
        for m in NUMERIC_RE.finditer(body):
            token = m.group(1)
            if token.lower() not in ev_nums:
                findings.append({"content_id": cid, "extracted_claim_fragment": _frag(body, m.start(), m.end()), "numeric_token": token, "evidence_status": "missing_numeric_support" if ev else "missing_evidence", "recommended_action": "link claim check, citation, or source evidence containing the supporting numeric value"})
    findings.sort(key=lambda f: (f["content_id"], f["numeric_token"], f["extracted_claim_fragment"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"limit": limit}, "summary": {"content_count": len(contents), "numeric_gap_count": len(findings), "shown": len(shown)}, "numeric_evidence_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No content claim numeric evidence gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_content_claim_numeric_evidence_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; contents = []; evidence = []
    table = "generated_content" if "generated_content" in s else ("content" if "content" in s else None)
    if not table: mt.append("generated_content|content")
    else:
        c = s[table]
        if not ({"body", "text", "content"} & c): mc[table] = ["body|text|content"]
        contents = load_table(conn, table, c, {"content_id": ("id", "content_id"), "body": ("body", "text", "content"), "citations": ("citations", "citation_metadata"), "evidence_urls": ("evidence_urls", "source_urls"), "metadata": ("metadata",)})
    for t in [x for x in ("claim_checks", "content_knowledge_links", "citations", "content_evidence") if x in s]:
        c = s[t]; evidence += load_table(conn, t, c, {"content_id": ("content_id", "generated_content_id"), "evidence_text": ("evidence_text", "body", "text", "claim"), "url": ("url", "evidence_url", "source_url")})
    return build_content_claim_numeric_evidence_gaps_report(contents, evidence, missing_tables=mt, missing_columns=mc, **kw)


def format_content_claim_numeric_evidence_gaps_json(r): return json_dumps(r)
def format_content_claim_numeric_evidence_gaps_text(r):
    s = r["summary"]; lines = ["Content Claim Numeric Evidence Gaps", f"Generated: {r['generated_at']}", f"Totals: content={s['content_count']} gaps={s['numeric_gap_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["numeric_evidence_gaps"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "content_id | numeric_token | evidence_status"]
    for f in r["numeric_evidence_gaps"]: lines.append(f"{f['content_id']} | {f['numeric_token']} | {f['evidence_status']}")
    return "\n".join(lines)
