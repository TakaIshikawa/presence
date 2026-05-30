"""Find queued reply drafts with unsafe or unreviewed outbound links."""
from __future__ import annotations
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "reply_draft_link_safety_gaps"
DEFAULT_LIMIT = 50
DEFAULT_STATUSES = ("pending", "review")
SHORTENERS = {"bit.ly", "t.co", "tinyurl.com", "goo.gl", "ow.ly", "buff.ly", "cutt.ly", "rebrand.ly"}


def _urls(text_value: Any) -> list[str]:
    return re.findall(r"https?://[^\s)'\"<>]+", clean(text_value))


def _host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


def _set_blob(row: dict[str, Any], *names: str) -> str:
    return " ".join(clean(row.get(n)) for n in names)


def build_reply_draft_link_safety_gaps_report(rows: list[dict[str, Any]], *, statuses: tuple[str, ...] = DEFAULT_STATUSES, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit)
    wanted = {s.lower() for s in statuses}
    findings = []
    for r in rows:
        status = lower(r.get("status"), "pending")
        if wanted and status not in wanted:
            continue
        context = _set_blob(r, "relationship_context", "knowledge_context", "source_urls", "citations", "metadata")
        citation_blob = _set_blob(r, "citations", "source_urls", "metadata")
        for url in _urls(r.get("body") or r.get("text") or r.get("draft_text")):
            host = _host(url)
            reasons = []
            if url.startswith("http://"): reasons.append("insecure_http_url")
            if host in SHORTENERS: reasons.append("shortened_url")
            if url not in context and host not in context.lower(): reasons.append("link_not_in_context")
            if url not in citation_blob and host not in citation_blob.lower(): reasons.append("missing_citation")
            if lower(r.get("link_reviewed")) not in {"1", "true", "yes", "reviewed", "approved"}: reasons.append("unreviewed_link")
            for reason in dict.fromkeys(reasons):
                findings.append({"reply_id": r.get("reply_id") or r.get("id"), "target": r.get("target") or r.get("inbound_id") or r.get("target_id"), "url": url, "host": host, "reason": reason, "recommended_action": "review link safety and add matching citation/source metadata"})
    severity = {"insecure_http_url": 0, "shortened_url": 1, "link_not_in_context": 2, "missing_citation": 3, "unreviewed_link": 4}
    findings.sort(key=lambda f: (severity.get(f["reason"], 9), str(f["reply_id"]), f["url"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"statuses": sorted(wanted), "limit": limit}, "summary": {"reply_count": len(rows), "gap_count": len(findings), "shown": len(shown)}, "reply_link_safety_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No reply draft link safety gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_reply_draft_link_safety_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    table = "reply_drafts" if "reply_drafts" in s else ("reply_queue" if "reply_queue" in s else None)
    if not table:
        mt.append("reply_drafts|reply_queue")
    else:
        c = s[table]
        if not ({"body", "text", "draft_text"} & c): mc[table] = ["body|text|draft_text"]
        rows = load_table(conn, table, c, {"reply_id": ("id", "reply_id"), "target": ("target", "target_id", "inbound_id"), "status": ("status", "review_status"), "body": ("body", "text", "draft_text"), "relationship_context": ("relationship_context", "context"), "knowledge_context": ("knowledge_context",), "source_urls": ("source_urls", "sources"), "citations": ("citations", "citation_metadata"), "metadata": ("metadata",), "link_reviewed": ("link_reviewed", "links_reviewed")})
    return build_reply_draft_link_safety_gaps_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def format_reply_draft_link_safety_gaps_json(r): return json_dumps(r)


def format_reply_draft_link_safety_gaps_text(r):
    s = r["summary"]; lines = ["Reply Draft Link Safety Gaps", f"Generated: {r['generated_at']}", f"Totals: replies={s['reply_count']} gaps={s['gap_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["reply_link_safety_gaps"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "reply_id | target | host | reason"]
    for f in r["reply_link_safety_gaps"]: lines.append(f"{f['reply_id']} | {f['target']} | {f['host']} | {f['reason']}")
    return "\n".join(lines)
