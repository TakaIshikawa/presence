"""Find reused knowledge sources missing license or attribution metadata."""
from __future__ import annotations
from collections import Counter
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "knowledge_source_license_attribution_gaps"
DEFAULT_LIMIT = 50
UNKNOWN = {"", "unknown", "n/a", "none", "unspecified"}
BAD_REUSE = {"all rights reserved", "proprietary", "no reuse", "restricted"}


def build_knowledge_source_license_attribution_gaps_report(sources: list[dict[str, Any]], links: list[dict[str, Any]] | None = None, *, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit)
    usage = Counter(clean(l.get("source_id") or l.get("knowledge_id")) for l in (links or []))
    findings = []
    for s in sources:
        sid = clean(s.get("source_id") or s.get("id"))
        license_name = lower(s.get("license") or s.get("license_name") or s.get("rights"))
        used = usage.get(sid, to_int(s.get("usage_count"), 0))
        checks = []
        if license_name in UNKNOWN: checks.append("missing_license")
        if used and license_name in UNKNOWN: checks.append("unknown_license_on_reused_source")
        if used and any(b in license_name for b in BAD_REUSE): checks.append("incompatible_license_for_reuse")
        if not clean(s.get("source_url") or s.get("url")): checks.append("missing_source_url")
        if not clean(s.get("author") or s.get("creator") or s.get("attribution")): checks.append("missing_author_attribution")
        for reason in dict.fromkeys(checks):
            findings.append({"source_id": sid, "title": clean(s.get("title") or s.get("name")), "usage_count": used, "license": clean(s.get("license") or s.get("license_name") or s.get("rights")), "reason": reason, "recommended_action": "add license, source URL, author, and attribution text before reuse"})
    order = {"incompatible_license_for_reuse": 0, "unknown_license_on_reused_source": 1, "missing_license": 2, "missing_author_attribution": 3, "missing_source_url": 4}
    findings.sort(key=lambda f: (order.get(f["reason"], 9), -f["usage_count"], f["source_id"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "filters": {"limit": limit}, "summary": {"source_count": len(sources), "usage_link_count": len(links or []), "gap_count": len(findings), "shown": len(shown)}, "source_license_attribution_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No knowledge source license attribution gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_knowledge_source_license_attribution_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; sources = []; links = []
    table = "knowledge_sources" if "knowledge_sources" in s else ("knowledge" if "knowledge" in s else None)
    if not table: mt.append("knowledge_sources|knowledge")
    else:
        c = s[table]
        if not ({"license", "license_name", "rights", "source_url", "url", "author", "attribution"} & c): mc[table] = ["license|source_url|author|attribution"]
        sources = load_table(conn, table, c, {"source_id": ("id", "source_id", "knowledge_id"), "title": ("title", "name"), "license": ("license", "license_name", "rights"), "source_url": ("source_url", "url"), "author": ("author", "creator"), "attribution": ("attribution", "attribution_text"), "usage_count": ("usage_count",)})
    link_table = "content_knowledge_links" if "content_knowledge_links" in s else ("generated_content_knowledge_links" if "generated_content_knowledge_links" in s else None)
    if link_table:
        c = s[link_table]; links = load_table(conn, link_table, c, {"source_id": ("source_id", "knowledge_id")})
    return build_knowledge_source_license_attribution_gaps_report(sources, links, missing_tables=mt, missing_columns=mc, **kw)


def format_knowledge_source_license_attribution_gaps_json(r): return json_dumps(r)
def format_knowledge_source_license_attribution_gaps_text(r):
    s = r["summary"]; lines = ["Knowledge Source License Attribution Gaps", f"Generated: {r['generated_at']}", f"Totals: sources={s['source_count']} gaps={s['gap_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["source_license_attribution_gaps"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "source_id | usage_count | reason"]
    for f in r["source_license_attribution_gaps"]: lines.append(f"{f['source_id']} | {f['usage_count']} | {f['reason']}")
    return "\n".join(lines)
