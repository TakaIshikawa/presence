"""Flag generated content citing stale knowledge sources."""
from __future__ import annotations
from collections import Counter
from datetime import timedelta
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="knowledge_source_citation_recency"; DEFAULT_LIMIT=50; DEFAULT_MAX_AGE_DAYS=180
def build_knowledge_source_citation_recency_report(rows:list[dict[str,Any]],*,max_age_days:int=DEFAULT_MAX_AGE_DAYS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("max_age_days",max_age_days); positive("limit",limit); gen=now_value(now); findings=[]
    for r in rows:
        ts=dt(r.get("source_at") or r.get("last_refreshed_at") or r.get("updated_at") or r.get("created_at"))
        age=(gen-ts).days if ts else None
        if age is None or age>max_age_days:
            findings.append({"content_id":r.get("content_id"),"source_id":r.get("source_id"),"source_url":r.get("source_url"),"source_title":r.get("source_title"),"source_age_days":age,"cited_at":(dt(r.get("cited_at")) or dt(r.get("content_created_at")) or gen).isoformat(),"created_at":r.get("content_created_at"),"topic":r.get("topic"),"source_domain":domain(r.get("source_url")),"recommended_action":"refresh or replace stale cited source"})
    findings.sort(key=lambda f: (-(f["source_age_days"] or 999999),str(f["content_id"]),str(f["source_id"])))
    shown=findings[:limit]; by=Counter(clean(f.get("topic"),"unknown") for f in findings); bd=Counter(clean(f.get("source_domain"),"unknown") for f in findings)
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_age_days":max_age_days,"limit":limit},"summary":{"citation_count":len(rows),"stale_citation_count":len(findings),"shown":len(shown),"by_topic":dict(sorted(by.items())),"by_source_domain":dict(sorted(bd.items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No stale knowledge source citations found.",schema_gap=bool(missing_tables or missing_columns))}
def build_knowledge_source_citation_recency_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]; link=None; src=None
    for t in ("content_knowledge_links","generated_content_knowledge_links","knowledge_citations"):
        if t in s: link=t; break
    for t in ("knowledge_sources","knowledge_items"):
        if t in s: src=t; break
    if not link: mt.append("content_knowledge_links|knowledge_citations")
    if not src: mt.append("knowledge_sources|knowledge_items")
    if link and src:
        lc=s[link]; sc=s[src]
        if not ({"content_id","generated_content_id"} & lc): mc.setdefault(link,[]).append("content_id|generated_content_id")
        if not ({"source_id","knowledge_source_id","knowledge_item_id"} & lc): mc.setdefault(link,[]).append("source_id|knowledge_source_id")
        if not ({"id","source_id"} & sc): mc.setdefault(src,[]).append("id|source_id")
        if not mc:
            lcid="content_id" if "content_id" in lc else "generated_content_id"; lsid=next(c for c in ("source_id","knowledge_source_id","knowledge_item_id") if c in lc); sid="id" if "id" in sc else "source_id"
            rows=[dict(r) for r in conn.execute(f"SELECT l.{lcid} AS content_id, l.{lsid} AS source_id, {pick(lc,'cited_at','created_at',out='cited_at')}, {pick(sc,'url','source_url',out='source_url')}, {pick(sc,'title','name',out='source_title')}, {pick(sc,'source_at','published_at','last_refreshed_at','updated_at','created_at',out='source_at')}, {pick(sc,'topic','category',out='topic')} FROM {link} l JOIN {src} s ON s.{sid}=l.{lsid}")]
    return build_knowledge_source_citation_recency_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_knowledge_source_citation_recency_json(r): return json_dumps(r)
def format_knowledge_source_citation_recency_text(r):
    s=r["summary"]; lines=["Knowledge Source Citation Recency",f"Generated: {r['generated_at']}",f"Totals: citations={s['citation_count']} stale={s['stale_citation_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","content_id | source_id | age_days | source_url"]
    for f in r["findings"]: lines.append(f"{f['content_id']} | {f['source_id']} | {f['source_age_days']} | {f['source_url']}")
    return "\n".join(lines)
