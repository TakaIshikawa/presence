"""Measure author concentration and missing attribution in knowledge sources."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="knowledge_source_author_diversity"; DEFAULT_WINDOW_DAYS=30; DEFAULT_MAX_AUTHOR_SHARE=0.5; DEFAULT_LIMIT=100
def build_knowledge_source_author_diversity_report(rows:list[dict[str,Any]],*,window_days:int=DEFAULT_WINDOW_DAYS,max_author_share:float=DEFAULT_MAX_AUTHOR_SHARE,source_kind=None,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("window_days",window_days); bounded_share("max_author_share",max_author_share); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=window_days); filtered=[]
    for r in rows:
        if source_kind and lower(r.get("source_kind") or r.get("kind"))!=lower(source_kind): continue
        created=dt(r.get("created_at") or r.get("published_at") or r.get("ingested_at"))
        if created and created<cutoff: continue
        meta=_meta(r); author=clean(r.get("author") or r.get("source_author") or meta.get("author") or meta.get("byline"))
        filtered.append({**r,"_author":author})
    counts=Counter(r["_author"] for r in filtered if r["_author"]); missing=sum(1 for r in filtered if not r["_author"]); total=len(filtered); top_author,top_count=(counts.most_common(1)[0] if counts else (None,0)); share=round(top_count/max(1,total),4)
    findings=[]
    if top_author and share>max_author_share: findings.append({"issue_type":"dominant_author_concentration","author":top_author,"source_id":None,"source_kind":source_kind,"author_share":share,"source_count":top_count})
    for r in filtered:
        if not r["_author"]: findings.append({"issue_type":"missing_author_attribution","author":None,"source_id":r.get("id") or r.get("source_id"),"source_kind":r.get("source_kind") or r.get("kind"),"author_share":None,"source_count":1})
    findings.sort(key=lambda f:(f["issue_type"], clean(f["author"]), _sid(f["source_id"])))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"window_days":window_days,"max_author_share":max_author_share,"source_kind":source_kind,"limit":limit},"summary":{"source_count":total,"author_count":len(counts),"missing_author_count":missing,"top_author":top_author,"top_author_share":share,"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No knowledge source author diversity issues found.",schema_gap=bool(missing_tables or missing_columns))}
def build_knowledge_source_author_diversity_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="knowledge_sources" if "knowledge_sources" in s else ("curated_sources" if "curated_sources" in s else None); mt=[]; mc={}; rows=[]
    if not table: mt.append("knowledge_sources|curated_sources")
    else:
        c=s[table]
        if "id" not in c: mc[table]=["id"]
        else: rows=load_table(conn,table,c,{"id":("id","source_id"),"source_kind":("source_kind","kind","type"),"author":("author","source_author","byline"),"metadata":("metadata","source_metadata"),"created_at":("created_at","published_at","ingested_at")})
    return build_knowledge_source_author_diversity_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def _meta(r):
    try:return json.loads(clean(r.get("metadata"))) if clean(r.get("metadata")) else {}
    except json.JSONDecodeError:return {}
def format_knowledge_source_author_diversity_json(r): return json_dumps(r)
def format_knowledge_source_author_diversity_text(r):
    s=r["summary"]; lines=["Knowledge Source Author Diversity",f"Generated: {r['generated_at']}",f"Totals: sources={s['source_count']} authors={s['author_count']} missing={s['missing_author_count']} top_share={s['top_author_share']} findings={s['finding_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - issue={f['issue_type']} author={f['author']} source={f['source_id']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
