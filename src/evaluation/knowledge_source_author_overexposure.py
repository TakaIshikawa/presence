"""Report overexposed knowledge source authors in recent content links."""
from __future__ import annotations
from collections import defaultdict
from datetime import timedelta
from typing import Any
import json
from ._batch_report_common import bounded_share, clean, connection, dt, empty_state, flatten_missing, json_dumps, now_value, pick, positive, schema

ARTIFACT_TYPE="knowledge_source_author_overexposure"; DEFAULT_WINDOW_DAYS=30; DEFAULT_MAX_AUTHOR_SHARE=0.5; DEFAULT_LIMIT=100; UNKNOWN_AUTHOR="unknown_author"

def build_knowledge_source_author_overexposure_report(rows:list[dict[str,Any]],*,window_days:int=DEFAULT_WINDOW_DAYS,max_author_share:float=DEFAULT_MAX_AUTHOR_SHARE,limit:int=DEFAULT_LIMIT,now:Any=None,missing_tables=None,missing_columns=None)->dict[str,Any]:
    positive("window_days",window_days); bounded_share("max_author_share",max_author_share); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=window_days); authors:dict[str,dict[str,Any]]=defaultdict(lambda:{"source_ids":set(),"content_ids":set(),"link_count":0})
    scanned=0
    for row in rows:
        linked=dt(row.get("linked_at") or row.get("created_at") or row.get("published_at") or row.get("ingested_at"))
        if linked and linked<cutoff: continue
        scanned+=1; author=_author(row); bucket=authors[author]; bucket["link_count"]+=1
        if clean(row.get("source_id")): bucket["source_ids"].add(clean(row.get("source_id")))
        if clean(row.get("content_id")): bucket["content_ids"].add(clean(row.get("content_id")))
    total=sum(v["link_count"] for v in authors.values()); findings=[]
    for author,data in authors.items():
        share=round(data["link_count"]/max(1,total),4)
        if share>max_author_share:
            findings.append({"author":author,"author_share":share,"link_count":data["link_count"],"source_count":len(data["source_ids"]),"content_count":len(data["content_ids"]),"recommendation":"Diversify supporting knowledge sources before publishing more content in this window."})
    findings.sort(key=lambda f:(-f["author_share"],-f["link_count"],f["author"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"window_days":window_days,"max_author_share":max_author_share,"limit":limit},"summary":{"rows_scanned":scanned,"link_count":total,"author_count":len(authors),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No knowledge source author overexposure found.",schema_gap=bool(missing_tables or missing_columns))}

def build_knowledge_source_author_overexposure_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); link_table=next((t for t in ("content_knowledge_links","content_knowledge_link","content_sources","knowledge_content_links") if t in s),None); source_table=next((t for t in ("knowledge_sources","curated_sources") if t in s),None)
    if not link_table or not source_table:
        return build_knowledge_source_author_overexposure_report([],missing_tables=[name for name,ok in (("content_knowledge_links",link_table),("knowledge_sources",source_table)) if not ok],**kw)
    lc=s[link_table]; sc=s[source_table]; missing={}
    for table,cols,required in ((link_table,lc,["content_id","source_id"]),(source_table,sc,["id"])):
        miss=[c for c in required if c not in cols]
        if miss: missing[table]=miss
    if missing: return build_knowledge_source_author_overexposure_report([],missing_columns=missing,**kw)
    rows=_load_rows(conn,link_table,lc,source_table,sc)
    return build_knowledge_source_author_overexposure_report(rows,**kw)

def format_knowledge_source_author_overexposure_json(r): return json_dumps(r)
def format_knowledge_source_author_overexposure_text(r):
    s=r["summary"]; lines=["Knowledge Source Author Overexposure",f"Generated: {r['generated_at']}",f"Totals: links={s['link_count']} authors={s['author_count']} findings={s['finding_count']} shown={s['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines.extend(["","author | share | links | sources | content"])
    for f in r["findings"]: lines.append(f"{f['author']} | {f['author_share']:.4f} | {f['link_count']} | {f['source_count']} | {f['content_count']}")
    return "\n".join(lines)

def _load_rows(conn,lt,lc,st,sc):
    author=pick(sc,"author","source_author","byline",out="author"); metadata=pick(sc,"metadata","source_metadata",out="metadata")
    linked_at=pick(lc,"linked_at","created_at","updated_at",out="linked_at")
    sql=f"SELECT l.content_id AS content_id, l.source_id AS source_id, {linked_at}, {author}, {metadata} FROM {lt} l LEFT JOIN {st} s ON s.id=l.source_id ORDER BY l.rowid"
    return [dict(r) for r in conn.execute(sql)]
def _author(row):
    direct=clean(row.get("author"))
    if direct: return direct
    try: meta=json.loads(clean(row.get("metadata"))) if clean(row.get("metadata")) else {}
    except json.JSONDecodeError: meta={}
    return clean(meta.get("author") or meta.get("byline"),UNKNOWN_AUTHOR)
