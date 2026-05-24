"""Find blog drafts with weak meta descriptions."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_meta_description_gaps"; DEFAULT_MIN_LENGTH=120; DEFAULT_MAX_LENGTH=160; DEFAULT_LIMIT=50
GENERIC={"learn more","read more","untitled","coming soon","lorem ipsum","description"}
def build_blog_draft_meta_description_gaps_report(rows:list[dict[str,Any]],*,min_length:int=DEFAULT_MIN_LENGTH,max_length:int=DEFAULT_MAX_LENGTH,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("min_length",min_length); positive("max_length",max_length); positive("limit",limit)
    descs=defaultdict(list); parsed=[]
    for r in rows:
        meta=clean(r.get("meta_description") or _extract_meta(r.get("metadata")) or _extract_frontmatter(r.get("body") or r.get("content")))
        item={**r,"meta_description":meta}; parsed.append(item)
        if meta: descs[meta.lower()].append(item)
    findings=[]
    for r in parsed:
        meta=r["meta_description"]; reasons=[]; dup=None
        if not meta: reasons.append("missing")
        if meta and len(meta)<min_length: reasons.append("too_short")
        if meta and len(meta)>max_length: reasons.append("too_long")
        if meta and any(g in meta.lower() for g in GENERIC): reasons.append("generic")
        if meta and len(descs[meta.lower()])>1: reasons.append("duplicate"); dup=next((x.get("draft_id") or x.get("id") for x in descs[meta.lower()] if (x.get("draft_id") or x.get("id"))!=(r.get("draft_id") or r.get("id"))),None)
        if reasons: findings.append({"draft_id":r.get("draft_id") or r.get("id"),"title":r.get("title"),"meta_description":meta,"length":len(meta),"gap_reasons":reasons,"duplicate_of":dup,"recommended_action":"write a unique concise meta description"})
    findings.sort(key=lambda f:(-len(f["gap_reasons"]),str(f["draft_id"])))
    shown=findings[:limit]; by=Counter(reason for f in findings for reason in f["gap_reasons"])
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"min_length":min_length,"max_length":max_length,"limit":limit},"summary":{"draft_count":len(rows),"gap_count":len(findings),"shown":len(shown),"by_reason":dict(sorted(by.items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No blog draft meta description gaps found.",schema_gap=bool(missing_tables or missing_columns))}
def _extract_meta(value):
    text=clean(value)
    if not text: return ""
    try:
        obj=json.loads(text); return clean(obj.get("meta_description") or obj.get("description"))
    except (TypeError,ValueError,AttributeError): return ""
def _extract_frontmatter(text):
    m=re.search(r"(?im)^meta_description:\s*(.+)$",clean(text))
    return clean(m.group(1).strip("'\"")) if m else ""
def build_blog_draft_meta_description_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]; table="blog_drafts" if "blog_drafts" in s else ("blog_posts" if "blog_posts" in s else None)
    if not table: mt.append("blog_drafts|blog_posts")
    else:
        c=s[table]
        if not ({"meta_description","metadata","body","content"} & c): mc[table]=["meta_description|metadata|body|content"]
        else: rows=load_table(conn,table,c,{"draft_id":("id","draft_id"),"title":("title","headline"),"meta_description":("meta_description","seo_description"),"metadata":("metadata","meta_json","frontmatter"),"body":("body","content")})
    return build_blog_draft_meta_description_gaps_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_blog_draft_meta_description_gaps_json(r): return json_dumps(r)
def format_blog_draft_meta_description_gaps_text(r):
    s=r["summary"]; lines=["Blog Draft Meta Description Gaps",f"Generated: {r['generated_at']}",f"Totals: drafts={s['draft_count']} gaps={s['gap_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","draft_id | length | reasons"]
    for f in r["findings"]: lines.append(f"{f['draft_id']} | {f['length']} | {', '.join(f['gap_reasons'])}")
    return "\n".join(lines)
