"""Audit blog draft heading depth for skipped and over-deep levels."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_outline_depth_variance"; DEFAULT_MAX_DEPTH=4; DEFAULT_LIMIT=100; HEADING_RE=re.compile(r"^(#{1,6})\s+(.+?)\s*$",re.M)
def build_blog_draft_outline_depth_variance_report(rows:list[dict[str,Any]],*,max_depth:int=DEFAULT_MAX_DEPTH,draft_id=None,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("max_depth",max_depth); positive("limit",limit); gen=now_value(now); findings=[]
    for r in rows:
        did=r.get("draft_id") or r.get("id")
        if draft_id is not None and str(did)!=str(draft_id): continue
        prev=None
        for m in HEADING_RE.finditer(clean(r.get("body") or r.get("content") or r.get("markdown"))):
            lvl=len(m.group(1)); text=m.group(2).strip(); reason=None
            if lvl>max_depth: reason="heading_deeper_than_max_depth"
            elif prev and lvl>prev+1: reason="skipped_heading_level"
            elif prev and abs(lvl-prev)>2: reason="sharp_depth_variance"
            if reason: findings.append({"draft_id":did,"heading_text":text,"heading_level":lvl,"previous_level":prev,"gap_reason":reason})
            prev=lvl
    findings.sort(key=lambda f:(_sid(f["draft_id"]),f["heading_level"],f["heading_text"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_depth":max_depth,"draft_id":draft_id,"limit":limit},"summary":{"draft_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No blog draft outline depth variance found.",schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_outline_depth_variance_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]
    if "blog_drafts" not in s: mt.append("blog_drafts")
    else:
        c=s["blog_drafts"]; miss=[]
        if "id" not in c: miss.append("id")
        if not {"body","content","markdown"}&c: miss.append("body|content|markdown")
        if miss: mc["blog_drafts"]=miss
        else: rows=load_table(conn,"blog_drafts",c,{"draft_id":("id","draft_id"),"body":("body","content","markdown")})
    return build_blog_draft_outline_depth_variance_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_blog_draft_outline_depth_variance_json(r): return json_dumps(r)
def format_blog_draft_outline_depth_variance_text(r):
    lines=["Blog Draft Outline Depth Variance",f"Generated: {r['generated_at']}",f"Totals: drafts={r['summary']['draft_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - draft={f['draft_id']} h{f['heading_level']} prev={f['previous_level']} reason={f['gap_reason']} {f['heading_text']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
