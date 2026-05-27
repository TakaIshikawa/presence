"""Audit newsletter images for dimensions, total weight, and count."""
from __future__ import annotations
from html.parser import HTMLParser
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE="newsletter_image_load_weight"; DEFAULT_MAX_IMAGES=10; DEFAULT_MAX_TOTAL_KB=800; DEFAULT_LIMIT=100

class _ImgParser(HTMLParser):
    def __init__(self): super().__init__(); self.images=[]
    def handle_starttag(self, tag, attrs):
        if tag.lower()=="img": self.images.append(dict(attrs))

def build_newsletter_image_load_weight_report(rows:list[dict[str,Any]],*,max_images:int=DEFAULT_MAX_IMAGES,max_total_kb:int=DEFAULT_MAX_TOTAL_KB,issue_id=None,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("max_images",max_images); positive("max_total_kb",max_total_kb); positive("limit",limit)
    gen=now_value(now); findings=[]
    for r in rows:
        iid=r.get("issue_id") or r.get("id")
        if issue_id is not None and str(iid)!=str(issue_id): continue
        imgs=_images(r); total=sum(to_float(i.get("estimated_kb") or i.get("size_kb"),0) for i in imgs)
        if len(imgs)>max_images:
            findings.append({"issue_id":iid,"image_url":None,"issue_type":"too_many_images","estimated_kb":round(total,2),"image_count":len(imgs),"gap_reason":"image_count_exceeds_limit"})
        if total>max_total_kb:
            findings.append({"issue_id":iid,"image_url":None,"issue_type":"total_image_weight_exceeded","estimated_kb":round(total,2),"image_count":len(imgs),"gap_reason":"total_kb_exceeds_limit"})
        for img in imgs:
            url=img.get("src") or img.get("url")
            kb=to_float(img.get("estimated_kb") or img.get("size_kb") or img.get("bytes"),0)
            if img.get("bytes") and not img.get("estimated_kb") and not img.get("size_kb"): kb=round(kb/1024,2)
            missing_dim=not (img.get("width") and img.get("height"))
            if missing_dim or kb>max_total_kb:
                findings.append({"issue_id":iid,"image_url":url,"issue_type":"missing_dimensions" if missing_dim else "oversized_image","estimated_kb":round(kb,2),"image_count":len(imgs),"gap_reason":"missing_width_or_height" if missing_dim else "image_kb_exceeds_limit"})
    findings.sort(key=lambda f:(_sid(f["issue_id"]),f["issue_type"],f["image_url"] or ""))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_images":max_images,"max_total_kb":max_total_kb,"issue_id":issue_id,"limit":limit},"summary":{"issue_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No newsletter image load weight issues found.",schema_gap=bool(missing_tables or missing_columns))}

def build_newsletter_image_load_weight_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="newsletter_issues" if "newsletter_issues" in s else ("newsletter_drafts" if "newsletter_drafts" in s else None); mt=[]; mc={}; rows=[]
    if not table: mt.append("newsletter_issues|newsletter_drafts")
    else:
        c=s[table]
        if "id" not in c: mc[table]=["id"]
        else: rows=load_table(conn,table,c,{"issue_id":("id","issue_id"),"html":("html","body_html","body","content"),"metadata":("metadata","image_metadata","render_metadata")})
    return build_newsletter_image_load_weight_report(rows,missing_tables=mt,missing_columns=mc,**kw)

def _images(row):
    p=_ImgParser(); p.feed(clean(row.get("html"))); imgs=[dict(i, estimated_kb=i.get("data-size-kb") or i.get("size_kb"), bytes=i.get("data-bytes") or i.get("bytes")) for i in p.images]
    meta=_json(row.get("metadata"))
    def walk(v):
        if isinstance(v,dict):
            if ("url" in v or "src" in v) and any(k in v for k in ("width","height","bytes","size_kb","estimated_kb")): imgs.append(v)
            for child in v.values(): walk(child)
        elif isinstance(v,list):
            for child in v:
                if isinstance(child, str): imgs.append({"url": child})
                else: walk(child)
    walk(meta); return imgs
def _json(v):
    try: return json.loads(clean(v)) if clean(v) else None
    except json.JSONDecodeError: return None
def format_newsletter_image_load_weight_json(r): return json_dumps(r)
def format_newsletter_image_load_weight_text(r): return _text("Newsletter Image Load Weight",r,"issue_count")
def _text(title,r,count_key):
    lines=[title,f"Generated: {r['generated_at']}",f"Totals: items={r['summary'][count_key]} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - issue={f['issue_id']} type={f['issue_type']} image={f['image_url']} kb={f['estimated_kb']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
