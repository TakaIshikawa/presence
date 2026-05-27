"""Flag blog draft generated content with unusual estimated reading time."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_reading_time_outliers"; DEFAULT_DAYS=30; DEFAULT_LIMIT=50; DEFAULT_MIN_MINUTES=2.0; DEFAULT_MAX_MINUTES=12.0; WORDS_PER_MINUTE=200.0
def build_blog_draft_reading_time_outliers_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,limit:int=DEFAULT_LIMIT,min_minutes:float=DEFAULT_MIN_MINUTES,max_minutes:float=DEFAULT_MAX_MINUTES,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
    positive("days",days); positive("limit",limit); positive("min_minutes",min_minutes); positive("max_minutes",max_minutes)
    if min_minutes>=max_minutes: raise ValueError("min_minutes must be less than max_minutes")
    findings=[]; acceptable=0
    for r in rows:
        body=clean(r.get("body") or r.get("content") or r.get("draft_text") or r.get("text")); wc=len(re.findall(r"\b[\w'-]+\b",body)); minutes=round(wc/WORDS_PER_MINUTE,2) if wc else 0.0
        if minutes<min_minutes: reason="underlong"; severity=round(min_minutes-minutes,2)
        elif minutes>max_minutes: reason="overlong"; severity=round(minutes-max_minutes,2)
        else: acceptable+=1; continue
        findings.append({"draft_id":r.get("draft_id") or r.get("id"),"title":clean(r.get("title")) or None,"slug":clean(r.get("slug")) or None,"word_count":wc,"estimated_minutes":minutes,"reason":reason,"severity_minutes":severity})
    findings.sort(key=lambda f:(-f["severity_minutes"],f["reason"],str(f["draft_id"])))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"limit":limit,"min_minutes":min_minutes,"max_minutes":max_minutes},"totals":{"draft_count":len(rows),"acceptable_count":acceptable,"outlier_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No blog draft reading time outliers found.",schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_reading_time_outliers_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    days=kw.get("days",DEFAULT_DAYS); positive("days",days); conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}
    if "generated_content" not in s: return build_blog_draft_reading_time_outliers_report([],missing_tables=["generated_content"],**kw)
    c=s["generated_content"]; needed={"id","content_type"}; body_cols={"body","content","draft_text","text"}
    miss=sorted(x for x in needed if x not in c)
    if not (body_cols & c): miss.append("body|content|draft_text|text")
    if miss: return build_blog_draft_reading_time_outliers_report([],missing_columns={"generated_content":miss},**kw)
    where=["LOWER(content_type) IN ('blog_draft','blog_post','blog')"]; params=[]
    if "created_at" in c:
        cutoff=(now_value(kw.get("now"))-timedelta(days=days)).isoformat(); where.append("(created_at IS NULL OR created_at >= ?)"); params.append(cutoff)
    rows=[dict(r) for r in conn.execute(f"SELECT {pick(c,'id',out='draft_id')}, {pick(c,'title','headline',out='title')}, {pick(c,'slug',out='slug')}, {pick(c,'body','content','draft_text','text',out='body')} FROM generated_content WHERE {' AND '.join(where)} ORDER BY id",params)]
    return build_blog_draft_reading_time_outliers_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_blog_draft_reading_time_outliers_json(r:dict[str,Any])->str: return json_dumps(r)
def format_blog_draft_reading_time_outliers_text(r:dict[str,Any])->str:
    t=r["totals"]; lines=["Blog Draft Reading Time Outliers",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: drafts={t['draft_count']} acceptable={t['acceptable_count']} outliers={t['outlier_count']} shown={t['shown_count']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","draft_id | title/slug | words | minutes | reason"]
    for f in r["findings"]: lines.append(f"{f['draft_id']} | {f.get('title') or f.get('slug') or ''} | {f['word_count']} | {f['estimated_minutes']} | {f['reason']}")
    return "\n".join(lines)
