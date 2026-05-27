"""Find fenced code blocks in blog drafts with missing or invalid language labels."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="blog_draft_code_block_language_gaps"; DEFAULT_ALLOWED_LANGUAGES="python,javascript,typescript,bash,sh,json,yaml,html,css,sql,markdown,go,rust"
DEFAULT_LIMIT=100; FENCE_RE=re.compile(r"```([^\n`]*)\n.*?```",re.S)
def build_blog_draft_code_block_language_gaps_report(rows:list[dict[str,Any]],*,allowed_languages:str|list[str]=DEFAULT_ALLOWED_LANGUAGES,draft_id=None,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("limit",limit); allowed={lower(x) for x in (allowed_languages if isinstance(allowed_languages,list) else clean(allowed_languages).split(",")) if lower(x)}
    gen=now_value(now); findings=[]
    for r in rows:
        did=r.get("draft_id") or r.get("id")
        if draft_id is not None and str(did)!=str(draft_id): continue
        body=clean(r.get("body") or r.get("content") or r.get("markdown")); hint=lower(_meta(r).get("language") or _meta(r).get("primary_language"))
        for idx,m in enumerate(FENCE_RE.finditer(body),1):
            lang=lower(m.group(1).split()[0] if m.group(1).strip() else "")
            reason=None
            if not lang: reason="missing_language"
            elif allowed and lang not in allowed: reason="unknown_language"
            elif hint and lang!=hint: reason="metadata_language_mismatch"
            if reason: findings.append({"draft_id":did,"block_index":idx,"language":lang or None,"gap_reason":reason})
    findings.sort(key=lambda f:(_sid(f["draft_id"]),f["block_index"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"allowed_languages":sorted(allowed),"draft_id":draft_id,"limit":limit},"summary":{"draft_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No blog draft code block language gaps found.",schema_gap=bool(missing_tables or missing_columns))}
def build_blog_draft_code_block_language_gaps_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]
    if "blog_drafts" not in s: mt.append("blog_drafts")
    else:
        c=s["blog_drafts"]; miss=[]
        if "id" not in c: miss.append("id")
        if not {"body","content","markdown"}&c: miss.append("body|content|markdown")
        if miss: mc["blog_drafts"]=miss
        else: rows=load_table(conn,"blog_drafts",c,{"draft_id":("id","draft_id"),"body":("body","content","markdown"),"metadata":("metadata","frontmatter")})
    return build_blog_draft_code_block_language_gaps_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def _meta(r):
    try:return json.loads(clean(r.get("metadata"))) if clean(r.get("metadata")) else {}
    except json.JSONDecodeError:return {}
def format_blog_draft_code_block_language_gaps_json(r): return json_dumps(r)
def format_blog_draft_code_block_language_gaps_text(r):
    lines=["Blog Draft Code Block Language Gaps",f"Generated: {r['generated_at']}",f"Totals: drafts={r['summary']['draft_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - draft={f['draft_id']} block={f['block_index']} language={f['language']} reason={f['gap_reason']}")
    return "\n".join(lines)
def _sid(v):
    try:return(0,int(v))
    except(TypeError,ValueError):return(1,clean(v))
