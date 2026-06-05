from __future__ import annotations
from datetime import datetime, timezone
import json, re, sqlite3
from typing import Any
ARTIFACT_TYPE="blog_draft_heading_structure_gaps"; DEFAULT_MAX_HEADING_LENGTH=80; DEFAULT_LIMIT=100
_MD=re.compile(r"^(#{1,6})\s+(.+?)\s*$",re.M); _HTML=re.compile(r"<h([1-6])[^>]*>(.*?)</h\1>",re.I|re.S)
def build_blog_draft_heading_structure_gaps_report(rows:list[dict[str,Any]],*,max_heading_length:int=DEFAULT_MAX_HEADING_LENGTH,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if max_heading_length<=0 or limit<=0: raise ValueError("numeric filters must be positive")
    findings=[]
    for r in rows:
        hs=extract_headings(_body(r)); h1=[h for h in hs if h["level"]==1]
        base={"draft_id":r.get("draft_id") or r.get("id"),"title":_clean(r.get("title") or r.get("slug"))}
        if not h1: findings.append({**base,"offending_heading":"","reason":"missing_h1","heading_level":None,"severity":"high"})
        if len(h1)>1:
            for h in h1[1:]: findings.append({**base,"offending_heading":h["text"],"reason":"multiple_h1","heading_level":1,"severity":"high"})
        prev=0
        for h in hs:
            if prev and h["level"]>prev+1: findings.append({**base,"offending_heading":h["text"],"reason":"skipped_heading_level","heading_level":h["level"],"severity":"medium"})
            if len(h["text"])>max_heading_length: findings.append({**base,"offending_heading":h["text"],"reason":"long_heading","heading_level":h["level"],"severity":"low"})
            prev=h["level"]
    findings.sort(key=lambda f:({"high":0,"medium":1,"low":2}[f["severity"]],str(f["draft_id"]),f["reason"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":_iso(now),"filters":{"max_heading_length":max_heading_length,"limit":limit},"summary":{"draft_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No blog draft heading structure gaps found." if not findings else None}}
def build_blog_draft_heading_structure_gaps_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t="blog_drafts" if "blog_drafts" in s else ("generated_content" if "generated_content" in s else None)
    if not t: return build_blog_draft_heading_structure_gaps_report([],missing_tables=["blog_drafts|generated_content"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','draft_id',fallback='rowid')} AS id,{_pick(cols,'draft_id','id',fallback='NULL')} AS draft_id,{_pick(cols,'title','slug',fallback='NULL')} AS title,{_pick(cols,'body','content','markdown','html',fallback='NULL')} AS body FROM {t}")]
    return build_blog_draft_heading_structure_gaps_report(rows,**kw)
def extract_headings(text:Any)->list[dict[str,Any]]:
    raw=_clean(text); out=[]
    for m in _MD.finditer(raw): out.append({"level":len(m.group(1)),"text":_strip(m.group(2)),"pos":m.start()})
    for m in _HTML.finditer(raw): out.append({"level":int(m.group(1)),"text":_strip(re.sub(r"<[^>]+>","",m.group(2))),"pos":m.start()})
    return sorted(out,key=lambda h:h["pos"])
def format_blog_draft_heading_structure_gaps_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_blog_draft_heading_structure_gaps_text(r): return _text("Blog Draft Heading Structure Gaps",r)
def _body(r): return "\n".join(_clean(r.get(k)) for k in ("body","content","markdown","html"))
def _strip(s): return re.sub(r"\s+"," ",_clean(s).strip("# ")).strip()
def _clean(v): return "" if v is None else str(v).strip()
def _iso(n=None): return ((n or datetime.now(timezone.utc)).replace(tzinfo=timezone.utc) if (n or datetime.now(timezone.utc)).tzinfo is None else (n or datetime.now(timezone.utc)).astimezone(timezone.utc)).isoformat()
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
def _text(title,r):
    lines=[title,"Summary: "+", ".join(f"{k}={v}" for k,v in sorted(r.get("summary",{}).items()))]
    lines += [f"- {f.get('draft_id')} {f.get('reason')} h{f.get('heading_level')} {f.get('offending_heading')}" for f in r.get("findings",[])] or [r.get("empty_state",{}).get("message") or "No findings."]
    return "\n".join(lines)
