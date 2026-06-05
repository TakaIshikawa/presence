from __future__ import annotations
from datetime import datetime,timezone
import json,re,sqlite3
from typing import Any
ARTIFACT_TYPE="reply_draft_excessive_apology_report"; DEFAULT_MIN_APOLOGY_COUNT=2; DEFAULT_LIMIT=100
PHRASES=("sorry","apologies","my bad","forgive me","i apologize","we apologize")
def count_apologies(text):
    s=_clean(text).lower(); found=[]
    for p in PHRASES:
        found += [p]*len(re.findall(r"\b"+re.escape(p)+r"\b",s))
    return found
def strip_quoted_or_source_text(text):
    raw=re.sub(r"```.*?```"," ",_clean(text),flags=re.S)
    return "\n".join(l for l in raw.splitlines() if not l.lstrip().startswith(">"))
def build_reply_draft_excessive_apology_report(rows:list[dict[str,Any]],*,min_apology_count:int=DEFAULT_MIN_APOLOGY_COUNT,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if min_apology_count<=0 or limit<=0: raise ValueError("numeric filters must be positive")
    findings=[]
    for r in rows:
        text=strip_quoted_or_source_text(r.get("draft_text") or r.get("body") or r.get("reply_text"))
        source=_clean(r.get("source_text") or r.get("inbound_text") or r.get("mention_text"))
        if source: text=text.replace(source," ")
        matches=count_apologies(text)
        if len(matches)>=min_apology_count:
            findings.append({"reply_id":r.get("reply_id") or r.get("id"),"target_id":r.get("target_id") or r.get("mention_id"),"mention_id":r.get("mention_id") or r.get("target_id"),"apology_count":len(matches),"matched_phrases":sorted(set(matches)),"excerpt":text[:160],"severity":"high" if len(matches)>=4 else "medium","created_at":_clean(r.get("created_at") or r.get("updated_at"))})
    findings.sort(key=lambda f:(-f["apology_count"],str(f["reply_id"]))); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":_iso(now),"filters":{"min_apology_count":min_apology_count,"limit":limit},"summary":{"reply_count":len(rows),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No reply draft excessive apology findings." if not findings else None}}
def build_reply_draft_excessive_apology_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t=next((x for x in ("reply_drafts","reply_queue") if x in s),None)
    if not t: return build_reply_draft_excessive_apology_report([],missing_tables=["reply_drafts|reply_queue"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','reply_id',fallback='rowid')} AS id,{_pick(cols,'reply_id','id',fallback='NULL')} AS reply_id,{_pick(cols,'target_id','mention_id',fallback='NULL')} AS target_id,{_pick(cols,'mention_id','target_id',fallback='NULL')} AS mention_id,{_pick(cols,'draft_text','body','reply_text',fallback='NULL')} AS draft_text,{_pick(cols,'source_text','inbound_text','mention_text',fallback='NULL')} AS source_text,{_pick(cols,'created_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_reply_draft_excessive_apology_report(rows,**kw)
def format_reply_draft_excessive_apology_report_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_reply_draft_excessive_apology_report_text(r): return "\n".join(["Reply Draft Excessive Apology"]+[f"- {f['reply_id']} apologies={f['apology_count']} severity={f['severity']}" for f in r.get("findings",[])] or ["No findings."])
def _clean(v): return "" if v is None else str(v).strip()
def _iso(n=None): return ((n or datetime.now(timezone.utc)).replace(tzinfo=timezone.utc) if (n or datetime.now(timezone.utc)).tzinfo is None else (n or datetime.now(timezone.utc)).astimezone(timezone.utc)).isoformat()
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
