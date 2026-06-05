from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json,re,sqlite3
from typing import Any
ARTIFACT_TYPE="generated_content_first_person_density"; DEFAULT_MIN_DENSITY=0.04; DEFAULT_MIN_WORDS=20; DEFAULT_LOOKBACK_DAYS=90; DEFAULT_LIMIT=100
TERMS=("i","me","my","mine","we","us","our","ours")
def build_generated_content_first_person_density_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,content_type:str|None=None,min_density:float=DEFAULT_MIN_DENSITY,min_words:int=DEFAULT_MIN_WORDS,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_density<0 or min_words<=0 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); findings=[]; inspected=0
    for r in rows:
        ctype=_clean(r.get("content_type") or r.get("type") or r.get("channel"))
        if content_type and ctype!=content_type: continue
        created=_dt(r.get("created_at") or r.get("updated_at"))
        if created and created<cutoff: continue
        text=strip_quotes_and_code(_body(r)); words=re.findall(r"[A-Za-z][A-Za-z']*",text.lower())
        if len(words)<min_words: continue
        inspected+=1; matches=[w for w in words if w in TERMS]; density=round(len(matches)/len(words),4)
        if density>=min_density:
            findings.append({"content_id":r.get("content_id") or r.get("id"),"content_type":ctype,"status":_clean(r.get("status")),"first_person_count":len(matches),"word_count":len(words),"density":density,"sample_terms":sorted(set(matches))[:8],"created_at":_clean(r.get("created_at") or r.get("updated_at"))})
    findings.sort(key=lambda f:(-f["density"],-f["first_person_count"],str(f["content_id"]))); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"content_type":content_type,"min_density":min_density,"min_words":min_words,"limit":limit},"summary":{"content_count":inspected,"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No generated content first-person density issues found." if not findings else None}}
def build_generated_content_first_person_density_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); rows=[]; mt=[]
    for t in ("generated_content","blog_drafts"):
        if t in s:
            cols=s[t]; type_fallback="'blog_draft'" if t=="blog_drafts" else "NULL"
            rows += [dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','content_id',fallback='rowid')} AS id,{_pick(cols,'content_id','id',fallback='NULL')} AS content_id,{_pick(cols,'content_type','type',fallback=type_fallback)} AS content_type,{_pick(cols,'status',fallback='NULL')} AS status,{_pick(cols,'body','content','text','markdown',fallback='NULL')} AS body,{_pick(cols,'created_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    if not rows and "generated_content" not in s and "blog_drafts" not in s: mt=["generated_content|blog_drafts"]
    return build_generated_content_first_person_density_report(rows,missing_tables=mt,**kw)
def strip_quotes_and_code(text:Any)->str:
    raw=re.sub(r"```.*?```"," ",_clean(text),flags=re.S)
    return "\n".join(line for line in raw.splitlines() if not line.lstrip().startswith(">"))
def format_generated_content_first_person_density_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_generated_content_first_person_density_text(r): return "\n".join(["Generated Content First Person Density"]+[f"- {f['content_id']} {f['density']} {','.join(f['sample_terms'])}" for f in r.get("findings",[])] or ["No findings."])
def _body(r): return "\n".join(_clean(r.get(k)) for k in ("body","content","text","markdown"))
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
