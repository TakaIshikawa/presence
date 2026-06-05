from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json,re,sqlite3
from typing import Any
ARTIFACT_TYPE="generated_content_source_attribution_gaps"; DEFAULT_LOOKBACK_DAYS=90; DEFAULT_LIMIT=100
def build_generated_content_source_attribution_gaps_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,content_type:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or limit<=0: raise ValueError("numeric filters must be positive")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); findings=[]; inspected=0
    for r in rows:
        ctype=_clean(r.get("content_type") or r.get("channel") or r.get("type"))
        if content_type and ctype!=content_type: continue
        created=_dt(r.get("created_at") or r.get("updated_at"))
        if created and created<cutoff: continue
        sources=_sources(r)
        if not sources: continue
        inspected+=1
        if not _has_attr(r,sources):
            findings.append({"content_id":r.get("content_id") or r.get("id"),"channel":ctype,"content_type":ctype,"source_count":len(sources),"missing_attribution_reasons":["source_metadata_without_visible_attribution"],"source_samples":sources[:3],"created_at":_clean(r.get("created_at") or r.get("updated_at"))})
    findings.sort(key=lambda f:(-f["source_count"],str(f["content_id"]))); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"content_type":content_type,"limit":limit},"summary":{"source_backed_count":inspected,"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No generated content source attribution gaps found." if not findings else None}}
def build_generated_content_source_attribution_gaps_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t="generated_content" if "generated_content" in s else None
    if not t: return build_generated_content_source_attribution_gaps_report([],missing_tables=["generated_content"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','content_id',fallback='rowid')} AS id,{_pick(cols,'content_id','id',fallback='NULL')} AS content_id,{_pick(cols,'content_type','channel','type',fallback='NULL')} AS content_type,{_pick(cols,'body','content','text',fallback='NULL')} AS body,{_pick(cols,'metadata','source_urls','evidence','source_ids',fallback='NULL')} AS metadata,{_pick(cols,'created_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_generated_content_source_attribution_gaps_report(rows,**kw)
def _sources(r):
    blob=" ".join(_clean(r.get(k)) for k in ("source_ids","source_urls","evidence","sources","metadata"))
    urls=re.findall(r"https?://[^\s,;\]\)\"']+",blob); ids=re.findall(r"\b(?:source|knowledge)[-_ ]?id['\"]?\s*[:=]\s*['\"]?([A-Za-z0-9_-]+)",blob,re.I)
    if _clean(r.get("source_id")): ids.append(_clean(r.get("source_id")))
    return sorted(set(urls+ids))
def _has_attr(r,sources):
    visible=(" ".join(_clean(r.get(k)) for k in ("body","content","text","attribution","citation","citations"))).lower()
    if any(w in visible for w in ("source:","sources:","according to","via ","citation","references")): return True
    return any(s.lower() in visible and s.startswith("http") for s in sources)
def format_generated_content_source_attribution_gaps_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_generated_content_source_attribution_gaps_text(r): return "\n".join(["Generated Content Source Attribution Gaps"]+[f"- {f['content_id']} sources={f['source_count']}" for f in r.get("findings",[])] or ["No findings."])
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
