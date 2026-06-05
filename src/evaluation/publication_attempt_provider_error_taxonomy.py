from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import json,sqlite3
from typing import Any
ARTIFACT_TYPE="publication_attempt_provider_error_taxonomy"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_COUNT=1; DEFAULT_LIMIT=100
FAMILIES={"auth":("auth","unauthorized","forbidden","token","credential"),"rate_limit":("rate limit","429","too many"),"timeout":("timeout","timed out","deadline"),"media":("media","image","video","attachment"),"payload":("payload","invalid","schema","too large","body"),"network":("network","dns","connection","ssl"),"duplicate":("duplicate","already exists","idempot")}
def classify_error_family(text):
    s=_clean(text).lower()
    for fam,keys in FAMILIES.items():
        if any(k in s for k in keys): return fam
    return "unknown"
def build_publication_attempt_provider_error_taxonomy_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_count:int=DEFAULT_MIN_COUNT,provider:str|None=None,platform:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_count<=0 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); groups=defaultdict(list)
    for r in rows:
        st=_clean(r.get("status") or r.get("state")).lower()
        if st and st not in {"failed","error","errored","failure"}: continue
        if provider and _clean(r.get("provider"))!=provider: continue
        if platform and _clean(r.get("platform"))!=platform: continue
        ts=_dt(r.get("created_at") or r.get("attempted_at"))
        if ts and ts<cutoff: continue
        fam=classify_error_family(r.get("error") or r.get("error_message") or r.get("response_body"))
        groups[(_clean(r.get("provider")),_clean(r.get("platform")),fam)].append(r)
    findings=[]
    for (prov,plat,fam),rs in groups.items():
        if len(rs)>=min_count: findings.append({"provider":prov,"platform":plat,"error_family":fam,"attempt_count":len(rs),"newest_attempt_at":max(_clean(r.get("created_at") or r.get("attempted_at")) for r in rs),"sample_error":_clean(rs[0].get("error") or rs[0].get("error_message") or rs[0].get("response_body"))[:200],"affected_content_count":len({r.get("content_id") or r.get("post_id") for r in rs})})
    findings.sort(key=lambda f:(-f["attempt_count"],f["provider"],f["platform"],f["error_family"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_count":min_count,"provider":provider,"platform":platform,"limit":limit},"summary":{"group_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No publication attempt provider error taxonomy findings." if not findings else None}}
def build_publication_attempt_provider_error_taxonomy_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t="publication_attempts" if "publication_attempts" in s else None
    if not t: return build_publication_attempt_provider_error_taxonomy_report([],missing_tables=["publication_attempts"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id',fallback='rowid')} AS id,{_pick(cols,'content_id','post_id','draft_id',fallback='NULL')} AS content_id,{_pick(cols,'provider',fallback='NULL')} AS provider,{_pick(cols,'platform',fallback='NULL')} AS platform,{_pick(cols,'status','state',fallback='NULL')} AS status,{_pick(cols,'error','error_message','response_body',fallback='NULL')} AS error,{_pick(cols,'created_at','attempted_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_publication_attempt_provider_error_taxonomy_report(rows,**kw)
def format_publication_attempt_provider_error_taxonomy_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_publication_attempt_provider_error_taxonomy_text(r): return "\n".join(["Publication Attempt Provider Error Taxonomy"]+[f"- {f['provider']}/{f['platform']} {f['error_family']} count={f['attempt_count']}" for f in r.get("findings",[])] or ["No findings."])
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
