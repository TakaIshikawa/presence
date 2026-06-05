from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import json,sqlite3
from typing import Any
ARTIFACT_TYPE="publication_attempt_retry_loop_risk"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_FAILURES=3; DEFAULT_LIMIT=100
FAIL={"failed","error","errored","failure"}; SUCCESS={"success","succeeded","published","complete"}
def build_publication_attempt_retry_loop_risk_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_failures:int=DEFAULT_MIN_FAILURES,platform:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_failures<=0 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); groups=defaultdict(list)
    for r in rows:
        if platform and _clean(r.get("platform"))!=platform: continue
        ts=_dt(r.get("created_at") or r.get("attempted_at"))
        if ts and ts<cutoff: continue
        groups[(_clean(r.get("content_id") or r.get("post_id") or r.get("draft_id")),_clean(r.get("platform")))].append(r)
    findings=[]
    for (cid,plat),rs in groups.items():
        rs=sorted(rs,key=lambda r:_clean(r.get("created_at") or r.get("attempted_at")))
        last_success=max((_clean(r.get("created_at") or r.get("attempted_at")) for r in rs if _status(r) in SUCCESS),default="")
        failures=[r for r in rs if _status(r) in FAIL and _clean(r.get("created_at") or r.get("attempted_at"))>last_success]
        if len(failures)>=min_failures:
            findings.append({"content_id":cid,"platform":plat,"failed_attempt_count":len(failures),"first_attempt_at":_clean(failures[0].get("created_at") or failures[0].get("attempted_at")),"latest_attempt_at":_clean(failures[-1].get("created_at") or failures[-1].get("attempted_at")),"failure_reasons":sorted({_clean(f.get("error") or f.get("error_message")) for f in failures if _clean(f.get("error") or f.get("error_message"))})[:5],"providers":sorted({_clean(f.get("provider")) for f in failures if _clean(f.get("provider"))}),"severity":"high" if len(failures)>=5 else "medium"})
    findings.sort(key=lambda f:(-f["failed_attempt_count"],f["content_id"],f["platform"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_failures":min_failures,"platform":platform,"limit":limit},"summary":{"group_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No publication attempt retry loop risks found." if not findings else None}}
def build_publication_attempt_retry_loop_risk_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t="publication_attempts" if "publication_attempts" in s else None
    if not t: return build_publication_attempt_retry_loop_risk_report([],missing_tables=["publication_attempts"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id',fallback='rowid')} AS id,{_pick(cols,'content_id','post_id','draft_id',fallback='NULL')} AS content_id,{_pick(cols,'platform',fallback='NULL')} AS platform,{_pick(cols,'provider',fallback='NULL')} AS provider,{_pick(cols,'status','state',fallback='NULL')} AS status,{_pick(cols,'error','error_message',fallback='NULL')} AS error,{_pick(cols,'created_at','attempted_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_publication_attempt_retry_loop_risk_report(rows,**kw)
def format_publication_attempt_retry_loop_risk_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_publication_attempt_retry_loop_risk_text(r): return "\n".join(["Publication Attempt Retry Loop Risk"]+[f"- {f['content_id']} {f['platform']} failures={f['failed_attempt_count']}" for f in r.get("findings",[])] or ["No findings."])
def _status(r): return _clean(r.get("status") or r.get("state")).lower()
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
