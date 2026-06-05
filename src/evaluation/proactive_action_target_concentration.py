from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timedelta,timezone
import json,re,sqlite3
from typing import Any
from urllib.parse import urlparse
ARTIFACT_TYPE="proactive_action_target_concentration"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_COUNT=3; DEFAULT_MIN_SHARE=0.5; DEFAULT_LIMIT=100
def build_proactive_action_target_concentration_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,target_type:str|None=None,min_count:int=DEFAULT_MIN_COUNT,min_share:float=DEFAULT_MIN_SHARE,status:list[str]|tuple[str,...]|str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_count<=0 or not 0<=min_share<=1 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); statuses=_statuses(status); items=[]
    for r in rows:
        if statuses and _clean(r.get("status")).lower() not in statuses: continue
        ts=_dt(r.get("created_at") or r.get("updated_at") or r.get("action_at"))
        if ts and ts<cutoff: continue
        for typ,key in _targets(r):
            if target_type and typ!=target_type: continue
            items.append((typ,key,r))
    total=len(items); groups=defaultdict(list)
    for typ,key,r in items: groups[(typ,key)].append(r)
    findings=[]
    for (typ,key),rs in groups.items():
        share=round(len(rs)/total,4) if total else 0
        if len(rs)>=min_count and share>=min_share:
            newest=max(_clean(r.get("created_at") or r.get("updated_at") or r.get("action_at")) for r in rs)
            findings.append({"target_key":key,"target_type":typ,"action_count":len(rs),"share_of_actions":share,"newest_action_at":newest,"sample_action_ids":[r.get("action_id") or r.get("id") for r in rs[:3]],"severity":"high" if share>=.75 else "medium"})
    findings.sort(key=lambda f:(-f["share_of_actions"],-f["action_count"],f["target_type"],f["target_key"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"target_type":target_type,"min_count":min_count,"min_share":min_share,"status":sorted(statuses),"limit":limit},"summary":{"action_count":total,"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No proactive action target concentration found." if not findings else None}}
def build_proactive_action_target_concentration_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t=next((x for x in ("proactive_actions","proactive_action_queue","actions") if x in s),None)
    if not t: return build_proactive_action_target_concentration_report([],missing_tables=["proactive_actions|proactive_action_queue"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','action_id',fallback='rowid')} AS id,{_pick(cols,'action_id','id',fallback='NULL')} AS action_id,{_pick(cols,'target_handle','handle','screen_name',fallback='NULL')} AS target_handle,{_pick(cols,'target_url','url','profile_url',fallback='NULL')} AS target_url,{_pick(cols,'relationship_id','relationship',fallback='NULL')} AS relationship_id,{_pick(cols,'status',fallback='NULL')} AS status,{_pick(cols,'created_at','updated_at','action_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_proactive_action_target_concentration_report(rows,**kw)
def normalize_handle(v):
    h=_clean(v).lower().lstrip("@"); return re.sub(r"[^a-z0-9_]+","",h)
def extract_domain(v):
    p=urlparse(_clean(v).lower() if "://" in _clean(v) else "//"+_clean(v).lower()); host=(p.hostname or "").strip("."); return host[4:] if host.startswith("www.") else host
def _targets(r):
    out=[]; h=normalize_handle(r.get("target_handle") or r.get("handle")); d=extract_domain(r.get("target_url") or r.get("url")); rel=_clean(r.get("relationship_id") or r.get("relationship"))
    if h: out.append(("handle",h))
    if d: out.append(("domain",d))
    if rel: out.append(("relationship",rel))
    return out
def format_proactive_action_target_concentration_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_proactive_action_target_concentration_text(r): return "\n".join(["Proactive Action Target Concentration"]+[f"- {f['target_type']}:{f['target_key']} {f['share_of_actions']}" for f in r.get("findings",[])] or ["No findings."])
def _statuses(v):
    if v is None: return set()
    if isinstance(v,str): v=(v,)
    return {_clean(x).lower() for x in v if _clean(x)}
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
