from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import json,sqlite3
from typing import Any
ARTIFACT_TYPE="newsletter_bounce_domain_clusters"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_COUNT=2; DEFAULT_MIN_BOUNCE_RATE=0.5; DEFAULT_LIMIT=100
BOUNCE={"bounce","bounced","delivery_failed","failed","complaint","dropped"}
def build_newsletter_bounce_domain_clusters_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_count:int=DEFAULT_MIN_COUNT,min_bounce_rate:float=DEFAULT_MIN_BOUNCE_RATE,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_count<=0 or not 0<=min_bounce_rate<=1 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); groups=defaultdict(lambda:{"total":0,"bounce":0,"newest":"","sample_event_ids":[]})
    for r in rows:
        ts=_dt(r.get("created_at") or r.get("event_at") or r.get("timestamp"))
        if ts and ts<cutoff: continue
        dom=extract_email_domain(r.get("recipient_email") or r.get("email"))
        if not dom: continue
        g=groups[dom]; g["total"]+=1
        if _clean(r.get("event_type") or r.get("type") or r.get("status")).lower() in BOUNCE: g["bounce"]+=1
        t=_clean(r.get("created_at") or r.get("event_at") or r.get("timestamp")); g["newest"]=max(g["newest"],t)
        if len(g["sample_event_ids"])<3: g["sample_event_ids"].append(r.get("event_id") or r.get("id"))
    findings=[]
    for dom,g in groups.items():
        rate=round(g["bounce"]/g["total"],4) if g["total"] else 0
        if g["bounce"]>=min_count and rate>=min_bounce_rate: findings.append({"domain":dom,"bounce_count":g["bounce"],"event_count":g["total"],"bounce_rate":rate,"newest_event_at":g["newest"],"sample_event_ids":g["sample_event_ids"],"severity":"high" if rate>=.8 else "medium"})
    findings.sort(key=lambda f:(-f["bounce_rate"],-f["bounce_count"],f["domain"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_count":min_count,"min_bounce_rate":min_bounce_rate,"limit":limit},"summary":{"domain_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No newsletter bounce domain clusters found." if not findings else None}}
def build_newsletter_bounce_domain_clusters_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); rows=[]; tables=[t for t in ("newsletter_events","buttondown_email_events","subscriber_events") if t in s]
    for t in tables:
        cols=s[t]; rows += [dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','event_id',fallback='rowid')} AS id,{_pick(cols,'event_id','id',fallback='NULL')} AS event_id,{_pick(cols,'recipient_email','email',fallback='NULL')} AS recipient_email,{_pick(cols,'event_type','type','status',fallback='NULL')} AS event_type,{_pick(cols,'created_at','event_at','timestamp',fallback='NULL')} AS created_at FROM {t}")]
    return build_newsletter_bounce_domain_clusters_report(rows,missing_tables=[] if tables else ["newsletter_events|buttondown_email_events|subscriber_events"],**kw)
def extract_email_domain(v):
    text=_clean(v).lower()
    if text.count("@")!=1: return ""
    local,dom=text.rsplit("@",1)
    return dom.strip(".") if local and "." in dom else ""
def format_newsletter_bounce_domain_clusters_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_newsletter_bounce_domain_clusters_text(r): return "\n".join(["Newsletter Bounce Domain Clusters"]+[f"- {f['domain']} {f['bounce_count']}/{f['event_count']} rate={f['bounce_rate']}" for f in r.get("findings",[])] or ["No findings."])
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
