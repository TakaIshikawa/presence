from __future__ import annotations
from datetime import datetime,timedelta,timezone
import json,re,sqlite3
from typing import Any
ARTIFACT_TYPE="reply_draft_sentiment_mismatch"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_SEVERITY=1; DEFAULT_LIMIT=100
SCORES={"negative":-1,"angry":-1,"frustrated":-1,"positive":1,"upbeat":1,"happy":1,"neutral":0,"cold":-0.5,"dismissive":-1,"defensive":-1}
def infer_sentiment(text):
    s=_clean(text).lower()
    if any(w in s for w in ("angry","awful","terrible","broken","upset","frustrated")): return "negative"
    if any(w in s for w in ("great","happy","thanks","excited","love")): return "positive"
    return "neutral"
def build_reply_draft_sentiment_mismatch_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,status:list[str]|tuple[str,...]|str|None=None,min_severity:int=DEFAULT_MIN_SEVERITY,platform:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_severity<0 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); statuses=_statuses(status); findings=[]
    for r in rows:
        if statuses and _clean(r.get("status")).lower() not in statuses: continue
        if platform and _clean(r.get("platform"))!=platform: continue
        ts=_dt(r.get("created_at") or r.get("updated_at"))
        if ts and ts<cutoff: continue
        inbound=_clean(r.get("inbound_sentiment") or r.get("mention_sentiment")) or infer_sentiment(r.get("inbound_text") or r.get("mention_text"))
        draft=_clean(r.get("draft_sentiment") or r.get("reply_sentiment")) or infer_sentiment(r.get("draft_text") or r.get("body"))
        mismatch,severity=_mismatch(inbound,draft)
        if severity>=min_severity and mismatch:
            findings.append({"reply_id":r.get("reply_id") or r.get("id"),"mention_id":r.get("mention_id"),"inbound_sentiment":inbound,"draft_sentiment":draft,"mismatch_type":mismatch,"severity":severity,"draft_excerpt":_clean(r.get("draft_text") or r.get("body"))[:160],"created_at":_clean(r.get("created_at") or r.get("updated_at"))})
    findings.sort(key=lambda f:(-f["severity"],str(f["reply_id"]))); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"status":sorted(statuses),"min_severity":min_severity,"platform":platform,"limit":limit},"summary":{"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No reply draft sentiment mismatches found." if not findings else None}}
def build_reply_draft_sentiment_mismatch_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t=next((x for x in ("reply_drafts","reply_queue") if x in s),None)
    if not t: return build_reply_draft_sentiment_mismatch_report([],missing_tables=["reply_drafts|reply_queue"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','reply_id',fallback='rowid')} AS id,{_pick(cols,'reply_id','id',fallback='NULL')} AS reply_id,{_pick(cols,'mention_id','target_id',fallback='NULL')} AS mention_id,{_pick(cols,'platform',fallback='NULL')} AS platform,{_pick(cols,'status',fallback='NULL')} AS status,{_pick(cols,'inbound_sentiment','mention_sentiment',fallback='NULL')} AS inbound_sentiment,{_pick(cols,'draft_sentiment','reply_sentiment',fallback='NULL')} AS draft_sentiment,{_pick(cols,'inbound_text','mention_text','source_text',fallback='NULL')} AS inbound_text,{_pick(cols,'draft_text','body','reply_text',fallback='NULL')} AS draft_text,{_pick(cols,'created_at','updated_at',fallback='NULL')} AS created_at FROM {t}")]
    return build_reply_draft_sentiment_mismatch_report(rows,**kw)
def _mismatch(inb,draft):
    i=SCORES.get(_clean(inb).lower(),0); d=SCORES.get(_clean(draft).lower(),0)
    if i<0 and d>0: return "inappropriately_upbeat",3
    if i<0 and d<=-0.5: return "defensive_or_dismissive",2
    if i>0 and d<0: return "cold_or_dismissive",2
    return "",0
def format_reply_draft_sentiment_mismatch_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_reply_draft_sentiment_mismatch_text(r): return "\n".join(["Reply Draft Sentiment Mismatch"]+[f"- {f['reply_id']} {f['mismatch_type']} severity={f['severity']}" for f in r.get("findings",[])] or ["No findings."])
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
