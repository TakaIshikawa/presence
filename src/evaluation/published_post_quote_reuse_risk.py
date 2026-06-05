from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import json,re,sqlite3
from typing import Any
ARTIFACT_TYPE="published_post_quote_reuse_risk"; DEFAULT_LOOKBACK_DAYS=90; DEFAULT_MIN_PHRASE_WORDS=3; DEFAULT_MIN_REUSE_COUNT=2; DEFAULT_LIMIT=100
QUOTE_RE=re.compile(r"[\"“”']([^\"“”']{8,})[\"“”']")
def extract_quoted_phrases(text,min_words=DEFAULT_MIN_PHRASE_WORDS):
    out=[]
    for m in QUOTE_RE.finditer(_clean(text)):
        p=re.sub(r"\s+"," ",m.group(1)).strip()
        if len(re.findall(r"[A-Za-z0-9]+",p))>=min_words: out.append(p)
    return out
def normalize_phrase(p): return re.sub(r"\W+"," ",_clean(p).lower()).strip()
def build_published_post_quote_reuse_risk_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_phrase_words:int=DEFAULT_MIN_PHRASE_WORDS,min_reuse_count:int=DEFAULT_MIN_REUSE_COUNT,platform:str|None=None,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if lookback_days<=0 or min_phrase_words<=0 or min_reuse_count<=0 or limit<=0: raise ValueError("invalid filter")
    gen=_utc(now or datetime.now(timezone.utc)); cutoff=gen-timedelta(days=lookback_days); groups=defaultdict(list)
    for r in rows:
        if platform and _clean(r.get("platform"))!=platform: continue
        ts=_dt(r.get("published_at") or r.get("created_at"))
        if ts and ts<cutoff: continue
        for p in extract_quoted_phrases(_body(r),min_phrase_words): groups[normalize_phrase(p)].append((p,r))
    findings=[]
    for norm,items in groups.items():
        posts={i[1].get("post_id") or i[1].get("id") for i in items}
        if len(posts)>=min_reuse_count:
            findings.append({"phrase":items[0][0],"normalized_phrase":norm,"reuse_count":len(posts),"post_ids":sorted(posts),"platforms":sorted({_clean(i[1].get("platform")) for i in items}),"newest_published_at":max(_clean(i[1].get("published_at") or i[1].get("created_at")) for i in items),"sample_contexts":[_body(i[1])[:120] for i in items[:3]]})
    findings.sort(key=lambda f:(-f["reuse_count"],f["normalized_phrase"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_phrase_words":min_phrase_words,"min_reuse_count":min_reuse_count,"platform":platform,"limit":limit},"summary":{"phrase_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No published post quote reuse risks found." if not findings else None}}
def build_published_post_quote_reuse_risk_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t=next((x for x in ("published_posts","content_publications","generated_content") if x in s),None)
    if not t: return build_published_post_quote_reuse_risk_report([],missing_tables=["published_posts|content_publications|generated_content"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','post_id',fallback='rowid')} AS id,{_pick(cols,'post_id','id',fallback='NULL')} AS post_id,{_pick(cols,'platform',fallback='NULL')} AS platform,{_pick(cols,'body','content','text',fallback='NULL')} AS body,{_pick(cols,'published_at','created_at','updated_at',fallback='NULL')} AS published_at FROM {t}")]
    return build_published_post_quote_reuse_risk_report(rows,**kw)
def format_published_post_quote_reuse_risk_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_published_post_quote_reuse_risk_text(r): return "\n".join(["Published Post Quote Reuse Risk"]+[f"- {f['normalized_phrase']} reuse={f['reuse_count']}" for f in r.get("findings",[])] or ["No findings."])
def _body(r): return " ".join(_clean(r.get(k)) for k in ("body","content","text"))
def _clean(v): return "" if v is None else str(v).strip()
def _utc(v): return v.replace(tzinfo=timezone.utc) if v.tzinfo is None else v.astimezone(timezone.utc)
def _dt(v):
    try: return _utc(datetime.fromisoformat(_clean(v).replace("Z","+00:00")))
    except ValueError: return None
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
