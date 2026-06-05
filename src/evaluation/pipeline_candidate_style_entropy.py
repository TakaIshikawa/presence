from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone
import json,math,re,sqlite3
from typing import Any
ARTIFACT_TYPE="pipeline_candidate_style_entropy"; DEFAULT_MIN_ENTROPY=0.5; DEFAULT_MIN_UNIQUE_FORMATS=2; DEFAULT_LIMIT=100
def build_pipeline_candidate_style_entropy_report(rows:list[dict[str,Any]],*,min_entropy:float=DEFAULT_MIN_ENTROPY,min_unique_formats:int=DEFAULT_MIN_UNIQUE_FORMATS,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables=None,missing_columns=None):
    if min_entropy<0 or min_unique_formats<=0 or limit<=0: raise ValueError("invalid filter")
    groups=defaultdict(list)
    for r in rows: groups[_key(r)].append(r)
    findings=[]
    for key,items in groups.items():
        if len(items)<2: continue
        formats=[_fmt(i) for i in items]; openings=[_opening(_text(i)) for i in items]; verbs=[_verb(_text(i)) for i in items]
        unique=len(set(formats)); repeated=sum(c-1 for c in Counter(v for v in verbs if v).values() if c>1)
        entropy=round((_entropy(formats)+_entropy(openings)+_entropy(verbs))/3,4)
        reasons=[]
        if entropy<min_entropy: reasons.append("low_style_entropy")
        if unique<min_unique_formats: reasons.append("low_unique_formats")
        if repeated: reasons.append("repeated_hook_verbs")
        if reasons: findings.append({"run_id":key[0],"pipeline_run_id":key[0],"content_id":key[1],"candidate_count":len(items),"unique_format_count":unique,"repeated_hook_count":repeated,"entropy_score":entropy,"risk_reasons":reasons})
    findings.sort(key=lambda f:(f["entropy_score"],f["unique_format_count"],str(f["run_id"]))); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":_iso(now),"filters":{"min_entropy":min_entropy,"min_unique_formats":min_unique_formats,"limit":limit},"summary":{"group_count":len(groups),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":missing_tables or [],"missing_columns":missing_columns or {},"empty_state":{"is_empty":not findings,"message":"No pipeline candidate style entropy risks found." if not findings else None}}
def build_pipeline_candidate_style_entropy_report_from_db(db_or_conn:Any,**kw):
    c=_conn(db_or_conn); s=_schema(c); t=next((x for x in ("pipeline_candidates","content_candidates","candidate_generations") if x in s),None)
    if not t: return build_pipeline_candidate_style_entropy_report([],missing_tables=["pipeline_candidates|content_candidates"],**kw)
    cols=s[t]; rows=[dict(r) for r in c.execute(f"SELECT {_pick(cols,'id','candidate_id',fallback='rowid')} AS id,{_pick(cols,'run_id','pipeline_run_id',fallback='NULL')} AS run_id,{_pick(cols,'pipeline_run_id','run_id',fallback='NULL')} AS pipeline_run_id,{_pick(cols,'content_id',fallback='NULL')} AS content_id,{_pick(cols,'format','format_label','candidate_format',fallback='NULL')} AS format,{_pick(cols,'body','content','text','hook',fallback='NULL')} AS body FROM {t}")]
    return build_pipeline_candidate_style_entropy_report(rows,**kw)
def _key(r): return (_clean(r.get("run_id") or r.get("pipeline_run_id") or r.get("content_id") or "unknown"),_clean(r.get("content_id")))
def _fmt(r): return _clean(r.get("format") or r.get("format_label") or r.get("candidate_format") or "unknown").lower()
def _text(r): return _clean(r.get("body") or r.get("content") or r.get("text") or r.get("hook"))
def _opening(t): return re.match(r"\W*",t).group(0)[:3] if t else ""
def _verb(t):
    words=re.findall(r"[A-Za-z]+",t.lower()); return words[0] if words else ""
def _entropy(vals):
    vals=[v for v in vals if v]; n=len(vals)
    if n<=1: return 0.0
    h=-sum((c/n)*math.log(c/n,2) for c in Counter(vals).values()); return h/math.log(n,2)
def format_pipeline_candidate_style_entropy_json(r): return json.dumps(r,indent=2,sort_keys=True)
def format_pipeline_candidate_style_entropy_text(r): return "\n".join(["Pipeline Candidate Style Entropy"]+[f"- {f['run_id']} entropy={f['entropy_score']} reasons={','.join(f['risk_reasons'])}" for f in r.get("findings",[])] or ["No findings."])
def _clean(v): return "" if v is None else str(v).strip()
def _iso(n=None): return ((n or datetime.now(timezone.utc)).replace(tzinfo=timezone.utc) if (n or datetime.now(timezone.utc)).tzinfo is None else (n or datetime.now(timezone.utc)).astimezone(timezone.utc)).isoformat()
def _conn(db): c=getattr(db,"conn",db); c.row_factory=sqlite3.Row; return c
def _schema(c): return {str(r[0]):{str(x[1]) for x in c.execute(f"PRAGMA table_info({r[0]})")} for r in c.execute("SELECT name FROM sqlite_master WHERE type IN ('table','view')")}
def _pick(cols,*names,fallback="NULL"): return next((n for n in names if n in cols),fallback)
