"""Compare publication retry reasons between baseline and current windows."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timedelta,timezone
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema,to_float
ARTIFACT_TYPE="publication_attempt_retry_reason_drift"; DEFAULT_BASELINE_DAYS=28; DEFAULT_CURRENT_DAYS=7; DEFAULT_MIN_DELTA=0.25; DEFAULT_LIMIT=50
def build_publication_attempt_retry_reason_drift_report(rows:list[dict[str,Any]],*,baseline_days:int=DEFAULT_BASELINE_DAYS,current_days:int=DEFAULT_CURRENT_DAYS,min_delta:float=DEFAULT_MIN_DELTA,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("baseline_days",baseline_days); positive("current_days",current_days); positive("limit",limit)
 if min_delta<0: raise ValueError("min_delta must be non-negative")
 gen=now if isinstance(now,datetime) else datetime.now(timezone.utc); current_start=gen-timedelta(days=current_days); baseline_start=current_start-timedelta(days=baseline_days)
 buckets=defaultdict(lambda:{"baseline":Counter(),"current":Counter()})
 for r in rows:
  ts=dt(r.get("attempted_at") or r.get("created_at")); 
  if not ts or ts<baseline_start: continue
  window="current" if ts>=current_start else "baseline"; platform=clean(r.get("platform"),"unknown"); reason=clean(r.get("retry_reason") or r.get("error_category"),"unknown")
  buckets[platform][window][reason]+=1
 findings=[]; breakdown=[]
 for platform,b in buckets.items():
  reasons=sorted(set(b["baseline"])|set(b["current"]))
  bt=sum(b["baseline"].values()); ct=sum(b["current"].values())
  for reason in reasons:
   bs=b["baseline"][reason]; cs=b["current"][reason]; bshare=bs/bt if bt else 0; cshare=cs/ct if ct else 0; delta=round(cshare-bshare,4)
   row={"platform":platform,"reason":reason,"baseline_count":bs,"current_count":cs,"baseline_share":round(bshare,4),"current_share":round(cshare,4),"delta":delta}
   breakdown.append(row)
   if abs(delta)>=min_delta and cs: findings.append({**row,"severity":round(abs(delta)*100+cs,2)})
 findings.sort(key=lambda f:(-f["severity"],f["platform"],f["reason"])); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"baseline_days":baseline_days,"current_days":current_days,"min_delta":min_delta,"limit":limit},"totals":{"row_count":len(rows),"finding_count":len(findings),"shown_findings":len(shown)},"reason_breakdown":sorted(breakdown,key=lambda r:(r["platform"],r["reason"])),"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No publication attempt retry reason drift found." if not findings else None}}
def build_publication_attempt_retry_reason_drift_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); table="publication_retries" if "publication_retries" in s else "publication_attempts" if "publication_attempts" in s else None; mc={}
 if not table: return build_publication_attempt_retry_reason_drift_report([],missing_tables=["publication_attempts"],**kwargs)
 return build_publication_attempt_retry_reason_drift_report(_load(conn,table,s[table],mc),missing_columns=mc,**kwargs)
def format_publication_attempt_retry_reason_drift_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publication_attempt_retry_reason_drift_text(report:dict[str,Any])->str:
 lines=["Publication Attempt Retry Reason Drift",f"Generated: {report['generated_at']}",f"Totals: rows={report['totals']['row_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","platform | reason | baseline | current | delta"]
 for f in report["findings"]: lines.append(f"{f['platform']} | {f['reason']} | {f['baseline_share']} | {f['current_share']} | {f['delta']}")
 return "\n".join(lines)
def _load(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 missing=[c for c in ("attempted_at","platform") if c not in cols]
 if "retry_reason" not in cols and "error_category" not in cols: missing.append("retry_reason")
 if missing: mc[t]=missing
 select=[expr(cols,"attempted_at","created_at",out="attempted_at"),expr(cols,"platform",default="'unknown'",out="platform"),expr(cols,"retry_reason","error_category",default="'unknown'",out="retry_reason"),expr(cols,"status",default="'retry'",out="status")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY 1")]
