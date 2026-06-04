"""Find unusually slow publication attempts."""
from __future__ import annotations
from statistics import median
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="publication_attempt_latency_spikes"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_DURATION_MS=1000; DEFAULT_PERCENTILE_BASELINE=0.5; DEFAULT_LIMIT=50
def build_publication_attempt_latency_spikes_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_duration_ms:int=DEFAULT_MIN_DURATION_MS,percentile_baseline:float=DEFAULT_PERCENTILE_BASELINE,platform:str|None=None,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
 positive("lookback_days",lookback_days); positive("min_duration_ms",min_duration_ms); bounded_share("percentile_baseline",percentile_baseline); positive("limit",limit)
 gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); plat=lower(platform); prepared=[]; durations=[]
 for row in rows:
  rp=lower(row.get("platform")); attempted=dt(row.get("attempted_at") or row.get("started_at") or row.get("created_at"))
  if plat and rp!=plat: continue
  if attempted and attempted<cutoff: continue
  dur=_duration(row)
  if dur is None: continue
  item={"attempt_id":clean(row.get("id") or row.get("attempt_id")),"content_id":clean(row.get("content_id") or row.get("post_id")),"platform":rp or "unknown","provider":clean(row.get("provider")),"duration_ms":dur,"attempted_at":attempted.isoformat() if attempted else ""}
  prepared.append(item); durations.append(dur)
 baseline=_baseline(durations,percentile_baseline)
 findings=[]
 for item in prepared:
  if item["duration_ms"]>=min_duration_ms and (baseline==0 or item["duration_ms"]>=baseline*1.5):
   out=dict(item); out["baseline_ms"]=baseline; out["spike_ratio"]=round(item["duration_ms"]/baseline,4) if baseline else None; findings.append(out)
 findings.sort(key=lambda i:(-i["duration_ms"],i["attempt_id"])); findings=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_duration_ms":min_duration_ms,"percentile_baseline":percentile_baseline,"platform":platform,"limit":limit},"summary":{"attempts_scanned":len(prepared),"spike_count":len(findings)},"spikes":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No publication attempt latency spikes found.",schema_gap=bool(missing_tables or missing_columns))}
def build_publication_attempt_latency_spikes_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); sch=schema(conn); table=next((t for t in ("publication_attempts","publish_attempts") if t in sch),None)
 if not table: return build_publication_attempt_latency_spikes_report([],missing_tables=["publication_attempts"],**kwargs)
 rows=load_table(conn,table,sch[table],{"id":("id","attempt_id"),"content_id":("content_id","post_id"),"platform":("platform","channel"),"provider":("provider",),"duration_ms":("duration_ms","latency_ms","elapsed_ms"),"attempted_at":("attempted_at","started_at","created_at"),"completed_at":("completed_at","finished_at")})
 return build_publication_attempt_latency_spikes_report(rows,**kwargs)
def format_publication_attempt_latency_spikes_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publication_attempt_latency_spikes_text(report:dict[str,Any])->str:
 lines=["Publication Attempt Latency Spikes",f"Generated: {report['generated_at']}",f"Totals: scanned={report['summary']['attempts_scanned']} spikes={report['summary']['spike_count']}"]
 if not report["spikes"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
 lines+=["","attempt_id | content_id | platform | provider | duration_ms | baseline_ms | ratio"]+[f"{i['attempt_id']} | {i['content_id']} | {i['platform']} | {i['provider']} | {i['duration_ms']} | {i['baseline_ms']} | {i['spike_ratio']}" for i in report["spikes"]]
 return "\n".join(lines)
def _duration(row:dict[str,Any])->int|None:
 val=row.get("duration_ms") or row.get("latency_ms") or row.get("elapsed_ms")
 if val not in (None,""): return to_int(val)
 a=dt(row.get("attempted_at") or row.get("started_at") or row.get("created_at")); b=dt(row.get("completed_at") or row.get("finished_at"))
 return int((b-a).total_seconds()*1000) if a and b and b>=a else None
def _baseline(values:list[int],p:float)->int:
 if not values: return 0
 vals=sorted(values); idx=min(len(vals)-1,max(0,int(round((len(vals)-1)*p))))
 return int(vals[idx])
