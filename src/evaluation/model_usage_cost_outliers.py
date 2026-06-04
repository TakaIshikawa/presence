"""Identify high-cost model usage records or runs."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="model_usage_cost_outliers"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_COST_USD=1.0; DEFAULT_GROUP_BY="usage"; DEFAULT_LIMIT=50
def build_model_usage_cost_outliers_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_cost_usd:float=DEFAULT_MIN_COST_USD,group_by:str=DEFAULT_GROUP_BY,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
 positive("lookback_days",lookback_days); non_negative("min_cost_usd",min_cost_usd); positive("limit",limit)
 if group_by not in {"usage","run"}: raise ValueError("group_by must be usage or run")
 gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); filtered=[]
 for row in rows:
  created=dt(row.get("created_at") or row.get("timestamp"))
  if created and created<cutoff: continue
  filtered.append(row)
 if group_by=="run":
  buckets=defaultdict(lambda:{"tokens":0,"cost_usd":0.0,"models":set(),"operations":set(),"usage_ids":[],"created_at":""})
  for r in filtered:
   key=clean(r.get("run_id") or r.get("trace_id") or r.get("id"))
   b=buckets[key]; b["tokens"]+=to_int(r.get("total_tokens") or r.get("tokens")); b["cost_usd"]+=to_float(r.get("estimated_cost") or r.get("cost_usd")); b["models"].add(clean(r.get("model") or r.get("model_name"))); b["operations"].add(clean(r.get("operation") or r.get("operation_name"))); b["usage_ids"].append(clean(r.get("id") or r.get("usage_id")))
  findings=[{"usage_id":None,"run_id":k,"model":",".join(sorted(v["models"])),"operation":",".join(sorted(v["operations"])),"tokens":v["tokens"],"cost_usd":round(v["cost_usd"],6),"outlier_reason":f"run cost >= ${min_cost_usd:g}","usage_ids":v["usage_ids"]} for k,v in buckets.items() if v["cost_usd"]>=min_cost_usd]
 else:
  findings=[{"usage_id":clean(r.get("id") or r.get("usage_id")),"run_id":clean(r.get("run_id") or r.get("trace_id")),"model":clean(r.get("model") or r.get("model_name")),"operation":clean(r.get("operation") or r.get("operation_name")),"tokens":to_int(r.get("total_tokens") or r.get("tokens")),"cost_usd":round(to_float(r.get("estimated_cost") or r.get("cost_usd")),6),"outlier_reason":f"usage cost >= ${min_cost_usd:g}"} for r in filtered if to_float(r.get("estimated_cost") or r.get("cost_usd"))>=min_cost_usd]
 findings.sort(key=lambda i:(-i["cost_usd"],i.get("usage_id") or i.get("run_id") or "")); findings=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_cost_usd":min_cost_usd,"group_by":group_by,"limit":limit},"summary":{"usage_records_scanned":len(filtered),"outlier_count":len(findings)},"outliers":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No model usage cost outliers found.",schema_gap=bool(missing_tables or missing_columns))}
def build_model_usage_cost_outliers_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); sch=schema(conn)
 if "model_usage" not in sch: return build_model_usage_cost_outliers_report([],missing_tables=["model_usage"],**kwargs)
 rows=load_table(conn,"model_usage",sch["model_usage"],{"id":("id","usage_id"),"run_id":("run_id","trace_id"),"model":("model","model_name"),"operation":("operation","operation_name"),"total_tokens":("total_tokens","tokens"),"estimated_cost":("estimated_cost","cost_usd"),"created_at":("created_at","timestamp")})
 return build_model_usage_cost_outliers_report(rows,**kwargs)
def format_model_usage_cost_outliers_json(report:dict[str,Any])->str: return json_dumps(report)
def format_model_usage_cost_outliers_text(report:dict[str,Any])->str:
 lines=["Model Usage Cost Outliers",f"Generated: {report['generated_at']}",f"Totals: scanned={report['summary']['usage_records_scanned']} outliers={report['summary']['outlier_count']}"]
 if not report["outliers"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
 lines+=["","usage_id | run_id | model | operation | tokens | cost_usd | reason"]+[f"{i.get('usage_id')} | {i.get('run_id')} | {i['model']} | {i['operation']} | {i['tokens']} | {i['cost_usd']} | {i['outlier_reason']}" for i in report["outliers"]]
 return "\n".join(lines)
