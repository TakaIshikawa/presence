"""Summarize model usage by pipeline stage against daily budgets."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="model_usage_stage_budget_burn"; DEFAULT_DAYS=7; DEFAULT_LIMIT=20; DEFAULT_BUDGET_USD=10.0; DEFAULT_TOKEN_BUDGET=100000
def build_model_usage_stage_budget_burn_report(rows:list[dict[str,Any]],*,days:int=DEFAULT_DAYS,limit:int=DEFAULT_LIMIT,stage:str|None=None,budget_usd:float=DEFAULT_BUDGET_USD,token_budget:int=DEFAULT_TOKEN_BUDGET,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
 positive("days",days); positive("limit",limit); positive("budget_usd",budget_usd); positive("token_budget",token_budget); wanted=lower(stage) if stage else None; buckets=defaultdict(lambda:{"stage":"","cost_usd":0.0,"tokens":0,"runs":[]})
 for r in rows:
  st=lower(r.get("stage") or r.get("pipeline_stage") or "unknown","unknown")
  if wanted and st!=wanted: continue
  b=buckets[st]; b["stage"]=st; cost=to_float(r.get("cost_usd") or r.get("cost") or r.get("total_cost_usd")); toks=to_int(r.get("tokens") or r.get("total_tokens") or to_int(r.get("input_tokens"))+to_int(r.get("output_tokens"))); b["cost_usd"]+=cost; b["tokens"]+=toks; b["runs"].append({"run_id":r.get("run_id") or r.get("id"),"model":clean(r.get("model")) or None,"cost_usd":round(cost,4),"tokens":toks})
 stages=[]
 for b in buckets.values():
  b["cost_usd"]=round(b["cost_usd"],4); b["percent_budget_used"]=round((b["cost_usd"]/budget_usd)*100,2); b["percent_token_budget_used"]=round((b["tokens"]/token_budget)*100,2); b["over_budget"]=b["cost_usd"]>budget_usd or b["tokens"]>token_budget; b["top_runs"]=sorted(b.pop("runs"),key=lambda x:(-x["cost_usd"],-x["tokens"],str(x["run_id"])))[:limit]; stages.append(b)
 stages.sort(key=lambda b:(not b["over_budget"],-b["percent_budget_used"],-b["percent_token_budget_used"],b["stage"]))
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"days":days,"limit":limit,"stage":stage,"budget_usd":budget_usd,"token_budget":token_budget},"summary":{"stage_count":len(stages),"over_budget_count":sum(1 for s in stages if s["over_budget"]),"total_cost_usd":round(sum(s["cost_usd"] for s in stages),4),"total_tokens":sum(s["tokens"] for s in stages)},"stages":stages[:limit],"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(stages,"No model usage stage budget burn found.",schema_gap=bool(missing_tables or missing_columns))}
def build_model_usage_stage_budget_burn_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn)
 if "model_usage" not in s: return build_model_usage_stage_budget_burn_report([],missing_tables=["model_usage"],**kw)
 c=s["model_usage"]; miss=[] if ({"stage","pipeline_stage"}&c) else ["stage|pipeline_stage"]
 if not ({"cost_usd","cost","total_cost_usd"}&c): miss.append("cost_usd|cost|total_cost_usd")
 if not ({"tokens","total_tokens","input_tokens","output_tokens"}&c): miss.append("tokens|total_tokens|input_tokens|output_tokens")
 if miss: return build_model_usage_stage_budget_burn_report([],missing_columns={"model_usage":miss},**kw)
 days=kw.get("days",DEFAULT_DAYS); where=[]; params=[]
 if "created_at" in c: where.append("(created_at IS NULL OR created_at >= ?)"); params.append((now_value(kw.get("now"))-timedelta(days=days)).isoformat())
 q=f"SELECT {pick(c,'id','run_id',out='run_id')}, {pick(c,'stage','pipeline_stage',out='stage')}, {pick(c,'model','model_name',out='model')}, {pick(c,'cost_usd','cost','total_cost_usd',default='0',out='cost_usd')}, {pick(c,'tokens','total_tokens',default='0',out='tokens')}, {pick(c,'input_tokens',default='0',out='input_tokens')}, {pick(c,'output_tokens',default='0',out='output_tokens')} FROM model_usage"+((" WHERE "+" AND ".join(where)) if where else "")+" ORDER BY rowid"
 return build_model_usage_stage_budget_burn_report([dict(r) for r in conn.execute(q,params)],**kw)
def format_model_usage_stage_budget_burn_json(r): return json_dumps(r)
def format_model_usage_stage_budget_burn_text(r):
 lines=["Model Usage Stage Budget Burn",f"Artifact: {r['artifact_type']}",f"Generated: {r['generated_at']}",f"Totals: stages={r['summary']['stage_count']} over_budget={r['summary']['over_budget_count']} cost={r['summary']['total_cost_usd']} tokens={r['summary']['total_tokens']}"]
 for s in r["stages"]: lines.append(f"- {s['stage']}: ${s['cost_usd']} {s['tokens']} tokens budget={s['percent_budget_used']}% token_budget={s['percent_token_budget_used']}% over={s['over_budget']}")
 return "\n".join(lines)
