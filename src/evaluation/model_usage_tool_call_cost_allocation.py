"""Allocate model usage costs to tool-call-heavy pipeline stages."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="model_usage_tool_call_cost_allocation"
def build_model_usage_tool_call_cost_allocation_report(usage_rows:list[dict[str,Any]],tool_rows:list[dict[str,Any]],*,since:str|None=None,stage:str|None=None,tool:str|None=None,missing_tables=None,missing_columns=None,now=None):
    since_dt=dt(since); sf=lower(stage); tf=lower(tool); tools_by_session={}
    for tr in tool_rows:
        sid=clean(tr.get("session_id") or tr.get("run_id")); tools_by_session.setdefault(sid,[]).append(tr)
    groups={}
    for row in usage_rows:
        used=dt(row.get("created_at") or row.get("usage_at"));
        if since_dt and used and used<since_dt: continue
        sid=clean(row.get("session_id") or row.get("run_id")); related=tools_by_session.get(sid,[])
        targets=related or [{"stage":row.get("stage") or "unallocated","tool_name":"unallocated","reason":"no_matching_tool_call"}]
        for tr in targets:
            st=clean(tr.get("stage") or row.get("stage") or "unallocated"); tn=clean(tr.get("tool_name") or tr.get("tool") or "unallocated")
            if sf and lower(st)!=sf: continue
            if tf and lower(tn)!=tf: continue
            key=(st,tn,clean(row.get("model")),used.date().isoformat() if used else None); g=groups.setdefault(key,{"stage":st,"tool_name":tn,"model":key[2],"day":key[3],"prompt_tokens":0,"completion_tokens":0,"total_tokens":0,"estimated_cost":0.0,"usage_count":0,"unallocated_reason":tr.get("reason")})
            g["prompt_tokens"]+=to_int(row.get("prompt_tokens") or row.get("input_tokens")); g["completion_tokens"]+=to_int(row.get("completion_tokens") or row.get("output_tokens")); g["total_tokens"]+=to_int(row.get("total_tokens")) or to_int(row.get("prompt_tokens") or row.get("input_tokens"))+to_int(row.get("completion_tokens") or row.get("output_tokens")); g["estimated_cost"]=round(g["estimated_cost"]+to_float(row.get("cost") or row.get("estimated_cost_usd")),6); g["usage_count"]+=1
    findings=sorted(groups.values(),key=lambda g:(g["stage"],g["tool_name"],g["model"],g["day"] or ""))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"since":since,"stage":stage,"tool":tool},"totals":{"usage_rows":len(usage_rows),"tool_rows":len(tool_rows),"groups":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No model usage tool-call allocations found.",schema_gap=bool(missing_tables or missing_columns))}
def build_model_usage_tool_call_cost_allocation_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; usage=[]; tools=[]
    utable="model_usage" if "model_usage" in s else "model_usage_records" if "model_usage_records" in s else None
    if not utable: mt.append("model_usage")
    else: usage=load_table(conn,utable,s[utable],{"session_id":("session_id","run_id"),"stage":("stage","pipeline_stage"),"model":("model","model_name"),"prompt_tokens":("prompt_tokens","input_tokens"),"completion_tokens":("completion_tokens","output_tokens"),"total_tokens":("total_tokens",),"cost":("cost","estimated_cost_usd"),"created_at":("created_at","usage_at")})
    ttable="tool_calls" if "tool_calls" in s else "session_tool_calls" if "session_tool_calls" in s else None
    if ttable: tools=load_table(conn,ttable,s[ttable],{"session_id":("session_id","run_id"),"stage":("stage","pipeline_stage"),"tool_name":("tool_name","tool"),"created_at":("created_at",)})
    return build_model_usage_tool_call_cost_allocation_report(usage,tools,missing_tables=mt,**kw)
def format_model_usage_tool_call_cost_allocation_json(r): return json_dumps(r)
def format_model_usage_tool_call_cost_allocation_text(r):
    lines=["Model Usage Tool Call Cost Allocation",f"Generated: {r['generated_at']}",f"Totals: usage={r['totals']['usage_rows']} groups={r['totals']['groups']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","stage | tool | model | day | tokens | cost"]
    for f in r["findings"]: lines.append(f"{f['stage']} | {f['tool_name']} | {f['model']} | {f['day']} | {f['total_tokens']} | {f['estimated_cost']}")
    return "\n".join(lines)
