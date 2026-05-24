"""Find pipeline candidates with token usage above cohort medians."""
from __future__ import annotations
from collections import defaultdict
from statistics import median
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="pipeline_candidate_prompt_token_outliers"; DEFAULT_LIMIT=50; DEFAULT_MULTIPLIER=2.0
def build_pipeline_candidate_prompt_token_outliers_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,multiplier:float=DEFAULT_MULTIPLIER,missing_tables=None,missing_columns=None,now=None):
    positive("limit",limit); positive("multiplier",multiplier); cohorts=defaultdict(list)
    for r in rows:
        total=to_int(r.get("total_tokens")) or to_int(r.get("prompt_tokens"))+to_int(r.get("completion_tokens"))
        cohorts[(clean(r.get("model"),"unknown"),clean(r.get("stage") or r.get("task_type"),"unknown"))].append(total)
    meds={k:median(v) for k,v in cohorts.items() if v}
    findings=[]
    for r in rows:
        key=(clean(r.get("model"),"unknown"),clean(r.get("stage") or r.get("task_type"),"unknown")); total=to_int(r.get("total_tokens")) or to_int(r.get("prompt_tokens"))+to_int(r.get("completion_tokens")); med=meds.get(key,0); ratio=round(total/med,4) if med else None
        if ratio is not None and ratio>=multiplier:
            findings.append({"run_id":r.get("run_id"),"candidate_id":r.get("candidate_id"),"content_id":r.get("content_id"),"model":key[0],"prompt_tokens":to_int(r.get("prompt_tokens")),"completion_tokens":to_int(r.get("completion_tokens")),"total_tokens":total,"cohort_median_tokens":med,"outlier_ratio":ratio,"recommended_action":"inspect prompt expansion and candidate payload size"})
    findings.sort(key=lambda f:(-f["outlier_ratio"],-f["total_tokens"],str(f["run_id"])))
    shown=findings[:limit]; summary={f"{k[0]}:{k[1]}":{"median_tokens":v,"rows":len(cohorts[k])} for k,v in sorted(meds.items())}
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"limit":limit,"multiplier":multiplier},"summary":{"row_count":len(rows),"outlier_count":len(findings),"shown":len(shown),"cohorts":summary},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No pipeline candidate token outliers found.",schema_gap=bool(missing_tables or missing_columns))}
def build_pipeline_candidate_prompt_token_outliers_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; rows=[]; table=None
    for t in ("model_usage","pipeline_candidate_usage","pipeline_runs","generated_content"):
        if t in s: table=t; break
    if not table: mt.append("model_usage|pipeline_runs|generated_content")
    else:
        c=s[table]
        if not ({"prompt_tokens","input_tokens"} & c): mc.setdefault(table,[]).append("prompt_tokens|input_tokens")
        if not ({"completion_tokens","output_tokens"} & c): mc.setdefault(table,[]).append("completion_tokens|output_tokens")
        if table not in mc: rows=load_table(conn,table,c,{"run_id":("run_id","pipeline_run_id","id"),"candidate_id":("candidate_id","candidate_key"),"content_id":("content_id","generated_content_id"),"model":("model","model_name"),"stage":("stage","task_type","pipeline_stage"),"prompt_tokens":("prompt_tokens","input_tokens"),"completion_tokens":("completion_tokens","output_tokens"),"total_tokens":("total_tokens",)})
    return build_pipeline_candidate_prompt_token_outliers_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_pipeline_candidate_prompt_token_outliers_json(r): return json_dumps(r)
def format_pipeline_candidate_prompt_token_outliers_text(r):
    s=r["summary"]; lines=["Pipeline Candidate Prompt Token Outliers",f"Generated: {r['generated_at']}",f"Totals: rows={s['row_count']} outliers={s['outlier_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","run_id | candidate_id | model | total | ratio"]
    for f in r["findings"]: lines.append(f"{f['run_id']} | {f['candidate_id'] or f['content_id']} | {f['model']} | {f['total_tokens']} | {f['outlier_ratio']}")
    return "\n".join(lines)
