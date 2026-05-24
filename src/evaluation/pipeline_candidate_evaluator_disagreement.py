"""Report candidate evaluation runs with evaluator disagreement."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="pipeline_candidate_evaluator_disagreement"; DEFAULT_MIN_SCORE_SPREAD=0.35
def build_pipeline_candidate_evaluator_disagreement_report(rows:list[dict[str,Any]],*,min_score_spread:float=DEFAULT_MIN_SCORE_SPREAD,since:str|None=None,missing_tables=None,missing_columns=None,now=None):
    bounded_share("min_score_spread",min_score_spread); since_dt=dt(since); findings=[]
    for row in rows:
        created=dt(row.get("created_at") or row.get("evaluated_at")); 
        if since_dt and created and created<since_dt: continue
        data=_artifact(row); scores=_scores(data,row); gates=_gates(data,row); spread=round(max(scores)-min(scores),4) if scores else 0.0; gate_dis=len(set(gates))>1 if gates else False; selected=clean(row.get("selected_candidate_id") or data.get("selected_candidate_id")); rank=_selected_rank(data,selected); anomaly=rank is not None and rank>1
        if spread>=min_score_spread or gate_dis or anomaly: findings.append({"run_id":row.get("run_id") or row.get("id"),"content_type":clean(row.get("content_type") or data.get("content_type"),"unknown"),"score_spread":spread,"disagreeing_evaluators":list(_evaluator_names(data,row)),"selected_candidate_id":selected or None,"selected_candidate_rank":rank,"severity":"high" if spread>=min_score_spread*1.5 or gate_dis else "medium","reasons":[r for r,b in (("score_spread",spread>=min_score_spread),("gate_disagreement",gate_dis),("winner_rank_anomaly",anomaly)) if b]})
    findings.sort(key=lambda f:({"high":0,"medium":1}.get(f["severity"],2),-f["score_spread"],str(f["run_id"])))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"min_score_spread":min_score_spread,"since":since},"totals":{"runs":len(rows),"findings":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No evaluator disagreement found.",schema_gap=bool(missing_tables or missing_columns))}
def build_pipeline_candidate_evaluator_disagreement_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table="pipeline_candidate_evaluations" if "pipeline_candidate_evaluations" in s else "pipeline_runs" if "pipeline_runs" in s else None
    if not table: return build_pipeline_candidate_evaluator_disagreement_report([],missing_tables=["pipeline_candidate_evaluations"],**kw)
    rows=load_table(conn,table,s[table],{"run_id":("run_id","id"),"content_type":("content_type",),"artifact_json":("artifact_json","evaluation_json","metadata","payload"),"score":("score",),"evaluator":("evaluator","evaluator_name"),"gate":("gate","gate_decision"),"selected_candidate_id":("selected_candidate_id",),"created_at":("created_at","evaluated_at")})
    return build_pipeline_candidate_evaluator_disagreement_report(rows,**kw)
def format_pipeline_candidate_evaluator_disagreement_json(r): return json_dumps(r)
def format_pipeline_candidate_evaluator_disagreement_text(r):
    lines=["Pipeline Candidate Evaluator Disagreement",f"Generated: {r['generated_at']}",f"Totals: runs={r['totals']['runs']} findings={r['totals']['findings']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","run_id | content_type | score_spread | severity | reasons"]
    for f in r["findings"]: lines.append(f"{f['run_id']} | {f['content_type']} | {f['score_spread']} | {f['severity']} | {','.join(f['reasons'])}")
    return "\n".join(lines)
def _artifact(row): 
    try: return json.loads(clean(row.get("artifact_json")))
    except Exception: return {}
def _scores(data,row):
    vals=[to_float(row.get("score"),None)] if row.get("score") not in (None,"") else []
    for ev in data.get("evaluations",[]) if isinstance(data.get("evaluations"),list) else []: vals.append(to_float(ev.get("score"),0))
    return [v for v in vals if v is not None]
def _gates(data,row):
    vals=[lower(row.get("gate"))] if row.get("gate") not in (None,"") else []
    vals += [lower(ev.get("gate") or ev.get("decision")) for ev in data.get("evaluations",[]) if isinstance(ev,dict)]
    return [v for v in vals if v]
def _evaluator_names(data,row):
    vals={clean(row.get("evaluator"))} if row.get("evaluator") else set()
    vals |= {clean(ev.get("evaluator") or ev.get("name")) for ev in data.get("evaluations",[]) if isinstance(ev,dict)}
    return sorted(v for v in vals if v)
def _selected_rank(data,selected):
    for cand in data.get("candidates",[]) if isinstance(data.get("candidates"),list) else []:
        if clean(cand.get("id") or cand.get("candidate_id"))==selected: return to_int(cand.get("rank"),0) or None
    return None
