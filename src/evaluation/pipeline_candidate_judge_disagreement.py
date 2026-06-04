"""Find pipeline candidates with materially divergent judge results."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="pipeline_candidate_judge_disagreement"; DEFAULT_LOOKBACK_DAYS=30; DEFAULT_MIN_SCORE_DELTA=0.25; DEFAULT_LIMIT=50
def build_pipeline_candidate_judge_disagreement_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,min_score_delta:float=DEFAULT_MIN_SCORE_DELTA,evaluators:list[str]|tuple[str,...]|None=None,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
 positive("lookback_days",lookback_days); non_negative("min_score_delta",min_score_delta); positive("limit",limit)
 gen=now_value(now); cutoff=gen-timedelta(days=lookback_days); allowed={lower(e) for e in evaluators or []}; groups=defaultdict(list)
 for row in rows:
  ev=lower(row.get("evaluator") or row.get("judge"))
  if allowed and ev not in allowed: continue
  created=dt(row.get("created_at") or row.get("evaluated_at"))
  if created and created<cutoff: continue
  key=(clean(row.get("run_id")),clean(row.get("candidate_id")))
  groups[key].append({"evaluator":ev,"score":to_float(row.get("score"),None),"label":lower(row.get("label") or row.get("decision"))})
 findings=[]
 for (run_id,candidate_id),items in groups.items():
  if len(items)<2: continue
  scores=[i["score"] for i in items if i["score"] is not None]; delta=round(max(scores)-min(scores),4) if len(scores)>=2 else 0.0
  labels={i["label"] for i in items if i["label"]}; conflict=len(labels)>1 and bool(labels & {"pass","approve","approved","yes"}) and bool(labels & {"fail","reject","rejected","no"})
  if delta>=min_score_delta or conflict:
   reason="score_delta" if delta>=min_score_delta else "label_conflict"
   findings.append({"run_id":run_id,"candidate_id":candidate_id,"evaluators":[i["evaluator"] for i in items],"scores":[i["score"] for i in items],"labels":[i["label"] for i in items],"score_delta":delta,"disagreement_reason":reason})
 findings.sort(key=lambda i:(-i["score_delta"],i["run_id"],i["candidate_id"])); findings=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookback_days":lookback_days,"min_score_delta":min_score_delta,"evaluators":list(evaluators or []),"limit":limit},"summary":{"candidates_scanned":len(groups),"disagreement_count":len(findings)},"disagreements":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No pipeline candidate judge disagreement found.",schema_gap=bool(missing_tables or missing_columns))}
def build_pipeline_candidate_judge_disagreement_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); sch=schema(conn); table=next((t for t in ("pipeline_candidate_evaluations","candidate_evaluations") if t in sch),None)
 if not table: return build_pipeline_candidate_judge_disagreement_report([],missing_tables=["pipeline_candidate_evaluations"],**kwargs)
 rows=load_table(conn,table,sch[table],{"run_id":("run_id",),"candidate_id":("candidate_id",),"evaluator":("evaluator","judge"),"score":("score",),"label":("label","decision"),"created_at":("created_at","evaluated_at")})
 return build_pipeline_candidate_judge_disagreement_report(rows,**kwargs)
def format_pipeline_candidate_judge_disagreement_json(report:dict[str,Any])->str: return json_dumps(report)
def format_pipeline_candidate_judge_disagreement_text(report:dict[str,Any])->str:
 lines=["Pipeline Candidate Judge Disagreement",f"Generated: {report['generated_at']}",f"Totals: candidates={report['summary']['candidates_scanned']} disagreements={report['summary']['disagreement_count']}"]
 if not report["disagreements"]: lines.append(report["empty_state"]["message"]); return "\n".join(lines)
 lines+=["","run_id | candidate_id | evaluators | score_delta | reason"]+[f"{i['run_id']} | {i['candidate_id']} | {','.join(i['evaluators'])} | {i['score_delta']} | {i['disagreement_reason']}" for i in report["disagreements"]]
 return "\n".join(lines)
