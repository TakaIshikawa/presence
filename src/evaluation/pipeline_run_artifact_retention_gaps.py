"""Identify pipeline run artifact retention gaps."""
from __future__ import annotations
from collections import Counter
from datetime import datetime,timezone
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema,to_int
ARTIFACT_TYPE="pipeline_run_artifact_retention_gaps"; DEFAULT_MAX_AGE_DAYS=30; DEFAULT_MAX_SIZE_BYTES=104857600; DEFAULT_LIMIT=50
def build_pipeline_run_artifact_retention_gaps_report(run_rows:list[dict[str,Any]],artifact_rows:list[dict[str,Any]]|None=None,*,max_age_days:int=DEFAULT_MAX_AGE_DAYS,max_size_bytes:int=DEFAULT_MAX_SIZE_BYTES,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("max_age_days",max_age_days); positive("max_size_bytes",max_size_bytes); positive("limit",limit); gen=now if isinstance(now,datetime) else datetime.now(timezone.utc)
 by_run={clean(a.get("run_id") or a.get("pipeline_run_id")):a for a in (artifact_rows or [])}; findings=[]
 for r in run_rows:
  rid=clean(r.get("run_id") or r.get("id")); stage=clean(r.get("stage"),"unknown"); art=by_run.get(rid); created=dt(r.get("created_at") or r.get("completed_at")); age=(gen-created).days if created else 0
  if not art: findings.append(_f("missing_artifact",rid,stage,age,None)); continue
  size=to_int(art.get("size_bytes")) or 0; expires=dt(art.get("expires_at"))
  if expires and expires<gen: findings.append(_f("expired_artifact",rid,stage,age,size))
  if size>max_size_bytes: findings.append(_f("oversized_artifact",rid,stage,age,size))
  if not clean(art.get("path") or art.get("url") or art.get("artifact_uri")): findings.append(_f("unlinked_artifact",rid,stage,age,size))
 for a in artifact_rows or []:
  rid=clean(a.get("run_id") or a.get("pipeline_run_id"))
  if rid and not any(clean(r.get("run_id") or r.get("id"))==rid for r in run_rows): findings.append(_f("orphan_artifact",rid,clean(a.get("stage"),"unknown"),0,to_int(a.get("size_bytes"))))
 findings.sort(key=lambda f:(f["issue_type"],-f["age_days"],f["run_id"])); shown=findings[:limit]; by_stage=Counter(f["stage"] for f in findings)
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"max_age_days":max_age_days,"max_size_bytes":max_size_bytes,"limit":limit},"totals":{"run_count":len(run_rows),"artifact_count":len(artifact_rows or []),"finding_count":len(findings),"shown_findings":len(shown)},"stage_summary":dict(sorted(by_stage.items())),"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No pipeline run artifact retention gaps found." if not findings else None}}
def build_pipeline_run_artifact_retention_gaps_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "pipeline_runs" in s else ["pipeline_runs"]; mc={}; arts=[]
 for t in ("pipeline_artifacts","publish_artifacts"):
  if t in s: arts+=_arts(conn,t,s[t],mc)
 return build_pipeline_run_artifact_retention_gaps_report(_runs(conn,s["pipeline_runs"],mc) if "pipeline_runs" in s else [],arts,missing_tables=mt,missing_columns=mc,**kwargs)
def format_pipeline_run_artifact_retention_gaps_json(report:dict[str,Any])->str: return json_dumps(report)
def format_pipeline_run_artifact_retention_gaps_text(report:dict[str,Any])->str:
 lines=["Pipeline Run Artifact Retention Gaps",f"Generated: {report['generated_at']}",f"Totals: runs={report['totals']['run_count']} artifacts={report['totals']['artifact_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","issue | run_id | stage | age_days | size_bytes"]
 for f in report["findings"]: lines.append(f"{f['issue_type']} | {f['run_id']} | {f['stage']} | {f['age_days']} | {f['size_bytes']}")
 return "\n".join(lines)
def _f(issue:str,rid:str,stage:str,age:int,size:int|None)->dict[str,Any]: return {"issue_type":issue,"run_id":rid,"stage":stage,"age_days":age,"size_bytes":size}
def _runs(conn:Any,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 select=[expr(cols,"id","run_id",out="run_id"),expr(cols,"stage",default="'unknown'",out="stage"),expr(cols,"created_at","completed_at",out="created_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM pipeline_runs ORDER BY rowid")]
def _arts(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 select=[expr(cols,"run_id","pipeline_run_id",out="run_id"),expr(cols,"stage",default="'unknown'",out="stage"),expr(cols,"path","url","artifact_uri",out="path"),expr(cols,"expires_at",out="expires_at"),expr(cols,"size_bytes","bytes",default="0",out="size_bytes")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY rowid")]
