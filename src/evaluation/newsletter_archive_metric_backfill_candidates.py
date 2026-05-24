"""Find archived newsletter issues missing downstream metrics."""
from __future__ import annotations
from datetime import datetime,timezone
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema,to_int
ARTIFACT_TYPE="newsletter_archive_metric_backfill_candidates"; DEFAULT_MIN_AGE_HOURS=24; DEFAULT_LIMIT=50; METRICS=("opens","clicks","bounces","unsubscribes")
def build_newsletter_archive_metric_backfill_candidates_report(issue_rows:list[dict[str,Any]],metric_rows:list[dict[str,Any]]|None=None,*,min_age_hours:int=DEFAULT_MIN_AGE_HOURS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("min_age_hours",min_age_hours); positive("limit",limit); gen=now if isinstance(now,datetime) else datetime.now(timezone.utc)
 metrics={clean(r.get("issue_id")):{m for m in METRICS if to_int(r.get(m)) is not None and to_int(r.get(m))>0} for r in (metric_rows or [])}
 findings=[]
 for row in issue_rows:
  status=clean(row.get("status")).lower(); sent=dt(row.get("sent_at") or row.get("published_at") or row.get("created_at"))
  if status not in {"sent","published","archived","complete","completed"} or not sent: continue
  age=(gen.astimezone(timezone.utc)-sent).total_seconds()/3600
  if age<min_age_hours: continue
  iid=clean(row.get("issue_id") or row.get("id")); missing=[m for m in METRICS if m not in metrics.get(iid,set())]
  if missing:
   audience=to_int(row.get("audience_size") or row.get("recipient_count")) or 0
   findings.append({"issue_id":iid,"status":status,"sent_at":sent.isoformat(),"age_hours":round(age,2),"audience_size":audience,"missing_metrics":missing,"priority_score":round(age/24+audience/1000+len(missing)*5,2)})
 findings.sort(key=lambda f:(-f["priority_score"],f["issue_id"]))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"min_age_hours":min_age_hours,"limit":limit},"totals":{"issue_count":len(issue_rows),"candidate_count":len(findings),"shown_findings":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No newsletter archive metric backfill candidates found." if not findings else None}}
def build_newsletter_archive_metric_backfill_candidates_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "newsletter_issues" in s else ["newsletter_issues"]; mc={}
 issues=_issues(conn,s["newsletter_issues"],mc) if "newsletter_issues" in s else []; metrics=[]
 for t in ("newsletter_metrics","newsletter_issue_metrics"):
  if t in s: metrics+=_metrics(conn,t,s[t],mc)
 return build_newsletter_archive_metric_backfill_candidates_report(issues,metrics,missing_tables=mt,missing_columns=mc,**kwargs)
def format_newsletter_archive_metric_backfill_candidates_json(report:dict[str,Any])->str: return json_dumps(report)
def format_newsletter_archive_metric_backfill_candidates_text(report:dict[str,Any])->str:
 lines=["Newsletter Archive Metric Backfill Candidates",f"Generated: {report['generated_at']}",f"Totals: issues={report['totals']['issue_count']} candidates={report['totals']['candidate_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","issue_id | score | age_hours | audience | missing_metrics"]
 for f in report["findings"]: lines.append(f"{f['issue_id']} | {f['priority_score']} | {f['age_hours']} | {f['audience_size']} | {','.join(f['missing_metrics'])}")
 return "\n".join(lines)
def _issues(conn:Any,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 if "id" not in cols and "issue_id" not in cols: mc["newsletter_issues"]=["id"]; return []
 select=[expr(cols,"id","issue_id",out="issue_id"),expr(cols,"status",default="'sent'",out="status"),expr(cols,"sent_at","published_at","created_at",out="sent_at"),expr(cols,"audience_size","recipient_count",default="0",out="audience_size")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_issues ORDER BY 1")]
def _metrics(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 iid=next((c for c in ("issue_id","newsletter_issue_id") if c in cols),None)
 if not iid: mc[t]=["issue_id"]; return []
 select=[f"{iid} AS issue_id"]+[expr(cols,m,default="NULL",out=m) for m in METRICS]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY {iid}")]
