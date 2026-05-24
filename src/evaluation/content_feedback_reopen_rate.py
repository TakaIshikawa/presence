"""Track content feedback reopen rates after resolution."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timezone
from statistics import median
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,lower,now_iso,positive,schema
ARTIFACT_TYPE="content_feedback_reopen_rate"; DEFAULT_WINDOW_DAYS=30; DEFAULT_MIN_RESOLVED=1; DEFAULT_LIMIT=50
def build_content_feedback_reopen_rate_report(rows:list[dict[str,Any]],*,window_days:int=DEFAULT_WINDOW_DAYS,min_resolved:int=DEFAULT_MIN_RESOLVED,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("window_days",window_days); positive("min_resolved",min_resolved); positive("limit",limit)
 items=defaultdict(list)
 for r in rows: items[clean(r.get("feedback_id") or r.get("id"),"unknown")].append(r)
 groups=defaultdict(lambda:{"resolved":0,"reopened":0,"hours":[]}); findings=[]
 for fid,events in items.items():
  events.sort(key=lambda r:dt(r.get("occurred_at") or r.get("updated_at") or r.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
  resolved=None; base=events[-1]; reviewer=clean(base.get("reviewer") or base.get("reviewer_id"),"unknown"); ctype=clean(base.get("content_type"),"unknown"); reason=clean(base.get("resolution_reason"),"unknown"); key=(reviewer,ctype,reason)
  for e in events:
   status=lower(e.get("status") or e.get("event_type")); ts=dt(e.get("occurred_at") or e.get("updated_at") or e.get("created_at"))
   if status in {"resolved","closed","done"} and ts: resolved=ts; groups[key]["resolved"]+=1
   if status in {"reopened","open"} and resolved and ts and ts>resolved:
    h=(ts-resolved).total_seconds()/3600; groups[key]["reopened"]+=1; groups[key]["hours"].append(h); findings.append({"feedback_id":fid,"reviewer":reviewer,"content_type":ctype,"resolution_reason":reason,"resolved_at":resolved.isoformat(),"reopened_at":ts.isoformat(),"hours_to_reopen":round(h,2)}); resolved=None
 breakdown=[]
 for (reviewer,ctype,reason),g in groups.items():
  if g["resolved"]>=min_resolved:
   breakdown.append({"reviewer":reviewer,"content_type":ctype,"resolution_reason":reason,"resolved_count":g["resolved"],"reopen_count":g["reopened"],"reopen_rate":round(g["reopened"]/g["resolved"],4),"median_hours_to_reopen":round(median(g["hours"]),2) if g["hours"] else None})
 findings.sort(key=lambda f:(f["reopened_at"],f["feedback_id"]),reverse=True); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"window_days":window_days,"min_resolved":min_resolved,"limit":limit},"totals":{"feedback_count":len(items),"resolved_count":sum(b["resolved_count"] for b in breakdown),"reopen_count":len(findings),"shown_findings":len(shown)},"reviewer_breakdown":sorted(breakdown,key=lambda b:(-b["reopen_rate"],b["reviewer"],b["content_type"])),"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No content feedback reopen rate issues found." if not findings else None}}
def build_content_feedback_reopen_rate_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "content_feedback" in s else ["content_feedback"]; mc={}; rows=[]
 if "content_feedback" in s: rows+=_load(conn,"content_feedback",s["content_feedback"],mc)
 if "content_feedback_events" in s: rows+=_load(conn,"content_feedback_events",s["content_feedback_events"],mc)
 return build_content_feedback_reopen_rate_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_content_feedback_reopen_rate_json(report:dict[str,Any])->str: return json_dumps(report)
def format_content_feedback_reopen_rate_text(report:dict[str,Any])->str:
 lines=["Content Feedback Reopen Rate",f"Generated: {report['generated_at']}",f"Totals: feedback={report['totals']['feedback_count']} reopened={report['totals']['reopen_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","feedback_id | reviewer | content_type | reason | hours"]
 for f in report["findings"]: lines.append(f"{f['feedback_id']} | {f['reviewer']} | {f['content_type']} | {f['resolution_reason']} | {f['hours_to_reopen']}")
 return "\n".join(lines)
def _load(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 select=[expr(cols,"id","feedback_id",out="feedback_id"),expr(cols,"reviewer","reviewer_id",out="reviewer"),expr(cols,"content_type",default="'unknown'",out="content_type"),expr(cols,"resolution_reason","reason",default="'unknown'",out="resolution_reason"),expr(cols,"status","event_type",default="'open'",out="status"),expr(cols,"occurred_at","updated_at","created_at",out="occurred_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY rowid")]
