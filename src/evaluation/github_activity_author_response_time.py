"""Measure response latency for GitHub activity by author and repository."""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from statistics import median
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema
ARTIFACT_TYPE="github_activity_author_response_time"; DEFAULT_SLA_HOURS=24; DEFAULT_WINDOW_DAYS=30; DEFAULT_LIMIT=50
def build_github_activity_author_response_time_report(activity_rows:list[dict[str,Any]],response_rows:list[dict[str,Any]]|None=None,*,sla_hours:int=DEFAULT_SLA_HOURS,window_days:int=DEFAULT_WINDOW_DAYS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("sla_hours",sla_hours); positive("window_days",window_days); positive("limit",limit); gen=now if isinstance(now,datetime) else datetime.now(timezone.utc); cutoff=gen-timedelta(days=window_days)
 responses=response_rows or []; findings=[]; groups=defaultdict(list)
 for a in activity_rows:
  at=dt(a.get("occurred_at") or a.get("created_at"))
  if not at or at<cutoff: continue
  key=clean(a.get("activity_id") or a.get("id")); resp=min((dt(r.get("responded_at") or r.get("created_at")) for r in responses if clean(r.get("activity_id") or r.get("github_activity_id"))==key and dt(r.get("responded_at") or r.get("created_at")) and dt(r.get("responded_at") or r.get("created_at"))>=at), default=None)
  author=clean(a.get("author"),"unknown"); repo=clean(a.get("repository") or a.get("repo"),"unknown")
  if resp:
   hours=(resp-at).total_seconds()/3600; groups[(author,repo)].append(hours)
   if hours>sla_hours: findings.append({"issue_type":"sla_breach","activity_id":key,"author":author,"repository":repo,"latency_hours":round(hours,2),"occurred_at":at.isoformat(),"responded_at":resp.isoformat()})
  else: findings.append({"issue_type":"missing_response","activity_id":key,"author":author,"repository":repo,"latency_hours":None,"occurred_at":at.isoformat(),"responded_at":None})
 breakdown=[{"author":a,"repository":r,"response_count":len(v),"median_latency_hours":round(median(v),2),"over_sla_count":sum(1 for x in v if x>sla_hours)} for (a,r),v in groups.items()]
 findings.sort(key=lambda f:(f["issue_type"],f["occurred_at"],f["activity_id"])); shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"filters":{"sla_hours":sla_hours,"window_days":window_days,"limit":limit},"totals":{"activity_count":len(activity_rows),"response_count":len(responses),"finding_count":len(findings),"shown_findings":len(shown)},"latency_buckets":_buckets(groups),"author_repo_breakdown":sorted(breakdown,key=lambda b:(b["author"],b["repository"])),"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No GitHub activity author response time findings found." if not findings else None}}
def build_github_activity_author_response_time_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "github_activity" in s else ["github_activity"]; acts=_load_acts(conn,s["github_activity"]) if "github_activity" in s else []; res=[]
 for t in ("reply_queue","proactive_actions"):
  if t in s: res+=_load_res(conn,t,s[t])
 return build_github_activity_author_response_time_report(acts,res,missing_tables=mt,**kwargs)
def format_github_activity_author_response_time_json(report:dict[str,Any])->str: return json_dumps(report)
def format_github_activity_author_response_time_text(report:dict[str,Any])->str:
 lines=["GitHub Activity Author Response Time",f"Generated: {report['generated_at']}",f"Totals: activities={report['totals']['activity_count']} responses={report['totals']['response_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","issue | activity_id | author | repository | latency_hours"]
 for f in report["findings"]: lines.append(f"{f['issue_type']} | {f['activity_id']} | {f['author']} | {f['repository']} | {f['latency_hours']}")
 return "\n".join(lines)
def _load_acts(conn:Any,cols:set[str])->list[dict[str,Any]]:
 select=[expr(cols,"id","activity_id",out="activity_id"),expr(cols,"author","actor",out="author"),expr(cols,"repository","repo",out="repository"),expr(cols,"occurred_at","created_at",out="occurred_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM github_activity ORDER BY rowid")]
def _load_res(conn:Any,t:str,cols:set[str])->list[dict[str,Any]]:
 select=[expr(cols,"activity_id","github_activity_id",out="activity_id"),expr(cols,"created_at","responded_at",out="responded_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY rowid")]
def _buckets(groups:dict[Any,list[float]])->dict[str,int]:
 vals=[x for v in groups.values() for x in v]
 return {"under_1h":sum(x<1 for x in vals),"under_24h":sum(1<=x<=24 for x in vals),"over_24h":sum(x>24 for x in vals)}
