"""Evaluate publish queue utilization against configured schedule windows."""
from __future__ import annotations
from collections import Counter,defaultdict
from datetime import datetime,timezone
from typing import Any
from ._report_utils import clean,connection,dt,expr,json_dumps,now_iso,positive,schema,to_int
ARTIFACT_TYPE="publish_queue_schedule_window_utilization"; DEFAULT_UNDERUSED=0.5; DEFAULT_OVERFILLED=1.0; DEFAULT_LIMIT=50
DAYS=("monday","tuesday","wednesday","thursday","friday","saturday","sunday")
def build_publish_queue_schedule_window_utilization_report(queue_rows:list[dict[str,Any]],window_rows:list[dict[str,Any]]|None=None,*,timezone:str="UTC",underused_threshold:float=DEFAULT_UNDERUSED,overfilled_threshold:float=DEFAULT_OVERFILLED,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 if underused_threshold<0 or overfilled_threshold<0: raise ValueError("thresholds must be non-negative")
 positive("limit",limit); windows=[_window(w) for w in (window_rows or [])]; counts=Counter(); outside=[]; findings=[]
 for q in queue_rows:
  ts=dt(q.get("scheduled_at") or q.get("published_at")); platform=clean(q.get("platform"),"unknown")
  match=_match(ts,platform,windows)
  if match: counts[(match["platform"],match["day"],match["start"],match["end"])] += 1
  else: outside.append({"type":"outside_window","platform":platform,"content_id":clean(q.get("content_id") or q.get("id"),"unknown"),"scheduled_at":ts.isoformat() if ts else None})
 for w in windows:
  key=(w["platform"],w["day"],w["start"],w["end"]); cap=w["capacity"]; count=counts[key]; util=round(count/cap,4) if cap else 0
  row={"platform":w["platform"],"day":w["day"],"window":f"{w['start']}-{w['end']}","capacity":cap,"item_count":count,"utilization":util}
  if count and util>overfilled_threshold: findings.append({"type":"overfilled",**row})
  elif util<underused_threshold: findings.append({"type":"underused",**row})
 findings+=outside; findings.sort(key=lambda f:(f["type"],f.get("platform",""),f.get("day",""),f.get("window",""),f.get("content_id","")))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"timezone":timezone,"underused_threshold":underused_threshold,"overfilled_threshold":overfilled_threshold,"limit":limit},"totals":{"queue_item_count":len(queue_rows),"window_count":len(windows),"finding_count":len(findings),"shown_findings":len(shown)},"window_utilization":[{"platform":p,"day":d,"window":f"{s}-{e}","item_count":c} for (p,d,s,e),c in sorted(counts.items())],"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No publish queue schedule window utilization issues found." if not findings else None}}
def build_publish_queue_schedule_window_utilization_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); mt=[] if "publish_queue" in s else ["publish_queue"]; mc={}; windows=[]
 for t in ("posting_windows","publish_windows"):
  if t in s: windows+=_windows(conn,t,s[t],mc)
 return build_publish_queue_schedule_window_utilization_report(_queue(conn,s["publish_queue"],mc) if "publish_queue" in s else [],windows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_publish_queue_schedule_window_utilization_json(report:dict[str,Any])->str: return json_dumps(report)
def format_publish_queue_schedule_window_utilization_text(report:dict[str,Any])->str:
 lines=["Publish Queue Schedule Window Utilization",f"Generated: {report['generated_at']}",f"Totals: queue_items={report['totals']['queue_item_count']} windows={report['totals']['window_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No findings."); return "\n".join(lines)
 lines+=["","type | platform | day | window | utilization | content_id"]
 for f in report["findings"]: lines.append(f"{f['type']} | {f.get('platform','-')} | {f.get('day','-')} | {f.get('window','-')} | {f.get('utilization','-')} | {f.get('content_id','-')}")
 return "\n".join(lines)
def _queue(conn:Any,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 select=[expr(cols,"id",out="id"),expr(cols,"content_id",out="content_id"),expr(cols,"platform",default="'unknown'",out="platform"),expr(cols,"scheduled_at","publish_at","published_at",out="scheduled_at")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue ORDER BY rowid")]
def _windows(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 select=[expr(cols,"platform",default="'unknown'",out="platform"),expr(cols,"day","weekday",default="'monday'",out="day"),expr(cols,"start_time","start",default="'00:00'",out="start_time"),expr(cols,"end_time","end",default="'23:59'",out="end_time"),expr(cols,"capacity","max_items",default="1",out="capacity")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {t} ORDER BY rowid")]
def _window(w:dict[str,Any])->dict[str,Any]: return {"platform":clean(w.get("platform"),"unknown"),"day":clean(w.get("day"),"monday").lower(),"start":clean(w.get("start_time"),"00:00"),"end":clean(w.get("end_time"),"23:59"),"capacity":to_int(w.get("capacity")) or 1}
def _match(ts:datetime|None,platform:str,windows:list[dict[str,Any]])->dict[str,Any]|None:
 if not ts: return None
 day=DAYS[ts.weekday()]; hm=ts.strftime("%H:%M")
 return next((w for w in windows if w["platform"]==platform and w["day"]==day and w["start"]<=hm<=w["end"]),None)
