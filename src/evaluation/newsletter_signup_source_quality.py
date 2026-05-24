"""Evaluate newsletter signup sources by downstream engagement quality."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, lower, now_iso, positive, schema
ARTIFACT_TYPE="newsletter_signup_source_quality"; DEFAULT_MIN_SUBSCRIBERS=1; DEFAULT_LIMIT=50
def build_newsletter_signup_source_quality_report(subscribers:list[dict[str,Any]],events:list[dict[str,Any]]|None=None,*,min_subscribers:int=DEFAULT_MIN_SUBSCRIBERS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("min_subscribers",min_subscribers); positive("limit",limit)
 ev=defaultdict(set)
 for e in events or []:
  sid=str(e.get("subscriber_id")); typ=lower(e.get("event_type"))
  if typ in {"open","opened"}: ev[sid].add("open")
  if typ in {"click","clicked"}: ev[sid].add("click")
 groups:dict[tuple[str,str],list[dict[str,Any]]]=defaultdict(list)
 for s in subscribers: groups[(clean(s.get("source"),"unknown"),clean(s.get("campaign"),""))].append(s)
 rows=[]
 for (source,campaign),items in groups.items():
  total=len(items)
  if total<min_subscribers: continue
  confirmed=sum(1 for x in items if lower(x.get("status")) in {"confirmed","active","subscribed"})
  bounced=sum(1 for x in items if "bounce" in lower(x.get("status")))
  unsub=sum(1 for x in items if lower(x.get("status")) in {"unsubscribed","cancelled"})
  opens=sum(1 for x in items if "open" in ev[str(x.get("subscriber_id"))])
  clicks=sum(1 for x in items if "click" in ev[str(x.get("subscriber_id"))])
  open_rate=round(opens/total,4); click_rate=round(clicks/total,4)
  score=round((confirmed/total)*50 + open_rate*25 + click_rate*25 - (bounced/total)*30 - (unsub/total)*20,2)
  rows.append({"source":source,"campaign":campaign or None,"subscriber_count":total,"confirmed_count":confirmed,"bounced_count":bounced,"unsubscribed_count":unsub,"first_open_rate":open_rate,"first_click_rate":click_rate,"quality_score":score})
 rows.sort(key=lambda r:(r["quality_score"],-r["subscriber_count"],r["source"],r["campaign"] or ""))
 shown=rows[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"min_subscribers":min_subscribers,"limit":limit},"summary":{"source_count":len(rows),"subscriber_count":len(subscribers),"shown_count":len(shown)},"sources":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_newsletter_signup_source_quality_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); miss=[] if "newsletter_subscribers" in s else ["newsletter_subscribers"]; mc={}
 subs=_subs(conn,s,mc) if "newsletter_subscribers" in s else []; events=[]
 for t in ("newsletter_events","newsletter_provider_events"):
  if t in s: events+=_events(conn,t,s[t],mc)
 return build_newsletter_signup_source_quality_report(subs,events,missing_tables=miss,missing_columns=mc,**kwargs)
def format_newsletter_signup_source_quality_json(report:dict[str,Any])->str: return json_dumps(report)
def format_newsletter_signup_source_quality_text(report:dict[str,Any])->str:
 lines=["Newsletter Signup Source Quality",f"Generated: {report['generated_at']}",f"Totals: sources={report['summary']['source_count']} subscribers={report['summary']['subscriber_count']} shown={report['summary']['shown_count']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["sources"]: lines.append("No signup source quality rows found."); return "\n".join(lines)
 lines+=["","source | campaign | subscribers | confirmed | bounced | unsubscribed | first_open_rate | first_click_rate | quality_score"]
 for r in report["sources"]: lines.append(f"{r['source']} | {r['campaign'] or '-'} | {r['subscriber_count']} | {r['confirmed_count']} | {r['bounced_count']} | {r['unsubscribed_count']} | {r['first_open_rate']} | {r['first_click_rate']} | {r['quality_score']}")
 return "\n".join(lines)
def _subs(conn:Any,s:dict[str,set[str]],mc:dict[str,list[str]])->list[dict[str,Any]]:
 cols=s["newsletter_subscribers"]; gid=next((c for c in ("id","subscriber_id") if c in cols),None)
 if not gid: mc["newsletter_subscribers"]=["id"]; return []
 select=[f"{gid} AS subscriber_id",expr(cols,"source","signup_source",default="'unknown'",out="source"),expr(cols,"campaign","utm_campaign",default="NULL",out="campaign"),expr(cols,"status",default="'unknown'",out="status")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_subscribers ORDER BY {gid}")]
def _events(conn:Any,t:str,cols:set[str],mc:dict[str,list[str]])->list[dict[str,Any]]:
 sid=next((c for c in ("subscriber_id","newsletter_subscriber_id") if c in cols),None); typ=next((c for c in ("event_type","type","name") if c in cols),None)
 if not sid or not typ: mc[t]=sorted({"subscriber_id","event_type"}-cols); return []
 return [dict(r) for r in conn.execute(f"SELECT {sid} AS subscriber_id,{typ} AS event_type FROM {t} ORDER BY rowid")]
