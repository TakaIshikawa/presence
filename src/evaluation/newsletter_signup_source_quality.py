"""Evaluate newsletter signup source quality and attribution gaps."""
from __future__ import annotations
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any
from ._report_utils import clean, connection, dt, expr, json_dumps, lower, now_iso, positive, schema
ARTIFACT_TYPE="newsletter_signup_source_quality"; DEFAULT_LIMIT=50; DEFAULT_BURST_THRESHOLD=3; DEFAULT_MIN_SUBSCRIBERS=1
DISPOSABLE={"mailinator.com","10minutemail.com","guerrillamail.com","tempmail.com","trashmail.com","yopmail.com"}
def build_newsletter_signup_source_quality_report(subscribers:list[dict[str,Any]],events:list[dict[str,Any]]|None=None,*,burst_threshold:int=DEFAULT_BURST_THRESHOLD,min_subscribers:int=DEFAULT_MIN_SUBSCRIBERS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:Any=None)->dict[str,Any]:
 positive("burst_threshold",burst_threshold); positive("min_subscribers",min_subscribers); positive("limit",limit)
 groups:dict[str,list[dict[str,Any]]]=defaultdict(list)
 for row in subscribers: groups[clean(row.get("source") or row.get("signup_source"),"unknown")].append(row)
 source_breakdown=[]; findings=[]
 for source,items in groups.items():
  if len(items)<min_subscribers: continue
  missing_consent=sum(1 for r in items if not clean(r.get("consented_at") or r.get("consent_timestamp")))
  missing_campaign=sum(1 for r in items if not clean(r.get("campaign") or r.get("utm_campaign")))
  domains=[_domain(r.get("email")) for r in items]; counts=Counter(d for d in domains if d)
  disposable=sum(c for d,c in counts.items() if d in DISPOSABLE)
  bursts={d:c for d,c in counts.items() if c>=burst_threshold}
  source_breakdown.append({"source":source,"subscriber_count":len(items),"missing_consent_count":missing_consent,"missing_campaign_count":missing_campaign,"disposable_domain_count":disposable,"burst_domains":dict(sorted(bursts.items()))})
  if missing_consent: findings.append(_finding(source,"missing_consent",missing_consent,items))
  if missing_campaign: findings.append(_finding(source,"missing_campaign",missing_campaign,items))
  if disposable: findings.append(_finding(source,"disposable_domain",disposable,items))
  for d,c in sorted(bursts.items()): findings.append(_finding(source,"domain_burst",c,items,domain=d))
 findings.sort(key=lambda f:(-_severity(f["issue_type"],f["count"])[0],-_severity(f["issue_type"],f["count"])[1], f["latest_created_at"] or "", f["source"], f.get("domain") or ""))
 source_breakdown.sort(key=lambda r:(-r["subscriber_count"],r["source"]))
 shown=findings[:limit]
 return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"min_subscribers":min_subscribers,"burst_threshold":burst_threshold,"limit":limit},"thresholds":{"min_subscribers":min_subscribers,"burst_threshold":burst_threshold,"limit":limit},"totals":{"subscriber_count":len(subscribers),"source_count":len(source_breakdown),"finding_count":len(findings),"shown_findings":len(shown)},"summary":{"source_count":len(source_breakdown),"subscriber_count":len(subscribers),"shown_count":len(shown)},"source_breakdown":source_breakdown,"sources":source_breakdown,"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":{"is_empty":not findings,"message":"No newsletter signup source quality issues found." if not findings else None}}
def build_newsletter_signup_source_quality_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
 conn=connection(db_or_conn); s=schema(conn); miss=[] if "newsletter_subscribers" in s else ["newsletter_subscribers"]; mc={}
 subs=_subs(conn,s,mc) if "newsletter_subscribers" in s else []
 return build_newsletter_signup_source_quality_report(subs,missing_tables=miss,missing_columns=mc,**kwargs)
def format_newsletter_signup_source_quality_json(report:dict[str,Any])->str: return json_dumps(report)
def format_newsletter_signup_source_quality_text(report:dict[str,Any])->str:
 lines=["Newsletter Signup Source Quality",f"Generated: {report['generated_at']}",f"Totals: sources={report['totals']['source_count']} subscribers={report['totals']['subscriber_count']} findings={report['totals']['finding_count']} shown={report['totals']['shown_findings']}"]
 if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
 if not report["findings"]: lines.append(report["empty_state"]["message"] or "No signup source quality rows found."); return "\n".join(lines)
 lines+=["","issue | source | count | domain | latest_created_at"]
 for r in report["findings"]: lines.append(f"{r['issue_type']} | {r['source']} | {r['count']} | {r.get('domain') or '-'} | {r.get('latest_created_at') or '-'}")
 return "\n".join(lines)
def _subs(conn:Any,s:dict[str,set[str]],mc:dict[str,list[str]])->list[dict[str,Any]]:
 cols=s["newsletter_subscribers"]; gid=next((c for c in ("id","subscriber_id") if c in cols),None)
 if not gid: mc["newsletter_subscribers"]=["id"]; return []
 needed={"email","consented_at","campaign","created_at"}; missing=sorted(c for c in needed if c not in cols and not (c=="campaign" and "utm_campaign" in cols))
 if missing: mc["newsletter_subscribers"]=missing
 select=[f"{gid} AS subscriber_id",expr(cols,"source","signup_source",default="'unknown'",out="source"),expr(cols,"campaign","utm_campaign",default="NULL",out="campaign"),expr(cols,"consented_at","consent_timestamp",default="NULL",out="consented_at"),expr(cols,"created_at","subscribed_at",default="NULL",out="created_at"),expr(cols,"email",default="NULL",out="email")]
 return [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_subscribers ORDER BY {gid}")]
def _domain(email:Any)->str:
 text=lower(email); return text.rsplit("@",1)[-1] if "@" in text else ""
def _finding(source:str,issue:str,count:int,items:list[dict[str,Any]],domain:str|None=None)->dict[str,Any]:
 latest=max((dt(r.get("created_at")) for r in items if dt(r.get("created_at"))), default=None)
 return {"issue_type":issue,"source":source,"count":count,"domain":domain,"latest_created_at":latest.isoformat() if latest else None}
def _severity(issue:str,count:int)->tuple[int,int]:
 rank={"disposable_domain":4,"domain_burst":3,"missing_consent":2,"missing_campaign":1}.get(issue,0)
 return (rank,count)
