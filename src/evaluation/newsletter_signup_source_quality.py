"""Evaluate newsletter subscriber signup source quality."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_signup_source_quality"; DEFAULT_LIMIT=50; DEFAULT_BURST_THRESHOLD=3
DISPOSABLE={"mailinator.com","10minutemail.com","guerrillamail.com","tempmail.com","yopmail.com"}
def build_newsletter_signup_source_quality_report(rows:list[dict[str,Any]],*,limit:int=DEFAULT_LIMIT,burst_threshold:int=DEFAULT_BURST_THRESHOLD,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now=None)->dict[str,Any]:
    positive("limit",limit); positive("burst_threshold",burst_threshold)
    source_stats=defaultdict(lambda:{"total":0,"missing_consent":0,"missing_campaign":0,"disposable_domain":0,"domain_bursts":0})
    domain_counts=Counter(domain(r.get("email")) for r in rows if domain(r.get("email")))
    findings=[]
    for i,r in enumerate(rows):
        src=clean(r.get("source") or r.get("signup_source"),"unknown"); created=dt(r.get("created_at")); d=domain(r.get("email")); campaign=clean(r.get("campaign"))
        source_stats[src]["total"]+=1
        reasons=[]; severity=0
        if not clean(r.get("consented_at")): reasons.append("missing_consent_timestamp"); source_stats[src]["missing_consent"]+=1; severity+=40
        if not campaign: reasons.append("missing_campaign_attribution"); source_stats[src]["missing_campaign"]+=1; severity+=20
        if d in DISPOSABLE: reasons.append("disposable_domain"); source_stats[src]["disposable_domain"]+=1; severity+=30
        if d and domain_counts[d]>=burst_threshold: reasons.append("repeated_email_domain_burst"); source_stats[src]["domain_bursts"]+=1; severity+=10+domain_counts[d]
        if reasons:
            findings.append({"subscriber_id":r.get("subscriber_id") or r.get("id") or i+1,"source":src,"email_domain":d or None,"created_at":created.isoformat() if created else None,"reasons":sorted(reasons),"severity":severity})
    breakdown=[{"source":s,**v,"issue_rate":round((v["missing_consent"]+v["missing_campaign"]+v["disposable_domain"]+v["domain_bursts"])/max(v["total"],1),4)} for s,v in source_stats.items()]
    breakdown.sort(key=lambda x:(-x["issue_rate"],x["source"]))
    findings.sort(key=lambda f:(-f["severity"], f["created_at"] or "", str(f["subscriber_id"])), reverse=False)
    findings=sorted(findings,key=lambda f:(-f["severity"], -(dt(f["created_at"]) or dt("1970-01-01T00:00:00+00:00")).timestamp(), str(f["subscriber_id"])))[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"limit":limit,"burst_threshold":burst_threshold},"totals":{"subscribers":len(rows),"sources":len(source_stats),"findings":len(findings)},"source_breakdown":breakdown,"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No newsletter signup source quality issues found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_signup_source_quality_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); mt=[] if "newsletter_subscribers" in s else ["newsletter_subscribers"]; mc={}; rows=[]
    if not mt:
        cols=s["newsletter_subscribers"]; req=["id"]; opt=["signup_source","source","campaign","consented_at","created_at","email"]
        miss=[c for c in req if c not in cols]
        if miss: mc["newsletter_subscribers"]=miss
        else: rows=load_table(conn,"newsletter_subscribers",cols,{"id":("id",),"subscriber_id":("subscriber_id","id"),"source":("signup_source","source"),"campaign":("campaign","utm_campaign"),"consented_at":("consented_at","consent_at"),"created_at":("created_at","signed_up_at"),"email":("email","email_address")})
    return build_newsletter_signup_source_quality_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_newsletter_signup_source_quality_json(r): return json_dumps(r)
def format_newsletter_signup_source_quality_text(r):
    lines=["Newsletter Signup Source Quality",f"Generated: {r['generated_at']}",f"Totals: subscribers={r['totals']['subscribers']} sources={r['totals']['sources']} findings={r['totals']['findings']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["","source | total | missing_consent | missing_campaign | disposable | bursts"]
    for b in r["source_breakdown"]: lines.append(f"{b['source']} | {b['total']} | {b['missing_consent']} | {b['missing_campaign']} | {b['disposable_domain']} | {b['domain_bursts']}")
    lines += ["","subscriber_id | source | severity | reasons"]
    for f in r["findings"]: lines.append(f"{f['subscriber_id']} | {f['source']} | {f['severity']} | {', '.join(f['reasons'])}")
    return "\n".join(lines)
