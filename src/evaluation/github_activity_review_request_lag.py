"""Find GitHub review requests without timely follow-up."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Any
from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema, dt
ARTIFACT_TYPE="github_activity_review_request_lag"; DEFAULT_LIMIT=50; DEFAULT_SLA_HOURS=48
FOLLOWUPS={"review_submitted","pull_request_review","commented","pull_request_comment","merged","closed"}
def build_github_activity_review_request_lag_report(rows:list[dict[str,Any]],*,sla_hours:int=DEFAULT_SLA_HOURS,limit:int=DEFAULT_LIMIT,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None,now:datetime|None=None)->dict[str,Any]:
    positive("sla_hours",sla_hours); positive("limit",limit); gen=now or datetime.now(timezone.utc); findings=[]; requests=[]
    for r in rows:
        if clean(r.get("event_type")).lower()=="review_requested": requests.append(r)
    for req in requests:
        requested=dt(req.get("created_at")) or gen; deadline=requested+timedelta(hours=sla_hours); matched=False
        for r in rows:
            if clean(r.get("repository"))==clean(req.get("repository")) and clean(r.get("pr_number"))==clean(req.get("pr_number")) and clean(r.get("event_type")).lower() in FOLLOWUPS:
                t=dt(r.get("created_at"))
                if t and requested<=t<=deadline: matched=True; break
        if not matched and deadline<=gen:
            findings.append({"repository":clean(req.get("repository")),"pr_number":clean(req.get("pr_number")),"requested_reviewer":clean(req.get("requested_reviewer") or req.get("actor")),"requested_at":requested.isoformat(),"issue_type":"review_request_lag"})
    findings.sort(key=lambda r:(r["repository"],r["pr_number"],r["requested_reviewer"],r["requested_at"])); shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(gen),"thresholds":{"sla_hours":sla_hours,"limit":limit},"summary":{"event_count":len(rows),"review_request_count":len(requests),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}
def build_github_activity_review_request_lag_report_from_db(db_or_conn:Any,**kwargs):
    conn=connection(db_or_conn); s=schema(conn); table="github_activity"; mt=[] if table in s else [table]; mc={}; rows=[]
    if table in s:
        c=s[table]; req={"event_type","created_at"}
        if not req.issubset(c): mc[table]=sorted(req-c)
        else:
            select=[expr(c,"repository","repo",default="NULL",out="repository"),expr(c,"pr_number","number",default="NULL",out="pr_number"),"event_type","created_at",expr(c,"requested_reviewer","reviewer",default="NULL",out="requested_reviewer"),expr(c,"actor",default="NULL",out="actor")]
            rows=[dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY created_at, rowid")]
    return build_github_activity_review_request_lag_report(rows,missing_tables=mt,missing_columns=mc,**kwargs)
def format_github_activity_review_request_lag_json(report): return json_dumps(report)
def format_github_activity_review_request_lag_text(report):
    lines=["GitHub Activity Review Request Lag",f"Generated: {report['generated_at']}",f"Totals: events={report['summary']['event_count']} requests={report['summary']['review_request_count']} findings={report['summary']['finding_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No GitHub review request lag found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- {r['repository']}#{r['pr_number']} reviewer={r['requested_reviewer']} requested={r['requested_at']}")
    return "\n".join(lines)
