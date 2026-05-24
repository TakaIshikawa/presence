"""Find GitHub activity mentions that still need follow-up."""
from __future__ import annotations
from collections import Counter
from datetime import datetime, timezone
from typing import Any
from ._report_utils import clean, connection, dt, expr, json_dumps, lower, now_iso, positive, schema
ARTIFACT_TYPE="github_activity_stale_mentions"; DEFAULT_STALE_DAYS=7; DEFAULT_LIMIT=50
DONE={"done","completed","closed","published","dismissed"}

def build_github_activity_stale_mentions_report(rows:list[dict[str,Any]],*,stale_days:int=DEFAULT_STALE_DAYS,limit:int=DEFAULT_LIMIT,now:datetime|None=None,missing_tables:list[str]|None=None,missing_columns:dict[str,list[str]]|None=None)->dict[str,Any]:
    positive("stale_days",stale_days); positive("limit",limit); ref=now or datetime.now(timezone.utc)
    findings=[]
    for r in rows:
        mentioned=dt(r.get("mentioned_at")); status=lower(r.get("followup_status"),"open")
        age=((ref.astimezone(timezone.utc)-mentioned).days if mentioned else None)
        if age is None or age<stale_days or status in DONE: continue
        findings.append({"github_activity_id":str(r.get("github_activity_id")),"repo":clean(r.get("repo")) or None,"issue_or_pr_number":clean(r.get("issue_or_pr_number")) or None,"mention_type":lower(r.get("mention_type"),"mention"),"mentioned_at":mentioned.isoformat(),"linked_content_id":clean(r.get("linked_content_id")) or None,"followup_status":status,"age_days":age,"owner":clean(r.get("owner"),"unassigned")})
    findings.sort(key=lambda x:(-x["age_days"],x["repo"] or "",x["github_activity_id"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"thresholds":{"stale_days":stale_days,"limit":limit},"summary":{"activity_count":len(rows),"stale_count":len(findings),"shown_count":len(shown),"by_repo":dict(sorted(Counter(f["repo"] or "unknown" for f in findings).items())),"by_mention_type":dict(sorted(Counter(f["mention_type"] for f in findings).items())),"by_followup_status":dict(sorted(Counter(f["followup_status"] for f in findings).items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())}}

def build_github_activity_stale_mentions_report_from_db(db_or_conn:Any,**kwargs:Any)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); missing=[] if "github_activity" in s else ["github_activity"]; miss:{}={}
    rows=_load(conn,s,miss) if "github_activity" in s else []
    return build_github_activity_stale_mentions_report(rows,missing_tables=missing,missing_columns=miss,**kwargs)

def format_github_activity_stale_mentions_json(report:dict[str,Any])->str: return json_dumps(report)
def format_github_activity_stale_mentions_text(report:dict[str,Any])->str:
    lines=["GitHub Activity Stale Mentions",f"Generated: {report['generated_at']}",f"Totals: activities={report['summary']['activity_count']} stale={report['summary']['stale_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: "+", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No stale GitHub mentions found."); return "\n".join(lines)
    lines+=["","github_activity_id | repo | number | mention_type | mentioned_at | linked_content_id | followup_status | age_days | owner"]
    for f in report["findings"]: lines.append(f"{f['github_activity_id']} | {f['repo'] or '-'} | {f['issue_or_pr_number'] or '-'} | {f['mention_type']} | {f['mentioned_at']} | {f['linked_content_id'] or '-'} | {f['followup_status']} | {f['age_days']} | {f['owner']}")
    return "\n".join(lines)

def _load(conn:Any,s:dict[str,set[str]],missing:dict[str,list[str]])->list[dict[str,Any]]:
    cols=s["github_activity"]; gid=next((c for c in ("id","github_activity_id") if c in cols),None)
    if not gid: missing["github_activity"]=["id"]; return []
    select=[f"ga.{gid} AS github_activity_id",expr(cols,"repo","repository",default="NULL",alias="ga",out="repo"),expr(cols,"number","issue_number","pr_number",default="NULL",alias="ga",out="issue_or_pr_number"),expr(cols,"mention_type","activity_type","type",default="'mention'",alias="ga",out="mention_type"),expr(cols,"mentioned_at","created_at","updated_at",default="NULL",alias="ga",out="mentioned_at"),expr(cols,"owner","assignee",default="NULL",alias="ga",out="owner")]
    joins=[]; linked="NULL AS linked_content_id"; status="'open' AS followup_status"
    if "content_ideas" in s and "github_activity_id" in s["content_ideas"]:
        joins.append("LEFT JOIN content_ideas ci ON ci.github_activity_id = ga."+gid); linked=expr(s["content_ideas"],"content_id","id",default="NULL",alias="ci",out="linked_content_id"); status=expr(s["content_ideas"],"status",default="'open'",alias="ci",out="followup_status")
    if "proactive_actions" in s and "github_activity_id" in s["proactive_actions"]:
        joins.append("LEFT JOIN proactive_actions pa ON pa.github_activity_id = ga."+gid); status=expr(s["proactive_actions"],"status",default=status.split(" AS ")[0],alias="pa",out="followup_status")
    return [dict(r) for r in conn.execute(f"SELECT {', '.join(select+[linked,status])} FROM github_activity ga {' '.join(joins)} ORDER BY ga.rowid")]
