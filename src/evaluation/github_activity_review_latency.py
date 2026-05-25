"""Measure GitHub activity review or publish latency."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="github_activity_review_latency"; DEFAULT_LIMIT=50; DEFAULT_MAX_HOURS=48.0
def build_github_activity_review_latency_report(activities:list[dict[str,Any]],decisions:list[dict[str,Any]]|None=None,*,max_hours:float=DEFAULT_MAX_HOURS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("max_hours",max_hours); positive("limit",limit); gen=now_value(now); dec={clean(d.get("activity_id") or d.get("github_activity_id") or d.get("commit_sha") or d.get("pr_number")):d for d in decisions or []}; findings=[]; stats=defaultdict(list)
    for a in activities:
        aid=clean(a.get("activity_id") or a.get("id") or a.get("sha") or a.get("pr_number")); at=dt(a.get("activity_at") or a.get("created_at") or a.get("committed_at")); d=dec.get(aid,{})
        done=dt(d.get("reviewed_at") or d.get("published_at") or d.get("decided_at")); lat=round(((done or gen)-at).total_seconds()/3600,2) if at else None
        status="reviewed" if done else ("overdue" if lat is not None and lat>max_hours else "pending")
        if lat is not None and (done or status=="overdue"):
            stats[(clean(a.get("repository"),"unknown"),clean(a.get("activity_type") or a.get("type"),"unknown"))].append(lat)
        if lat is not None and (lat>max_hours or status=="overdue"):
            findings.append({"activity_id":aid,"repository":a.get("repository"),"activity_type":a.get("activity_type") or a.get("type"),"activity_at":at.isoformat() if at else None,"reviewed_or_published_at":done.isoformat() if done else None,"latency_hours":lat,"status":status,"recommended_action":"prioritize review decision for stale GitHub activity"})
    findings.sort(key=lambda f:(-f["latency_hours"],str(f["repository"]),str(f["activity_id"])))
    shown=findings[:limit]; by={f"{k[0]}:{k[1]}":{"average_latency_hours":round(sum(v)/len(v),2),"max_latency_hours":max(v),"count":len(v)} for k,v in sorted(stats.items())}
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"max_hours":max_hours,"limit":limit},"summary":{"activity_count":len(activities),"finding_count":len(findings),"shown":len(shown),"by_repository_activity_type":by},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No GitHub activity review latency findings found.",schema_gap=bool(missing_tables or missing_columns))}
def build_github_activity_review_latency_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; acts=[]; dec=[]; table="github_activity" if "github_activity" in s else ("github_commits" if "github_commits" in s else None)
    if not table: mt.append("github_activity|github_commits")
    else:
        c=s[table]
        if not ({"activity_at","created_at","committed_at"} & c): mc[table]=["activity_at|created_at|committed_at"]
        else: acts=load_table(conn,table,c,{"activity_id":("id","activity_id","sha","pr_number"),"repository":("repository","repo"),"activity_type":("activity_type","type"),"activity_at":("activity_at","created_at","committed_at")})
    for t in ("content_reviews","publications","generated_content"):
        if t in s: dec+=load_table(conn,t,s[t],{"activity_id":("activity_id","github_activity_id","commit_sha","pr_number"),"reviewed_at":("reviewed_at","approved_at","decided_at"),"published_at":("published_at","created_at")})
    return build_github_activity_review_latency_report(acts,dec,missing_tables=mt,missing_columns=mc,**kw)
def format_github_activity_review_latency_json(r): return json_dumps(r)
def format_github_activity_review_latency_text(r):
    s=r["summary"]; lines=["GitHub Activity Review Latency",f"Generated: {r['generated_at']}",f"Totals: activities={s['activity_count']} findings={s['finding_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","activity_id | repository | status | latency_hours"]
    for f in r["findings"]: lines.append(f"{f['activity_id']} | {f['repository']} | {f['status']} | {f['latency_hours']}")
    return "\n".join(lines)
