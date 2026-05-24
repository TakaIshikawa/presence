"""Find newsletter subject winners selected too late or not used."""
from __future__ import annotations
from collections import Counter, defaultdict
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_subject_winner_lag"; DEFAULT_LIMIT=50; DEFAULT_MAX_LAG_HOURS=12.0
def build_newsletter_subject_winner_lag_report(candidates:list[dict[str,Any]],issues:list[dict[str,Any]]|None=None,*,max_lag_hours:float=DEFAULT_MAX_LAG_HOURS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("max_lag_hours",max_lag_hours); positive("limit",limit); by_issue=defaultdict(list); info={clean(i.get("issue_id") or i.get("id")):i for i in issues or []}
    for c in candidates: by_issue[clean(c.get("issue_id") or c.get("newsletter_issue_id"))].append(c)
    findings=[]; counts=Counter()
    for iid,rows in by_issue.items():
        winner=max(rows,key=lambda r:(1 if str(r.get("is_winner")).lower() in {"1","true","yes"} else 0,to_float(r.get("score"))))
        selected=next((r for r in rows if str(r.get("selected")).lower() in {"1","true","yes"} or clean(r.get("subject"))==clean(info.get(iid,{}).get("selected_subject"))),None)
        send=dt(info.get(iid,{}).get("send_at") or info.get(iid,{}).get("sent_at") or winner.get("send_at")); sel_at=dt((selected or {}).get("selected_at") or info.get(iid,{}).get("selected_at"))
        lag=round((send-sel_at).total_seconds()/3600,2) if send and sel_at else None
        status=None
        if not selected or clean(selected.get("subject"))!=clean(winner.get("subject")): status="winner_not_used"
        elif lag is None or lag>max_lag_hours: status="late_selection"
        if status:
            counts[status]+=1; findings.append({"issue_id":iid,"selected_subject":(selected or {}).get("subject") or info.get(iid,{}).get("selected_subject"),"winning_subject":winner.get("subject"),"winner_score":to_float(winner.get("score")),"selected_at":sel_at.isoformat() if sel_at else None,"send_at":send.isoformat() if send else None,"lag_hours":lag,"status":status,"recommended_action":"select winning subject earlier before send" if status=="late_selection" else "review why top subject was not selected"})
    findings.sort(key=lambda f:(f["status"]!="winner_not_used",-(f["lag_hours"] or 9999),str(f["issue_id"])))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"max_lag_hours":max_lag_hours,"limit":limit},"summary":{"issue_count":len(by_issue),"late_selection":counts["late_selection"],"winner_not_used":counts["winner_not_used"],"finding_count":len(findings),"shown":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No newsletter subject winner lag found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_subject_winner_lag_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; cand=[]; issues=[]
    if "newsletter_subject_candidates" not in s: mt.append("newsletter_subject_candidates")
    else:
        c=s["newsletter_subject_candidates"]
        if not ({"issue_id","newsletter_issue_id"} & c): mc.setdefault("newsletter_subject_candidates",[]).append("issue_id|newsletter_issue_id")
        if "subject" not in c: mc.setdefault("newsletter_subject_candidates",[]).append("subject")
        if not mc: cand=load_table(conn,"newsletter_subject_candidates",c,{"issue_id":("issue_id","newsletter_issue_id"),"subject":("subject",),"score":("score","winner_score"),"is_winner":("is_winner","winner"),"selected":("selected","is_selected"),"selected_at":("selected_at","created_at"),"send_at":("send_at",)})
    for t in ("newsletter_issues","newsletters"):
        if t in s: issues+=load_table(conn,t,s[t],{"issue_id":("id","issue_id"),"selected_subject":("selected_subject","subject"),"selected_at":("selected_at",),"send_at":("send_at","sent_at")})
    return build_newsletter_subject_winner_lag_report(cand,issues,missing_tables=mt,missing_columns=mc,**kw)
def format_newsletter_subject_winner_lag_json(r): return json_dumps(r)
def format_newsletter_subject_winner_lag_text(r):
    s=r["summary"]; lines=["Newsletter Subject Winner Lag",f"Generated: {r['generated_at']}",f"Totals: issues={s['issue_count']} late={s['late_selection']} not_used={s['winner_not_used']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","issue_id | status | lag_hours | winning_subject"]
    for f in r["findings"]: lines.append(f"{f['issue_id']} | {f['status']} | {f['lag_hours']} | {f['winning_subject']}")
    return "\n".join(lines)
