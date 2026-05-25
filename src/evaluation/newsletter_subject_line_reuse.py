"""Detect repeated or near-repeated newsletter subject lines."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "newsletter_subject_line_reuse"; DEFAULT_LOOKBACK_DAYS = 90; DEFAULT_SIMILARITY = 0.75

def build_newsletter_subject_line_reuse_report(rows:list[dict[str,Any]],*,lookback_days:int=DEFAULT_LOOKBACK_DAYS,similarity_threshold:float=DEFAULT_SIMILARITY,missing_tables=None,missing_columns=None,now=None)->dict[str,Any]:
    positive("lookback_days",lookback_days); bounded_share("similarity_threshold",similarity_threshold); generated=now_value(now); cutoff=generated-timedelta(days=lookback_days); groups=[]; used:set[int]=set()
    prepared=[]
    for i,row in enumerate(rows):
        sent=dt(row.get("send_date") or row.get("sent_at") or row.get("created_at"))
        if sent and sent < cutoff: continue
        subject=clean(row.get("subject") or row.get("subject_line") or row.get("title"))
        prepared.append((i,row,sent,subject,tokens(subject),_norm(subject)))
    for i,(idx,row,sent,subject,tok,norm) in enumerate(prepared):
        if idx in used or not subject: continue
        members=[(idx,row,sent,subject,1.0)]
        for idx2,row2,sent2,subject2,tok2,norm2 in prepared[i+1:]:
            sim=1.0 if norm and norm==norm2 else jaccard(tok,tok2)
            if sim>=similarity_threshold: members.append((idx2,row2,sent2,subject2,sim))
        if len(members)>1:
            for m in members: used.add(m[0])
            newest=max((m[2] for m in members if m[2]), default=None)
            groups.append({"normalized_subject":norm,"reuse_count":len(members),"most_recent_reuse_age_days":(generated-newest).days if newest else None,"max_similarity":max(m[4] for m in members),"issues":[{"issue_id":m[1].get("issue_id") or m[1].get("id"),"subject":m[3],"send_date":m[2].isoformat() if m[2] else clean(m[1].get("send_date") or m[1].get("sent_at"))} for m in members]})
    groups.sort(key=lambda g:(-g["reuse_count"],g["most_recent_reuse_age_days"] if g["most_recent_reuse_age_days"] is not None else 999999,g["normalized_subject"]))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":generated.isoformat(),"filters":{"lookback_days":lookback_days,"similarity_threshold":similarity_threshold},"totals":{"issues":len(rows),"groups":len(groups)},"groups":groups,"findings":groups,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(groups,"No newsletter subject line reuse found.",schema_gap=bool(missing_tables or missing_columns))}

def build_newsletter_subject_line_reuse_report_from_db(db_or_conn:Any,**kw)->dict[str,Any]:
    conn=connection(db_or_conn); s=schema(conn); table="newsletter_issues" if "newsletter_issues" in s else "newsletter_sends" if "newsletter_sends" in s else None
    if not table: return build_newsletter_subject_line_reuse_report([],missing_tables=["newsletter_issues"],**kw)
    rows=load_table(conn,table,s[table],{"issue_id":("issue_id","id","newsletter_send_id"),"subject":("subject","subject_line","title"),"sent_at":("sent_at","send_date","created_at")})
    return build_newsletter_subject_line_reuse_report(rows,**kw)

def format_newsletter_subject_line_reuse_json(r): return json_dumps(r)
def format_newsletter_subject_line_reuse_text(r):
    lines=["Newsletter Subject Line Reuse",f"Generated: {r['generated_at']}",f"Totals: issues={r['totals']['issues']} groups={r['totals']['groups']}"]
    if not r["groups"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","normalized_subject | reuse_count | most_recent_reuse_age_days"]
    for g in r["groups"]: lines.append(f"{g['normalized_subject']} | {g['reuse_count']} | {g['most_recent_reuse_age_days']}")
    return "\n".join(lines)
def _norm(value:Any)->str: return " ".join(sorted(tokens(value)))
