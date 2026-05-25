"""Report dead, error, or unknown newsletter links that still receive clicks."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="newsletter_link_dead_clicks"; DEFAULT_MIN_CLICKS=1
def build_newsletter_link_dead_clicks_report(click_rows:list[dict[str,Any]],inventory_rows:list[dict[str,Any]],*,since:str|None=None,min_clicks:int=DEFAULT_MIN_CLICKS,missing_tables=None,missing_columns=None,now=None):
    positive("min_clicks",min_clicks); since_dt=dt(since); inv={(clean(r.get("issue_id")),clean(r.get("url"))):lower(r.get("status") or r.get("link_status") or "ok") for r in inventory_rows}; grouped={}
    for row in click_rows:
        clicked=dt(row.get("last_clicked_at") or row.get("clicked_at") or row.get("date"))
        if since_dt and clicked and clicked<since_dt: continue
        key=(clean(row.get("issue_id")),clean(row.get("url"))); item=grouped.setdefault(key,{"issue_id":key[0] or None,"url":key[1],"click_count":0,"last_clicked_at":None})
        item["click_count"]+=to_int(row.get("click_count") or row.get("clicks") or 1,1)
        if clicked and (not item["last_clicked_at"] or clicked>dt(item["last_clicked_at"])): item["last_clicked_at"]=clicked.isoformat()
    findings=[]
    for key,item in grouped.items():
        status=inv.get(key,"unknown")
        if item["click_count"]>=min_clicks and (status=="unknown" or any(part in status for part in ("dead","error","404","failed"))):
            item={**item,"status":status,"recommended_action":"restore or replace destination" if status!="unknown" else "add link to inventory or verify tracking"}; findings.append(item)
    findings.sort(key=lambda f:(-f["click_count"],f["status"],str(f["issue_id"]),f["url"]))
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":now_iso(now),"filters":{"since":since,"min_clicks":min_clicks},"totals":{"click_rows":len(click_rows),"inventory_rows":len(inventory_rows),"findings":len(findings)},"findings":findings,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No dead newsletter links with clicks found.",schema_gap=bool(missing_tables or missing_columns))}
def build_newsletter_link_dead_clicks_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; clicks=[]; inv=[]; ctable="newsletter_link_clicks" if "newsletter_link_clicks" in s else "newsletter_clicks" if "newsletter_clicks" in s else None
    if not ctable: mt.append("newsletter_link_clicks")
    else: clicks=load_table(conn,ctable,s[ctable],{"issue_id":("issue_id","newsletter_issue_id"),"url":("url","link_url"),"click_count":("click_count","clicks"),"clicked_at":("clicked_at","last_clicked_at","date")})
    itable="newsletter_link_inventory" if "newsletter_link_inventory" in s else "newsletter_links" if "newsletter_links" in s else None
    if itable: inv=load_table(conn,itable,s[itable],{"issue_id":("issue_id","newsletter_issue_id"),"url":("url","link_url"),"status":("status","link_status")})
    return build_newsletter_link_dead_clicks_report(clicks,inv,missing_tables=mt,**kw)
def format_newsletter_link_dead_clicks_json(r): return json_dumps(r)
def format_newsletter_link_dead_clicks_text(r):
    lines=["Newsletter Link Dead Clicks",f"Generated: {r['generated_at']}",f"Totals: clicks={r['totals']['click_rows']} findings={r['totals']['findings']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","issue_id | status | clicks | last_clicked_at | url"]
    for f in r["findings"]: lines.append(f"{f['issue_id']} | {f['status']} | {f['click_count']} | {f['last_clicked_at']} | {f['url']}")
    return "\n".join(lines)
