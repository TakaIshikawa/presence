"""Flag scheduled content colliding with blackout windows."""
from __future__ import annotations
from collections import Counter
from datetime import timedelta
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="content_calendar_blackout_collision"; DEFAULT_LOOKAHEAD_DAYS=30; DEFAULT_LIMIT=50
def build_content_calendar_blackout_collision_report(items:list[dict[str,Any]],windows:list[dict[str,Any]]|None=None,holidays:list[dict[str,Any]]|None=None,incidents:list[dict[str,Any]]|None=None,*,lookahead_days:int=DEFAULT_LOOKAHEAD_DAYS,limit:int=DEFAULT_LIMIT,missing_tables=None,missing_columns=None,now=None):
    positive("lookahead_days",lookahead_days); positive("limit",limit); gen=now_value(now); end=gen+timedelta(days=lookahead_days); findings=[]
    for it in items:
        planned=dt(it.get("planned_at") or it.get("publish_at") or it.get("scheduled_at"))
        if not planned or planned<gen or planned>end: continue
        for w in windows or []:
            if _matches(it,w) and _between(planned,dt(w.get("starts_at") or w.get("start_at")),dt(w.get("ends_at") or w.get("end_at"))):
                findings.append(_finding(it,planned,"blackout_window",w,clean(w.get("severity"),"high")))
        for h in holidays or []:
            if str(planned.date())==clean(h.get("date") or h.get("holiday_date")):
                findings.append(_finding(it,planned,"holiday",h,clean(h.get("severity"),"medium")))
        for inc in incidents or []:
            if _matches(it,inc) and _between(planned,dt(inc.get("started_at")),dt(inc.get("ended_at") or inc.get("resolved_at"))):
                findings.append(_finding(it,planned,"provider_incident",inc,clean(inc.get("severity"),"high")))
    findings.sort(key=lambda f:({"high":0,"medium":1,"low":2}.get(f["severity"],3),f["planned_at"],str(f["calendar_item_id"])))
    shown=findings[:limit]; by=Counter(f["collision_type"] for f in findings)
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"lookahead_days":lookahead_days,"limit":limit},"summary":{"calendar_item_count":len(items),"collision_count":len(findings),"shown":len(shown),"by_collision_type":dict(sorted(by.items()))},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items())},"empty_state":empty_state(findings,"No content calendar blackout collisions found.",schema_gap=bool(missing_tables or missing_columns))}
def _matches(item,block): return clean(block.get("channel") or block.get("platform")) in ("",clean(item.get("channel") or item.get("platform"))) 
def _between(ts,start,end): return bool(start and ts>=start and (end is None or ts<=end))
def _finding(it,planned,typ,block,severity): return {"calendar_item_id":it.get("calendar_item_id") or it.get("id"),"title":it.get("title"),"planned_at":planned.isoformat(),"channel":it.get("channel"),"platform":it.get("platform"),"collision_type":typ,"blocking_window":block.get("name") or block.get("title") or block.get("reason") or block.get("date"),"severity":severity,"recommended_action":"reschedule content outside blocking window"}
def build_content_calendar_blackout_collision_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); mt=[]; mc={}; items=[]; wins=[]; hol=[]; inc=[]; table="content_calendar_items" if "content_calendar_items" in s else ("planned_posts" if "planned_posts" in s else None)
    if not table: mt.append("content_calendar_items|planned_posts")
    else:
        c=s[table]
        if not ({"planned_at","publish_at","scheduled_at"} & c): mc[table]=["planned_at|publish_at|scheduled_at"]
        else: items=load_table(conn,table,c,{"calendar_item_id":("id","calendar_item_id"),"title":("title","headline"),"planned_at":("planned_at","publish_at","scheduled_at"),"channel":("channel",),"platform":("platform",)})
    if "content_calendar_blackout_windows" in s: wins=load_table(conn,"content_calendar_blackout_windows",s["content_calendar_blackout_windows"],{"name":("name","title","reason"),"starts_at":("starts_at","start_at"),"ends_at":("ends_at","end_at"),"channel":("channel",),"platform":("platform",),"severity":("severity",)})
    if "content_calendar_holidays" in s: hol=load_table(conn,"content_calendar_holidays",s["content_calendar_holidays"],{"date":("date","holiday_date"),"name":("name","title"),"severity":("severity",)})
    if "publication_provider_status_incidents" in s: inc=load_table(conn,"publication_provider_status_incidents",s["publication_provider_status_incidents"],{"name":("name","title","reason"),"started_at":("started_at","start_at"),"ended_at":("ended_at","resolved_at"),"channel":("channel",),"platform":("platform",),"severity":("severity",)})
    return build_content_calendar_blackout_collision_report(items,wins,hol,inc,missing_tables=mt,missing_columns=mc,**kw)
def format_content_calendar_blackout_collision_json(r): return json_dumps(r)
def format_content_calendar_blackout_collision_text(r):
    s=r["summary"]; lines=["Content Calendar Blackout Collision",f"Generated: {r['generated_at']}",f"Totals: items={s['calendar_item_count']} collisions={s['collision_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: "+", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: "+flatten_missing(r["missing_columns"]))
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines+=["","item_id | type | severity | planned_at"]
    for f in r["findings"]: lines.append(f"{f['calendar_item_id']} | {f['collision_type']} | {f['severity']} | {f['planned_at']}")
    return "\n".join(lines)
