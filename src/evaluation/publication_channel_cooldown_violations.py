"""Find same-channel publication items scheduled closer than a cooldown."""
from __future__ import annotations
from typing import Any
from ._batch_report_common import *
ARTIFACT_TYPE="publication_channel_cooldown_violations"; DEFAULT_COOLDOWN_MINUTES=60; DEFAULT_WINDOW_DAYS=30; DEFAULT_LIMIT=100
def build_publication_channel_cooldown_violations_report(rows:list[dict[str,Any]],*,cooldown_minutes:int=DEFAULT_COOLDOWN_MINUTES,channel=None,window_days:int=DEFAULT_WINDOW_DAYS,limit:int=DEFAULT_LIMIT,now=None,missing_tables=None,missing_columns=None):
    positive("cooldown_minutes",cooldown_minutes); positive("window_days",window_days); positive("limit",limit)
    gen=now_value(now); cutoff=gen-timedelta(days=window_days); groups=defaultdict(list)
    for r in rows:
        ch=lower(r.get("channel") or r.get("platform"),"unknown")
        if channel and ch!=lower(channel): continue
        ts=dt(r.get("scheduled_at") or r.get("publish_at") or r.get("published_at") or r.get("created_at"))
        st=lower(r.get("status"),"queued")
        if not ts or ts<cutoff or st not in {"queued","scheduled","published","sent","posted"}: continue
        groups[ch].append({**r,"_ts":ts,"_channel":ch})
    findings=[]
    for ch,items in groups.items():
        items.sort(key=lambda r:r["_ts"])
        for a,b in zip(items,items[1:]):
            gap=round((b["_ts"]-a["_ts"]).total_seconds()/60,2)
            if gap<cooldown_minutes: findings.append({"channel":ch,"earlier_item_id":a.get("id") or a.get("queue_id") or a.get("content_id"),"later_item_id":b.get("id") or b.get("queue_id") or b.get("content_id"),"gap_minutes":gap,"scheduled_at":b["_ts"].isoformat()})
    findings.sort(key=lambda f:(f["channel"],f["scheduled_at"]))
    shown=findings[:limit]
    return {"artifact_type":ARTIFACT_TYPE,"generated_at":gen.isoformat(),"filters":{"cooldown_minutes":cooldown_minutes,"channel":channel,"window_days":window_days,"limit":limit},"summary":{"item_count":sum(len(v) for v in groups.values()),"finding_count":len(findings),"shown_count":len(shown)},"findings":shown,"missing_tables":sorted(missing_tables or []),"missing_columns":{k:sorted(v) for k,v in sorted((missing_columns or {}).items()) if v},"empty_state":empty_state(findings,"No publication channel cooldown violations found.",schema_gap=bool(missing_tables or missing_columns))}
def build_publication_channel_cooldown_violations_report_from_db(db_or_conn:Any,**kw):
    conn=connection(db_or_conn); s=schema(conn); table=next((t for t in ("publish_queue","publications","generated_content") if t in s),None); mt=[]; mc={}; rows=[]
    if not table: mt.append("publish_queue|publications|generated_content")
    else:
        c=s[table]; miss=[]
        if "id" not in c: miss.append("id")
        if not {"scheduled_at","publish_at","published_at","created_at"}&c: miss.append("scheduled_at|publish_at|published_at|created_at")
        if miss: mc[table]=miss
        else: rows=load_table(conn,table,c,{"id":("id","queue_id","content_id"),"channel":("channel","platform"),"status":("status","state"),"scheduled_at":("scheduled_at","publish_at","published_at","created_at")})
    return build_publication_channel_cooldown_violations_report(rows,missing_tables=mt,missing_columns=mc,**kw)
def format_publication_channel_cooldown_violations_json(r): return json_dumps(r)
def format_publication_channel_cooldown_violations_text(r):
    lines=["Publication Channel Cooldown Violations",f"Generated: {r['generated_at']}",f"Totals: items={r['summary']['item_count']} findings={r['summary']['finding_count']} shown={r['summary']['shown_count']}"]
    if not r["findings"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    for f in r["findings"]: lines.append(f"  - channel={f['channel']} earlier={f['earlier_item_id']} later={f['later_item_id']} gap={f['gap_minutes']}")
    return "\n".join(lines)
