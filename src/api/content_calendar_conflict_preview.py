from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from ._utils import columns, has_table, missing_schema
ARTIFACT_TYPE="content_calendar_conflict_preview"
def get_content_calendar_conflict_preview(conn:sqlite3.Connection,*,topic_window_hours:int=48)->dict:
    if not has_table(conn,"content_calendar"): return missing_schema(ARTIFACT_TYPE,["content_calendar"])
    cols=columns(conn,"content_calendar"); needed={"id","channel","scheduled_at","topic"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    rows=[dict(r) for r in conn.execute("SELECT id,channel,scheduled_at,topic FROM content_calendar ORDER BY scheduled_at,id")]
    conflicts=[]
    for i,a in enumerate(rows):
        for b in rows[i+1:]:
            if a["channel"]==b["channel"] and a["scheduled_at"]==b["scheduled_at"]: conflicts.append(_c("same_channel_collision",[a["id"],b["id"]],a["channel"],a["scheduled_at"],"same channel and scheduled_at"))
            if a.get("topic") and a["topic"]==b.get("topic") and abs(datetime.fromisoformat(a["scheduled_at"])-datetime.fromisoformat(b["scheduled_at"]))<=timedelta(hours=topic_window_hours): conflicts.append(_c("topic_repetition",[a["id"],b["id"]],a["channel"],a["scheduled_at"],"topic repeats within window"))
    if has_table(conn,"calendar_blackouts"):
        for r in rows:
            hit=conn.execute("SELECT reason FROM calendar_blackouts WHERE (channel IS NULL OR channel='' OR channel=?) AND start_at<=? AND end_at>?",(r["channel"],r["scheduled_at"],r["scheduled_at"])).fetchone()
            if hit: conflicts.append(_c("blackout_overlap",[r["id"]],r["channel"],r["scheduled_at"],hit[0] or "blackout window"))
    return {"artifact_type":ARTIFACT_TYPE,"summary":{"conflict_count":len(conflicts)},"conflicts":conflicts}
def _c(t,ids,ch,at,reason): return {"type":t,"affected_item_ids":ids,"channel":ch,"scheduled_at":at,"reason":reason}
