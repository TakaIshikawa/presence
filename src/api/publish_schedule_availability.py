from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from ._utils import columns, has_table, missing_schema
ARTIFACT_TYPE="publish_schedule_availability"
def get_publish_schedule_availability(conn:sqlite3.Connection,*,now:str="2026-01-01T09:00:00",lookahead_hours:int=24,daily_capacity:int=3)->dict:
    if not has_table(conn,"publish_queue"): return missing_schema(ARTIFACT_TYPE,["publish_queue"])
    cols=columns(conn,"publish_queue"); needed={"channel","scheduled_at","status"}
    if not needed<=cols: return missing_schema(ARTIFACT_TYPE,sorted(needed-cols))
    start=datetime.fromisoformat(now.replace("Z","")); end=start+timedelta(hours=lookahead_hours)
    channels=[r[0] for r in conn.execute("SELECT DISTINCT channel FROM publish_queue ORDER BY channel")]
    out=[]
    quiet=[dict(r) for r in conn.execute("SELECT channel,start_hour,end_hour FROM publish_quiet_hours")] if has_table(conn,"publish_quiet_hours") else []
    blackouts=[dict(r) for r in conn.execute("SELECT channel,start_at,end_at FROM publish_blackouts")] if has_table(conn,"publish_blackouts") else []
    for ch in channels:
        scheduled=conn.execute("SELECT COUNT(*) FROM publish_queue WHERE channel=? AND status IN ('scheduled','queued') AND scheduled_at>=? AND scheduled_at<?",(ch,now,end.isoformat())).fetchone()[0]
        probe=start
        while probe<end and (_quiet(ch,probe,quiet) or _blackout(ch,probe,blackouts)): probe+=timedelta(hours=1)
        out.append({"channel":ch,"next_available_at":probe.isoformat() if probe<end else None,"capacity_remaining":max(0,daily_capacity-scheduled),"scheduled_count":scheduled})
    return {"artifact_type":ARTIFACT_TYPE,"lookahead_hours":lookahead_hours,"channels":out}
def _quiet(ch,dt,rows): return any((r["channel"] in (None,"",ch)) and int(r["start_hour"])<=dt.hour<int(r["end_hour"]) for r in rows)
def _blackout(ch,dt,rows): return any((r["channel"] in (None,"",ch)) and r["start_at"]<=dt.isoformat()<r["end_at"] for r in rows)
