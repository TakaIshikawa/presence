"""Import Plausible goal events."""
from __future__ import annotations
import csv, io, json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS blog_plausible_goal_events (goal_name TEXT NOT NULL, path TEXT NOT NULL, referrer TEXT, source TEXT, visitor_id TEXT NOT NULL, occurred_at TEXT NOT NULL, metadata_json TEXT, PRIMARY KEY (goal_name, path, visitor_id, occurred_at))"""
def parse_blog_plausible_goal_events(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("goals","events","items")):
        goal=text(i.get("goal_name") or i.get("goal")); path=_path(i.get("path") or i.get("url")); visitor=text(i.get("visitor_id") or i.get("visitor_id_hash") or i.get("visitor")); occurred=text(i.get("occurred_at") or i.get("timestamp") or i.get("created_at"))
        if not goal or not path or not visitor or not occurred: raise ValueError("goal_name, path/url, visitor_id, and occurred_at are required")
        rows.append({"goal_name":goal,"path":path,"referrer":text(i.get("referrer")) or None,"source":text(i.get("source")) or None,"visitor_id":visitor,"occurred_at":occurred,"metadata_json":_metadata(i.get("metadata"))})
    rows.sort(key=lambda r:(r["goal_name"],r["path"],r["visitor_id"],r["occurred_at"])); return rows
def upsert_blog_plausible_goal_events(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"blog_plausible_goal_event_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO blog_plausible_goal_events VALUES (:goal_name,:path,:referrer,:source,:visitor_id,:occurred_at,:metadata_json) ON CONFLICT(goal_name,path,visitor_id,occurred_at) DO UPDATE SET referrer=excluded.referrer,source=excluded.source,metadata_json=excluded.metadata_json""",r)
    conn.commit(); return {"artifact_type":"blog_plausible_goal_event_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_blog_plausible_goal_events(conn,path,dry_run=False): return upsert_blog_plausible_goal_events(conn,parse_blog_plausible_goal_events(Path(path).read_text()),dry_run=dry_run)
def format_blog_plausible_goal_event_import_json(s): return dump_json(s)
def format_blog_plausible_goal_event_import_text(s): return f"Blog Plausible Goal Event Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _path(v):
    raw=text(v); p=urlsplit(raw); path=p.path if p.scheme or p.netloc else raw.split("?",1)[0].split("#",1)[0]
    return path or "/"
def _metadata(v):
    if isinstance(v,dict): return json.dumps(v,sort_keys=True,separators=(",",":"))
    raw=text(v)
    if not raw: return None
    try:
        parsed=json.loads(raw); return json.dumps(parsed,sort_keys=True,separators=(",",":")) if isinstance(parsed,dict) else raw
    except ValueError: return raw
def _records(raw,keys):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw)
        if isinstance(d,dict):
            for k in keys:
                if k in d: return d[k]
            return [d]
        return d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
