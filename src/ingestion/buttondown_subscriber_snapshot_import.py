"""Import Buttondown subscriber snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS buttondown_subscriber_snapshots (identity TEXT NOT NULL, snapshot_at TEXT NOT NULL, subscriber_id TEXT, email TEXT, status TEXT, tags TEXT, source TEXT, created_at TEXT, unsubscribed_at TEXT, subscriber_type TEXT, PRIMARY KEY (identity, snapshot_at))"""
def parse_buttondown_subscriber_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        sid=text(i.get("subscriber_id") or i.get("id")); email=text(i.get("email")).lower(); snapshot=text(i.get("snapshot_at") or i.get("fetched_at") or i.get("date"))
        ident=sid or email
        rows.append({"identity":ident,"snapshot_at":snapshot,"subscriber_id":sid or None,"email":email or None,"status":text(i.get("status")) or None,"tags":_tags(i.get("tags")),"source":text(i.get("source")) or None,"created_at":text(i.get("created_at")) or None,"unsubscribed_at":text(i.get("unsubscribed_at")) or None,"subscriber_type":text(i.get("subscriber_type") or i.get("type")) or None})
    rows.sort(key=lambda r:(r["identity"],r["snapshot_at"])); return rows
def upsert_buttondown_subscriber_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"buttondown_subscriber_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO buttondown_subscriber_snapshots VALUES (:identity,:snapshot_at,:subscriber_id,:email,:status,:tags,:source,:created_at,:unsubscribed_at,:subscriber_type) ON CONFLICT(identity,snapshot_at) DO UPDATE SET subscriber_id=excluded.subscriber_id,email=excluded.email,status=excluded.status,tags=excluded.tags,source=excluded.source,created_at=excluded.created_at,unsubscribed_at=excluded.unsubscribed_at,subscriber_type=excluded.subscriber_type""",r)
    conn.commit(); return {"artifact_type":"buttondown_subscriber_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_buttondown_subscriber_snapshots(conn,path,dry_run=False): return upsert_buttondown_subscriber_snapshots(conn,parse_buttondown_subscriber_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_buttondown_subscriber_snapshot_import_json(s): return dump_json(s)
def format_buttondown_subscriber_snapshot_import_text(s): return f"Buttondown Subscriber Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _tags(v):
    if isinstance(v,list): parts=[text(x) for x in v]
    else: parts=[p.strip() for p in text(v).replace(";",",").split(",")]
    return ",".join(sorted(p for p in parts if p))
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("subscribers") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
