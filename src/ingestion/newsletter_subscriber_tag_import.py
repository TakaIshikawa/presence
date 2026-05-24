"""Import newsletter subscriber tag snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS newsletter_subscriber_tags (subscriber_id TEXT, email TEXT, tag TEXT NOT NULL, observed_at TEXT NOT NULL, source TEXT, status TEXT, PRIMARY KEY (subscriber_id, email, tag, observed_at))"""
def parse_newsletter_subscriber_tags(raw:str)->list[dict[str,Any]]:
    rows=[]
    for item in _records(raw):
        sid=text(item.get("subscriber_id")); email=text(item.get("email")).lower(); tag=text(item.get("tag")); observed=text(item.get("observed_at") or item.get("timestamp"))
        if not (sid or email) or not tag or not observed: raise ValueError("subscriber_id or email, tag, and observed_at are required")
        rows.append({"subscriber_id":sid or None,"email":email or None,"tag":tag,"observed_at":observed,"source":text(item.get("source")) or None,"status":text(item.get("status")) or None})
    rows.sort(key=lambda r:(r["subscriber_id"] or "",r["email"] or "",r["tag"],r["observed_at"])); return rows
def upsert_newsletter_subscriber_tags(conn:sqlite3.Connection, rows:list[dict[str,Any]],*,dry_run:bool=False)->dict[str,Any]:
    if dry_run: return {"artifact_type":"newsletter_subscriber_tag_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO newsletter_subscriber_tags VALUES (:subscriber_id,:email,:tag,:observed_at,:source,:status) ON CONFLICT(subscriber_id,email,tag,observed_at) DO UPDATE SET source=excluded.source,status=excluded.status""",r)
    conn.commit(); return {"artifact_type":"newsletter_subscriber_tag_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_newsletter_subscriber_tags(conn,path,dry_run=False): return upsert_newsletter_subscriber_tags(conn,parse_newsletter_subscriber_tags(Path(path).read_text()),dry_run=dry_run)
def format_newsletter_subscriber_tag_import_json(s): return dump_json(s)
def format_newsletter_subscriber_tag_import_text(s): return f"Newsletter Subscriber Tag Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        data=json.loads(raw); return data.get("tags",[data]) if isinstance(data,dict) else data
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]
