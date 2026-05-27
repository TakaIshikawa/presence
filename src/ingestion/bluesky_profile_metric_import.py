"""Import Bluesky profile metric snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS bluesky_profile_metrics (identity TEXT NOT NULL, snapshot_at TEXT NOT NULL, did TEXT, handle TEXT, followers_count INTEGER, follows_count INTEGER, posts_count INTEGER, display_name TEXT, description TEXT, indexed_at TEXT, avatar_url TEXT, PRIMARY KEY (identity, snapshot_at))"""
def parse_bluesky_profile_metrics(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        did=text(i.get("did")); handle=text(i.get("handle")).lower(); snapshot=text(i.get("snapshot_at") or i.get("fetched_at") or i.get("date"))
        rows.append({"identity":did or handle,"snapshot_at":snapshot,"did":did or None,"handle":handle or None,"followers_count":_int(i.get("followers_count") or i.get("followersCount")),"follows_count":_int(i.get("follows_count") or i.get("followsCount")),"posts_count":_int(i.get("posts_count") or i.get("postsCount")),"display_name":text(i.get("display_name") or i.get("displayName")) or None,"description":text(i.get("description")) or None,"indexed_at":text(i.get("indexed_at") or i.get("indexedAt")) or None,"avatar_url":text(i.get("avatar_url") or i.get("avatar")) or None})
    rows.sort(key=lambda r:(r["identity"],r["snapshot_at"])); return rows
def upsert_bluesky_profile_metrics(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"bluesky_profile_metric_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO bluesky_profile_metrics VALUES (:identity,:snapshot_at,:did,:handle,:followers_count,:follows_count,:posts_count,:display_name,:description,:indexed_at,:avatar_url) ON CONFLICT(identity,snapshot_at) DO UPDATE SET did=excluded.did,handle=excluded.handle,followers_count=excluded.followers_count,follows_count=excluded.follows_count,posts_count=excluded.posts_count,display_name=excluded.display_name,description=excluded.description,indexed_at=excluded.indexed_at,avatar_url=excluded.avatar_url""",r)
    conn.commit(); return {"artifact_type":"bluesky_profile_metric_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_bluesky_profile_metrics(conn,path,dry_run=False): return upsert_bluesky_profile_metrics(conn,parse_bluesky_profile_metrics(Path(path).read_text()),dry_run=dry_run)
def format_bluesky_profile_metric_import_json(s): return dump_json(s)
def format_bluesky_profile_metric_import_text(s): return f"Bluesky Profile Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("profiles") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
