"""Import Bluesky follower snapshots."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS bluesky_follower_snapshots (handle TEXT NOT NULL, did TEXT NOT NULL DEFAULT '', captured_at TEXT NOT NULL, follower_count INTEGER, follows_count INTEGER, post_count INTEGER, raw_payload TEXT, PRIMARY KEY(handle,did,captured_at))"""
def parse_bluesky_follower_snapshots(raw:str):
    rows=[]; errors=[]
    for i,r in enumerate(records(raw,"snapshots"),1):
        try:
            handle=text(one(r,"handle")).lower(); did=text(one(r,"did")); captured=req(r,"captured_at","indexed_at")
            if not (handle or did): raise ValueError("handle or did is required")
            rows.append({"handle":handle,"did":did,"captured_at":captured,"follower_count":int0(one(r,"follower_count","followers_count")),"follows_count":int0(one(r,"follows_count","following_count")),"post_count":int0(one(r,"post_count")),"raw_payload":raw_payload(r)})
        except ValueError as e: errors.append(f"row {i}: {e}")
    return rows, errors
def upsert_bluesky_follower_snapshots(conn,parsed,dry_run=False):
    rows,errors=parsed if isinstance(parsed,tuple) else (parsed,[])
    if dry_run:return summary("bluesky_follower_snapshot_import",len(rows),0,True,errors)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO bluesky_follower_snapshots VALUES (:handle,:did,:captured_at,:follower_count,:follows_count,:post_count,:raw_payload) ON CONFLICT(handle,did,captured_at) DO UPDATE SET follower_count=excluded.follower_count,follows_count=excluded.follows_count,post_count=excluded.post_count,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("bluesky_follower_snapshot_import",len(rows),len(rows),False,errors)
def import_bluesky_follower_snapshots(conn,path,dry_run=False): return upsert_bluesky_follower_snapshots(conn,parse_bluesky_follower_snapshots(Path(path).read_text()),dry_run)
def format_bluesky_follower_snapshot_import_json(s): return fmt_json(s)
def format_bluesky_follower_snapshot_import_text(s): return fmt_text("Bluesky Follower Snapshot Import",s)
