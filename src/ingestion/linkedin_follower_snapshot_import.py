"""Import LinkedIn follower snapshots."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS linkedin_follower_snapshots (profile_id TEXT NOT NULL, captured_at TEXT NOT NULL, profile_url TEXT, follower_count INTEGER, impression_count INTEGER, visitor_count INTEGER, raw_payload TEXT, PRIMARY KEY(profile_id,captured_at))"""
def parse_linkedin_follower_snapshots(raw:str):
    return [{"profile_id":req(r,"profile_id"),"captured_at":req(r,"captured_at"),"profile_url":norm_url(one(r,"profile_url")) or None,"follower_count":int0(one(r,"follower_count")),"impression_count":int0(one(r,"impression_count")),"visitor_count":int0(one(r,"visitor_count")),"raw_payload":raw_payload(r)} for r in records(raw,"snapshots")]
def upsert_linkedin_follower_snapshots(conn,rows,dry_run=False):
    if dry_run:return summary("linkedin_follower_snapshot_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO linkedin_follower_snapshots VALUES (:profile_id,:captured_at,:profile_url,:follower_count,:impression_count,:visitor_count,:raw_payload) ON CONFLICT(profile_id,captured_at) DO UPDATE SET profile_url=excluded.profile_url,follower_count=excluded.follower_count,impression_count=excluded.impression_count,visitor_count=excluded.visitor_count,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("linkedin_follower_snapshot_import",len(rows),len(rows),False)
def import_linkedin_follower_snapshots(conn,path,dry_run=False): return upsert_linkedin_follower_snapshots(conn,parse_linkedin_follower_snapshots(Path(path).read_text()),dry_run)
def format_linkedin_follower_snapshot_import_json(s): return fmt_json(s)
def format_linkedin_follower_snapshot_import_text(s): return fmt_text("LinkedIn Follower Snapshot Import",s)
