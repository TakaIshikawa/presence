"""Import X follower snapshots."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS x_follower_snapshots (platform TEXT NOT NULL, profile TEXT NOT NULL, captured_at TEXT NOT NULL, follower_count INTEGER, following_count INTEGER, post_count INTEGER, listed_count INTEGER, profile_url TEXT, raw_payload TEXT, PRIMARY KEY(platform,profile,captured_at))"""
def parse_x_follower_snapshots(raw:str):
    out=[]
    for r in records(raw,"snapshots"):
        out.append({"platform":text(one(r,"platform")) or "x","profile":req(r,"profile","handle","username"),"captured_at":req(r,"captured_at","snapshot_date","date"),"follower_count":int0(one(r,"follower_count","followers_count")),"following_count":int0(one(r,"following_count")),"post_count":int0(one(r,"post_count","posts_count")),"listed_count":int0(one(r,"listed_count")),"profile_url":norm_url(one(r,"profile_url")) or None,"raw_payload":raw_payload(r)})
    return out
def upsert_x_follower_snapshots(conn,rows,dry_run=False):
    if dry_run:return summary("x_follower_snapshot_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO x_follower_snapshots VALUES (:platform,:profile,:captured_at,:follower_count,:following_count,:post_count,:listed_count,:profile_url,:raw_payload) ON CONFLICT(platform,profile,captured_at) DO UPDATE SET follower_count=excluded.follower_count,following_count=excluded.following_count,post_count=excluded.post_count,listed_count=excluded.listed_count,profile_url=excluded.profile_url,raw_payload=excluded.raw_payload""",r)
    conn.commit(); return summary("x_follower_snapshot_import",len(rows),len(rows),False)
def import_x_follower_snapshots(conn,path,dry_run=False): return upsert_x_follower_snapshots(conn,parse_x_follower_snapshots(Path(path).read_text()),dry_run)
def format_x_follower_snapshot_import_json(s): return fmt_json(s)
def format_x_follower_snapshot_import_text(s): return fmt_text("X Follower Snapshot Import",s)
