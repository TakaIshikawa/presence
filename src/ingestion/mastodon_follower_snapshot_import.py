"""Import Mastodon follower snapshots."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS mastodon_follower_snapshots (account_handle TEXT NOT NULL, snapshot_date TEXT NOT NULL, followers_count INTEGER, following_count INTEGER, statuses_count INTEGER, instance TEXT, display_name TEXT, source TEXT, PRIMARY KEY(account_handle,snapshot_date))"""
def parse_mastodon_follower_snapshots(raw:str):
    return [{"account_handle":req(r,"account_handle").lower(),"snapshot_date":req(r,"snapshot_date","date"),"followers_count":int0(one(r,"followers_count")),"following_count":int0(one(r,"following_count")),"statuses_count":int0(one(r,"statuses_count")),"instance":text(one(r,"instance")) or None,"display_name":text(one(r,"display_name")) or None,"source":text(one(r,"source")) or None} for r in records(raw,"snapshots")]
def upsert_mastodon_follower_snapshots(conn,rows,dry_run=False):
    if dry_run:return summary("mastodon_follower_snapshot_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO mastodon_follower_snapshots VALUES (:account_handle,:snapshot_date,:followers_count,:following_count,:statuses_count,:instance,:display_name,:source) ON CONFLICT(account_handle,snapshot_date) DO UPDATE SET followers_count=excluded.followers_count,following_count=excluded.following_count,statuses_count=excluded.statuses_count,instance=excluded.instance,display_name=excluded.display_name,source=excluded.source""",r)
    conn.commit(); return summary("mastodon_follower_snapshot_import",len(rows),len(rows),False)
def import_mastodon_follower_snapshots(conn,path,dry_run=False): return upsert_mastodon_follower_snapshots(conn,parse_mastodon_follower_snapshots(Path(path).read_text()),dry_run)
def format_mastodon_follower_snapshot_import_json(s): return fmt_json(s)
def format_mastodon_follower_snapshot_import_text(s): return fmt_text("Mastodon Follower Snapshot Import",s)
