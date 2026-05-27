"""Import blog RSS entry snapshots."""
from __future__ import annotations
from pathlib import Path
from ._simple_import_common import *
SCHEMA="""CREATE TABLE IF NOT EXISTS blog_rss_entry_snapshots (feed_url TEXT NOT NULL, entry_key TEXT NOT NULL, entry_url TEXT, guid TEXT, title TEXT, author TEXT, published_at TEXT, updated_at TEXT, summary TEXT, tags TEXT, PRIMARY KEY(feed_url,entry_key))"""
def parse_blog_rss_entry_snapshots(raw:str):
    data=records(raw,"entries","items"); parent_feed=""
    try:
        import json
        parsed=json.loads(raw.strip())
        if isinstance(parsed,dict): parent_feed=text(parsed.get("feed_url"))
    except Exception:
        pass
    rows=[]
    for r in data:
        if parent_feed and not text(r.get("feed_url")):
            r={**r,"feed_url":parent_feed}
        feed=norm_url(req(r,"feed_url")); url=norm_url(one(r,"entry_url","link"),strip_query=True); guid=text(one(r,"guid","id")); key=guid or url
        if not key: raise ValueError("entry_url/link or guid/id is required")
        rows.append({"feed_url":feed,"entry_key":key,"entry_url":url or None,"guid":guid or None,"title":text(one(r,"title")) or None,"author":text(one(r,"author")) or None,"published_at":text(one(r,"published_at")) or None,"updated_at":text(one(r,"updated_at")) or None,"summary":text(one(r,"summary")) or None,"tags":text(one(r,"tags")) or None})
    return rows
def upsert_blog_rss_entry_snapshots(conn,rows,dry_run=False):
    if dry_run:return summary("blog_rss_entry_snapshot_import",len(rows),0,True)
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO blog_rss_entry_snapshots VALUES (:feed_url,:entry_key,:entry_url,:guid,:title,:author,:published_at,:updated_at,:summary,:tags) ON CONFLICT(feed_url,entry_key) DO UPDATE SET entry_url=excluded.entry_url,guid=excluded.guid,title=excluded.title,author=excluded.author,published_at=excluded.published_at,updated_at=excluded.updated_at,summary=excluded.summary,tags=excluded.tags""",r)
    conn.commit(); return summary("blog_rss_entry_snapshot_import",len(rows),len(rows),False)
def import_blog_rss_entry_snapshots(conn,path,dry_run=False): return upsert_blog_rss_entry_snapshots(conn,parse_blog_rss_entry_snapshots(Path(path).read_text()),dry_run)
def format_blog_rss_entry_snapshot_import_json(s): return fmt_json(s)
def format_blog_rss_entry_snapshot_import_text(s): return fmt_text("Blog RSS Entry Snapshot Import",s)
