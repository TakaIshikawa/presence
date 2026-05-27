"""Import Reddit mention snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS reddit_mention_snapshots (thing_id TEXT PRIMARY KEY, subreddit TEXT, kind TEXT, author TEXT, title TEXT, body TEXT, score INTEGER, permalink TEXT, created_at TEXT NOT NULL, matched_term TEXT, fetched_at TEXT)"""
def parse_reddit_mention_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        thing_id=text(i.get("thing_id") or i.get("id") or i.get("name")); created=text(i.get("created_at") or i.get("created_utc") or i.get("created"))
        if not thing_id: raise ValueError("thing_id is required")
        if not created: raise ValueError("created_at is required")
        kind=_kind(i.get("kind") or i.get("type") or i.get("record_type") or thing_id)
        rows.append({"thing_id":thing_id,"subreddit":text(i.get("subreddit")) or None,"kind":kind,"author":text(i.get("author")) or None,"title":text(i.get("title")) or None,"body":text(i.get("body") or i.get("selftext") or i.get("comment")) or None,"score":_int(i.get("score")),"permalink":text(i.get("permalink") or i.get("url")) or None,"created_at":created,"matched_term":text(i.get("matched_term") or i.get("query")) or None,"fetched_at":text(i.get("fetched_at")) or None})
    rows.sort(key=lambda r:r["thing_id"]); return rows
def upsert_reddit_mention_snapshots(conn:sqlite3.Connection,rows:list[dict[str,Any]],*,dry_run:bool=False)->dict[str,Any]:
    if dry_run: return {"artifact_type":"reddit_mention_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO reddit_mention_snapshots VALUES (:thing_id,:subreddit,:kind,:author,:title,:body,:score,:permalink,:created_at,:matched_term,:fetched_at) ON CONFLICT(thing_id) DO UPDATE SET subreddit=excluded.subreddit,kind=excluded.kind,author=excluded.author,title=excluded.title,body=excluded.body,score=excluded.score,permalink=excluded.permalink,created_at=excluded.created_at,matched_term=excluded.matched_term,fetched_at=excluded.fetched_at""",r)
    conn.commit(); return {"artifact_type":"reddit_mention_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_reddit_mention_snapshots(conn,path,dry_run=False): return upsert_reddit_mention_snapshots(conn,parse_reddit_mention_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_reddit_mention_snapshot_import_json(s): return dump_json(s)
def format_reddit_mention_snapshot_import_text(s): return f"Reddit Mention Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _kind(v):
    raw=text(v).lower()
    if raw in ("comment","comments","t1") or raw.startswith("t1_"): return "comment"
    if raw in ("post","submission","link","posts","t3") or raw.startswith("t3_"): return "post"
    return raw or "post"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("mentions") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
