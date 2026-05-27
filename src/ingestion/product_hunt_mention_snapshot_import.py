"""Import Product Hunt mention snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS product_hunt_mention_snapshots (product_id TEXT NOT NULL, product_name TEXT, discussion_id TEXT NOT NULL, comment_id TEXT NOT NULL, kind TEXT NOT NULL, author TEXT, body TEXT, votes_count INTEGER, comments_count INTEGER, url TEXT, posted_at TEXT, matched_term TEXT, fetched_at TEXT, PRIMARY KEY (product_id, discussion_id, comment_id, kind))"""
def parse_product_hunt_mention_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        product_id=text(i.get("product_id") or i.get("id")); kind=text(i.get("kind") or i.get("type") or ("comment" if i.get("comment_id") else "discussion" if i.get("discussion_id") else "launch")).lower()
        rows.append({"product_id":product_id,"product_name":text(i.get("product_name") or i.get("name")) or None,"discussion_id":text(i.get("discussion_id")),"comment_id":text(i.get("comment_id")),"kind":kind,"author":text(i.get("author") or i.get("user")) or None,"body":text(i.get("body") or i.get("comment") or i.get("description")) or None,"votes_count":_int(i.get("votes_count") or i.get("votes")),"comments_count":_int(i.get("comments_count") or i.get("comments")),"url":text(i.get("url")) or None,"posted_at":text(i.get("posted_at") or i.get("created_at")) or None,"matched_term":text(i.get("matched_term") or i.get("query")) or None,"fetched_at":text(i.get("fetched_at")) or None})
    rows.sort(key=lambda r:(r["product_id"],r["discussion_id"],r["comment_id"],r["kind"])); return rows
def upsert_product_hunt_mention_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"product_hunt_mention_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO product_hunt_mention_snapshots VALUES (:product_id,:product_name,:discussion_id,:comment_id,:kind,:author,:body,:votes_count,:comments_count,:url,:posted_at,:matched_term,:fetched_at) ON CONFLICT(product_id,discussion_id,comment_id,kind) DO UPDATE SET product_name=excluded.product_name,author=excluded.author,body=excluded.body,votes_count=excluded.votes_count,comments_count=excluded.comments_count,url=excluded.url,posted_at=excluded.posted_at,matched_term=excluded.matched_term,fetched_at=excluded.fetched_at""",r)
    conn.commit(); return {"artifact_type":"product_hunt_mention_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_product_hunt_mention_snapshots(conn,path,dry_run=False): return upsert_product_hunt_mention_snapshots(conn,parse_product_hunt_mention_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_product_hunt_mention_snapshot_import_json(s): return dump_json(s)
def format_product_hunt_mention_snapshot_import_text(s): return f"Product Hunt Mention Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
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
