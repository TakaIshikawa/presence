"""Import Hacker News mention snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS hacker_news_mention_snapshots (identity TEXT NOT NULL, detected_at TEXT NOT NULL, hn_object_id TEXT, title TEXT, url TEXT, author TEXT, points INTEGER, comment_count INTEGER, story_text TEXT, comment_text TEXT, parent_id TEXT, source_query TEXT, PRIMARY KEY (identity, detected_at))"""
def parse_hacker_news_mention_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[_row(i) for i in _records(raw)]
    rows.sort(key=lambda r:(r["identity"],r["detected_at"])); return rows
def upsert_hacker_news_mention_snapshots(conn:sqlite3.Connection,rows:list[dict[str,Any]],dry_run:bool=False)->dict[str,Any]:
    if dry_run: return {"artifact_type":"hacker_news_mention_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO hacker_news_mention_snapshots VALUES (:identity,:detected_at,:hn_object_id,:title,:url,:author,:points,:comment_count,:story_text,:comment_text,:parent_id,:source_query) ON CONFLICT(identity,detected_at) DO UPDATE SET hn_object_id=excluded.hn_object_id,title=excluded.title,url=excluded.url,author=excluded.author,points=excluded.points,comment_count=excluded.comment_count,story_text=excluded.story_text,comment_text=excluded.comment_text,parent_id=excluded.parent_id,source_query=excluded.source_query""",r)
    conn.commit(); return {"artifact_type":"hacker_news_mention_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_hacker_news_mention_snapshots(conn,path,dry_run=False): return upsert_hacker_news_mention_snapshots(conn,parse_hacker_news_mention_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_hacker_news_mention_snapshot_import_json(s): return dump_json(s)
def format_hacker_news_mention_snapshot_import_text(s): return f"Hacker News Mention Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _row(i):
    obj=text(i.get("hn_object_id") or i.get("objectID") or i.get("story_id"))
    url=_url(i.get("url") or i.get("story_url"))
    title=text(i.get("title") or i.get("story_title"))
    detected_at=text(i.get("detected_at") or i.get("created_at") or i.get("created_at_i"))
    identity=obj or url or title
    if not identity or not detected_at: raise ValueError("hn_object_id or url/title plus detected_at are required")
    return {"identity":identity,"detected_at":detected_at,"hn_object_id":obj or None,"title":title or None,"url":url or None,"author":text(i.get("author") or i.get("user")) or None,"points":_int(i.get("points")),"comment_count":_int(i.get("comment_count") or i.get("num_comments")),"story_text":text(i.get("story_text")) or None,"comment_text":text(i.get("comment_text")) or None,"parent_id":text(i.get("parent_id") or i.get("parent")) or None,"source_query":text(i.get("source_query") or i.get("query")) or None}
def _url(v):
    raw=text(v)
    if not raw: return ""
    p=urlsplit(raw); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path,p.query,""))
def _int(v):
    if v in (None,""): return None
    return int(float(v))
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw); return d.get("hits") or d.get("rows") or ([d] if isinstance(d,dict) else d)
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
