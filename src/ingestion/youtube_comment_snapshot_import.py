"""Import YouTube comment snapshots."""
from __future__ import annotations
import csv, io, json
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS youtube_comment_snapshots (video_id TEXT NOT NULL, comment_id TEXT NOT NULL, author TEXT, text TEXT, published_at TEXT, like_count INTEGER, reply_count INTEGER, imported_at TEXT, PRIMARY KEY (video_id, comment_id))"""
def parse_youtube_comment_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("comments","items","data")):
        video_id=text(i.get("video_id") or i.get("videoId")); comment_id=text(i.get("comment_id") or i.get("commentId") or i.get("id"))
        if not video_id or not comment_id: raise ValueError("video_id and comment_id are required")
        rows.append({"video_id":video_id,"comment_id":comment_id,"author":text(i.get("author") or i.get("author_name") or i.get("authorDisplayName")) or None,"text":text(i.get("text") or i.get("body") or i.get("textDisplay") or i.get("textOriginal")) or None,"published_at":text(i.get("published_at") or i.get("publishedAt") or i.get("created_at")) or None,"like_count":_int(i.get("like_count") or i.get("likeCount") or i.get("likes")),"reply_count":_int(i.get("reply_count") or i.get("replyCount") or i.get("replies")),"imported_at":text(i.get("imported_at") or i.get("fetched_at") or i.get("captured_at") or i.get("snapshot_at") or i.get("collected_at") or i.get("date")) or None})
    rows.sort(key=lambda r:(r["video_id"],r["comment_id"])); return rows
def upsert_youtube_comment_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"youtube_comment_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO youtube_comment_snapshots VALUES (:video_id,:comment_id,:author,:text,:published_at,:like_count,:reply_count,:imported_at) ON CONFLICT(video_id,comment_id) DO UPDATE SET author=excluded.author,text=excluded.text,published_at=excluded.published_at,like_count=excluded.like_count,reply_count=excluded.reply_count,imported_at=excluded.imported_at""",r)
    conn.commit(); return {"artifact_type":"youtube_comment_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_youtube_comment_snapshots(conn,path,dry_run=False): return upsert_youtube_comment_snapshots(conn,parse_youtube_comment_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_youtube_comment_snapshot_import_json(s): return dump_json(s)
def format_youtube_comment_snapshot_import_text(s): return f"YouTube Comment Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw,keys):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        try:
            d=json.loads(raw)
            if isinstance(d,dict):
                for k in keys:
                    if k in d: return d[k]
                return [d]
            return d
        except json.JSONDecodeError: pass
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
