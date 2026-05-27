"""Import Threads post snapshot exports."""
from __future__ import annotations
import csv, io, json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS threads_post_snapshots (account_handle TEXT, post_id TEXT NOT NULL, url TEXT, posted_at TEXT, text TEXT, like_count INTEGER, reply_count INTEGER, repost_count INTEGER, view_count INTEGER, captured_at TEXT NOT NULL, PRIMARY KEY (post_id, captured_at))"""
def parse_threads_post_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("posts","items")):
        post_id=text(i.get("post_id") or i.get("id")); captured_at=text(i.get("captured_at") or i.get("collected_at") or i.get("snapshot_at"))
        if not post_id or not captured_at: raise ValueError("post_id and captured_at are required")
        rows.append({"account_handle":_handle(i.get("account_handle") or i.get("handle") or i.get("username")) or None,"post_id":post_id,"url":_url(i.get("url") or i.get("post_url")) or None,"posted_at":text(i.get("posted_at") or i.get("created_at")) or None,"text":text(i.get("text") or i.get("caption")) or None,"like_count":_int(i.get("like_count") or i.get("likes")),"reply_count":_int(i.get("reply_count") or i.get("replies")),"repost_count":_int(i.get("repost_count") or i.get("reposts")),"view_count":_int(i.get("view_count") or i.get("views")),"captured_at":captured_at})
    rows.sort(key=lambda r:(r["post_id"],r["captured_at"])); return rows
def upsert_threads_post_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"threads_post_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO threads_post_snapshots VALUES (:account_handle,:post_id,:url,:posted_at,:text,:like_count,:reply_count,:repost_count,:view_count,:captured_at) ON CONFLICT(post_id,captured_at) DO UPDATE SET account_handle=excluded.account_handle,url=excluded.url,posted_at=excluded.posted_at,text=excluded.text,like_count=excluded.like_count,reply_count=excluded.reply_count,repost_count=excluded.repost_count,view_count=excluded.view_count""",r)
    conn.commit(); return {"artifact_type":"threads_post_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_threads_post_snapshots(conn,path,dry_run=False): return upsert_threads_post_snapshots(conn,parse_threads_post_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_threads_post_snapshot_import_json(s): return dump_json(s)
def format_threads_post_snapshot_import_text(s): return f"Threads Post Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _handle(v): return text(v).lstrip("@").strip()
def _url(v):
    raw=text(v)
    if not raw: return ""
    p=urlsplit(raw); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path,p.query,p.fragment))
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw,keys):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw)
        if isinstance(d,dict):
            for k in keys:
                if k in d: return d[k]
            return [d]
        return d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
