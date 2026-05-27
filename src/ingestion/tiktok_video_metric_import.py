"""Import TikTok video metric snapshots."""
from __future__ import annotations
import csv, io, json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS tiktok_video_metrics (account_handle TEXT, video_id TEXT NOT NULL, url TEXT, posted_at TEXT, caption TEXT, view_count INTEGER, like_count INTEGER, comment_count INTEGER, share_count INTEGER, save_count INTEGER, captured_at TEXT NOT NULL, PRIMARY KEY (video_id, captured_at))"""
def parse_tiktok_video_metrics(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("videos","metrics","items")):
        video_id=text(i.get("video_id") or i.get("id")); captured_at=text(i.get("captured_at") or i.get("collected_at") or i.get("snapshot_at"))
        if not video_id or not captured_at: raise ValueError("video_id and captured_at are required")
        rows.append({"account_handle":_handle(i.get("account_handle") or i.get("handle") or i.get("username")) or None,"video_id":video_id,"url":_url(i.get("url") or i.get("video_url")) or None,"posted_at":text(i.get("posted_at") or i.get("created_at")) or None,"caption":text(i.get("caption") or i.get("text")) or None,"view_count":_int(i.get("view_count") or i.get("views") or i.get("plays")),"like_count":_int(i.get("like_count") or i.get("likes")),"comment_count":_int(i.get("comment_count") or i.get("comments")),"share_count":_int(i.get("share_count") or i.get("shares")),"save_count":_int(i.get("save_count") or i.get("saves")),"captured_at":captured_at})
    rows.sort(key=lambda r:(r["video_id"],r["captured_at"])); return rows
def upsert_tiktok_video_metrics(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"tiktok_video_metric_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO tiktok_video_metrics VALUES (:account_handle,:video_id,:url,:posted_at,:caption,:view_count,:like_count,:comment_count,:share_count,:save_count,:captured_at) ON CONFLICT(video_id,captured_at) DO UPDATE SET account_handle=excluded.account_handle,url=excluded.url,posted_at=excluded.posted_at,caption=excluded.caption,view_count=excluded.view_count,like_count=excluded.like_count,comment_count=excluded.comment_count,share_count=excluded.share_count,save_count=excluded.save_count""",r)
    conn.commit(); return {"artifact_type":"tiktok_video_metric_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_tiktok_video_metrics(conn,path,dry_run=False): return upsert_tiktok_video_metrics(conn,parse_tiktok_video_metrics(Path(path).read_text()),dry_run=dry_run)
def format_tiktok_video_metric_import_json(s): return dump_json(s)
def format_tiktok_video_metric_import_text(s): return f"TikTok Video Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
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
