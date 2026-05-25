"""Import blog analytics pageview snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS blog_analytics_pageviews (canonical_path TEXT NOT NULL, date TEXT NOT NULL, pageviews INTEGER, unique_visitors INTEGER, avg_time_seconds REAL, referrer TEXT, source TEXT, PRIMARY KEY (canonical_path, date))"""
def parse_blog_analytics_pageviews(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        path=_path(i.get("path") or i.get("url")); date=text(i.get("date"))
        if not path or not date: raise ValueError("path/url and date are required")
        rows.append({"canonical_path":path,"date":date,"pageviews":_int(i.get("pageviews")),"unique_visitors":_int(i.get("unique_visitors")),"avg_time_seconds":_float(i.get("avg_time_seconds")),"referrer":text(i.get("referrer")) or None,"source":text(i.get("source")) or None})
    rows.sort(key=lambda r:(r["canonical_path"],r["date"])); return rows
def upsert_blog_analytics_pageviews(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"blog_analytics_pageview_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO blog_analytics_pageviews VALUES (:canonical_path,:date,:pageviews,:unique_visitors,:avg_time_seconds,:referrer,:source) ON CONFLICT(canonical_path,date) DO UPDATE SET pageviews=excluded.pageviews,unique_visitors=excluded.unique_visitors,avg_time_seconds=excluded.avg_time_seconds,referrer=excluded.referrer,source=excluded.source""",r)
    conn.commit(); return {"artifact_type":"blog_analytics_pageview_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_blog_analytics_pageviews(conn,path,dry_run=False): return upsert_blog_analytics_pageviews(conn,parse_blog_analytics_pageviews(Path(path).read_text()),dry_run=dry_run)
def format_blog_analytics_pageview_import_json(s): return dump_json(s)
def format_blog_analytics_pageview_import_text(s): return f"Blog Analytics Pageview Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _path(v):
    raw=text(v); parts=urlsplit(raw); path=parts.path if parts.scheme or parts.netloc else raw.split("?",1)[0]; return path or "/"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _float(v):
    try: return float(v)
    except (TypeError,ValueError): return 0.0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw); return d.get("pageviews",[d]) if isinstance(d,dict) else d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
