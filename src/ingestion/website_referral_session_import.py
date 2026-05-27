"""Import website referral session aggregates."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS website_referral_sessions (date TEXT NOT NULL, source TEXT NOT NULL, medium TEXT NOT NULL, campaign TEXT NOT NULL, landing_path TEXT NOT NULL, sessions INTEGER, engaged_sessions INTEGER, conversions INTEGER, fetched_at TEXT, PRIMARY KEY (date, source, medium, campaign, landing_path))"""
def parse_website_referral_sessions(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        rows.append({"date":text(i.get("date") or i.get("event_date")),"source":_label(i.get("source")),"medium":_label(i.get("medium")),"campaign":_label(i.get("campaign")),"landing_path":_path(i.get("landing_path") or i.get("landing_url") or i.get("url")),"sessions":_int(i.get("sessions")),"engaged_sessions":_int(i.get("engaged_sessions")),"conversions":_int(i.get("conversions")),"fetched_at":text(i.get("fetched_at")) or None})
    rows.sort(key=lambda r:(r["date"],r["source"],r["medium"],r["campaign"],r["landing_path"])); return rows
def upsert_website_referral_sessions(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"website_referral_session_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO website_referral_sessions VALUES (:date,:source,:medium,:campaign,:landing_path,:sessions,:engaged_sessions,:conversions,:fetched_at) ON CONFLICT(date,source,medium,campaign,landing_path) DO UPDATE SET sessions=excluded.sessions,engaged_sessions=excluded.engaged_sessions,conversions=excluded.conversions,fetched_at=excluded.fetched_at""",r)
    conn.commit(); return {"artifact_type":"website_referral_session_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_website_referral_sessions(conn,path,dry_run=False): return upsert_website_referral_sessions(conn,parse_website_referral_sessions(Path(path).read_text()),dry_run=dry_run)
def format_website_referral_session_import_json(s): return dump_json(s)
def format_website_referral_session_import_text(s): return f"Website Referral Session Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _label(v): return text(v).lower() or "unknown"
def _path(v):
    raw=text(v); p=urlsplit(raw); return (p.path if p.scheme or p.netloc else raw.split("?",1)[0]) or "/"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("sessions") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
