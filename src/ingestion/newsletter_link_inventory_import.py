"""Import newsletter link inventory exports."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS newsletter_link_inventory (issue_id TEXT NOT NULL, url TEXT NOT NULL, link_text TEXT, section TEXT, position INTEGER, utm_campaign TEXT, observed_at TEXT, PRIMARY KEY (issue_id, url))"""
def parse_newsletter_link_inventory(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        issue=text(i.get("issue_id")); url=_url(i.get("url")); observed=text(i.get("observed_at"))
        if not issue or not url or not observed: raise ValueError("issue_id, url, and observed_at are required")
        rows.append({"issue_id":issue,"url":url,"link_text":text(i.get("link_text")) or None,"section":text(i.get("section")) or None,"position":_int(i.get("position")),"utm_campaign":text(i.get("utm_campaign")) or None,"observed_at":observed})
    rows.sort(key=lambda r:(r["issue_id"],r["url"])); return rows
def upsert_newsletter_link_inventory(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"newsletter_link_inventory_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO newsletter_link_inventory VALUES (:issue_id,:url,:link_text,:section,:position,:utm_campaign,:observed_at) ON CONFLICT(issue_id,url) DO UPDATE SET link_text=excluded.link_text,section=excluded.section,position=excluded.position,utm_campaign=excluded.utm_campaign,observed_at=excluded.observed_at""",r)
    conn.commit(); return {"artifact_type":"newsletter_link_inventory_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_newsletter_link_inventory(conn,path,dry_run=False): return upsert_newsletter_link_inventory(conn,parse_newsletter_link_inventory(Path(path).read_text()),dry_run=dry_run)
def format_newsletter_link_inventory_import_json(s): return dump_json(s)
def format_newsletter_link_inventory_import_text(s): return f"Newsletter Link Inventory Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _url(v):
    raw=text(v); p=urlsplit(raw); return urlunsplit((p.scheme.lower() or "https",p.netloc.lower(),p.path,p.query,""))
def _int(v):
    try: return int(v)
    except (TypeError,ValueError): return None
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw); return d.get("links",[d]) if isinstance(d,dict) else d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
