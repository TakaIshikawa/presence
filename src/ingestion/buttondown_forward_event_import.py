"""Import Buttondown forward/share events."""
from __future__ import annotations
import csv, hashlib, io, json
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS buttondown_forward_events (campaign_id TEXT NOT NULL, subscriber_email TEXT NOT NULL, event_id TEXT, forwarded_at TEXT NOT NULL, recipient_count INTEGER, link_url TEXT, source TEXT, idempotency_key TEXT NOT NULL UNIQUE)"""
def parse_buttondown_forward_events(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("events","forwards","items")):
        campaign=text(i.get("campaign_id") or i.get("newsletter_id")); email=text(i.get("subscriber_email") or i.get("email")).lower(); forwarded=text(i.get("forwarded_at") or i.get("created_at"))
        if not campaign or not email or not forwarded: raise ValueError("campaign_id, subscriber_email, and forwarded_at are required")
        event_id=text(i.get("event_id") or i.get("id")) or None; key=event_id or _key(campaign,email,forwarded)
        rows.append({"campaign_id":campaign,"subscriber_email":email,"event_id":event_id,"forwarded_at":forwarded,"recipient_count":_int(i.get("recipient_count") or i.get("recipients")),"link_url":text(i.get("link_url") or i.get("url")) or None,"source":text(i.get("source")) or None,"idempotency_key":key})
    rows.sort(key=lambda r:r["idempotency_key"]); return rows
def upsert_buttondown_forward_events(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"buttondown_forward_event_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO buttondown_forward_events VALUES (:campaign_id,:subscriber_email,:event_id,:forwarded_at,:recipient_count,:link_url,:source,:idempotency_key) ON CONFLICT(idempotency_key) DO UPDATE SET campaign_id=excluded.campaign_id,subscriber_email=excluded.subscriber_email,event_id=excluded.event_id,forwarded_at=excluded.forwarded_at,recipient_count=excluded.recipient_count,link_url=excluded.link_url,source=excluded.source""",r)
    conn.commit(); return {"artifact_type":"buttondown_forward_event_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_buttondown_forward_events(conn,path,dry_run=False): return upsert_buttondown_forward_events(conn,parse_buttondown_forward_events(Path(path).read_text()),dry_run=dry_run)
def format_buttondown_forward_event_import_json(s): return dump_json(s)
def format_buttondown_forward_event_import_text(s): return f"Buttondown Forward Event Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _key(*parts): return hashlib.sha256("|".join(parts).encode()).hexdigest()
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
