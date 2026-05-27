"""Import Buttondown device/client engagement exports."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS buttondown_device_clients (campaign_id TEXT NOT NULL, event_date TEXT NOT NULL, email_client TEXT NOT NULL, device_type TEXT NOT NULL, opens INTEGER, clicks INTEGER, recipients INTEGER, PRIMARY KEY (campaign_id, event_date, email_client, device_type))"""
def parse_buttondown_device_clients(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        campaign=text(i.get("campaign_id") or i.get("campaign_slug") or i.get("slug")); event_date=text(i.get("event_date") or i.get("date"))
        rows.append({"campaign_id":campaign,"event_date":event_date,"email_client":_label(i.get("email_client") or i.get("client")),"device_type":_label(i.get("device_type") or i.get("device")),"opens":_int(i.get("opens")),"clicks":_int(i.get("clicks")),"recipients":_int(i.get("recipients"))})
    rows.sort(key=lambda r:(r["campaign_id"],r["event_date"],r["email_client"],r["device_type"])); return rows
def upsert_buttondown_device_clients(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"buttondown_device_client_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO buttondown_device_clients VALUES (:campaign_id,:event_date,:email_client,:device_type,:opens,:clicks,:recipients) ON CONFLICT(campaign_id,event_date,email_client,device_type) DO UPDATE SET opens=excluded.opens,clicks=excluded.clicks,recipients=excluded.recipients""",r)
    conn.commit(); return {"artifact_type":"buttondown_device_client_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_buttondown_device_clients(conn,path,dry_run=False): return upsert_buttondown_device_clients(conn,parse_buttondown_device_clients(Path(path).read_text()),dry_run=dry_run)
def format_buttondown_device_client_import_json(s): return dump_json(s)
def format_buttondown_device_client_import_text(s): return f"Buttondown Device Client Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _label(v): return text(v).lower() or "unknown"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("items") or d.get("data") or d.get("rows") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
