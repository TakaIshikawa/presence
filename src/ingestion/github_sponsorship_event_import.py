"""Import GitHub sponsorship events."""
from __future__ import annotations
import csv, hashlib, io, json, re
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS github_sponsorship_events (sponsor_login TEXT NOT NULL, sponsor_id TEXT, event_type TEXT NOT NULL, tier_name TEXT, monthly_amount_cents INTEGER, occurred_at TEXT NOT NULL, repository TEXT, external_event_id TEXT, idempotency_key TEXT NOT NULL UNIQUE)"""
def parse_github_sponsorship_events(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("sponsorships","events","items")):
        login=text(i.get("sponsor_login") or i.get("login")).lower(); event=_event(i.get("event_type") or i.get("type")); occurred=text(i.get("occurred_at") or i.get("created_at"))
        if not login or not event or not occurred: raise ValueError("sponsor_login, event_type, and occurred_at are required")
        external=text(i.get("external_event_id") or i.get("id")) or None; key=external or _key(login,event,occurred)
        rows.append({"sponsor_login":login,"sponsor_id":text(i.get("sponsor_id")) or None,"event_type":event,"tier_name":text(i.get("tier_name") or i.get("tier")) or None,"monthly_amount_cents":_int(i.get("monthly_amount_cents") or i.get("amount_cents")),"occurred_at":occurred,"repository":text(i.get("repository") or i.get("repo")) or None,"external_event_id":external,"idempotency_key":key})
    rows.sort(key=lambda r:r["idempotency_key"]); return rows
def upsert_github_sponsorship_events(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"github_sponsorship_event_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO github_sponsorship_events VALUES (:sponsor_login,:sponsor_id,:event_type,:tier_name,:monthly_amount_cents,:occurred_at,:repository,:external_event_id,:idempotency_key) ON CONFLICT(idempotency_key) DO UPDATE SET sponsor_login=excluded.sponsor_login,sponsor_id=excluded.sponsor_id,event_type=excluded.event_type,tier_name=excluded.tier_name,monthly_amount_cents=excluded.monthly_amount_cents,occurred_at=excluded.occurred_at,repository=excluded.repository,external_event_id=excluded.external_event_id""",r)
    conn.commit(); return {"artifact_type":"github_sponsorship_event_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_github_sponsorship_events(conn,path,dry_run=False): return upsert_github_sponsorship_events(conn,parse_github_sponsorship_events(Path(path).read_text()),dry_run=dry_run)
def format_github_sponsorship_event_import_json(s): return dump_json(s)
def format_github_sponsorship_event_import_text(s): return f"GitHub Sponsorship Event Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _event(v): return re.sub(r"_+","_",re.sub(r"[^a-z0-9]+","_",text(v).lower())).strip("_")
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
