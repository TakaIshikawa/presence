"""Import Mastodon account metric snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS mastodon_account_metrics (instance TEXT NOT NULL, account_id TEXT NOT NULL, acct TEXT NOT NULL, snapshot_at TEXT NOT NULL, followers_count INTEGER, following_count INTEGER, statuses_count INTEGER, display_name TEXT, bot INTEGER, locked INTEGER, profile_url TEXT, PRIMARY KEY (instance, account_id, acct, snapshot_at))"""
def parse_mastodon_account_metrics(raw:str)->list[dict[str,Any]]:
    rows=[_row(i) for i in _records(raw)]
    rows.sort(key=lambda r:(r["instance"],r["account_id"],r["acct"],r["snapshot_at"])); return rows
def upsert_mastodon_account_metrics(conn:sqlite3.Connection,rows:list[dict[str,Any]],dry_run:bool=False)->dict[str,Any]:
    if dry_run: return {"artifact_type":"mastodon_account_metric_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO mastodon_account_metrics VALUES (:instance,:account_id,:acct,:snapshot_at,:followers_count,:following_count,:statuses_count,:display_name,:bot,:locked,:profile_url) ON CONFLICT(instance,account_id,acct,snapshot_at) DO UPDATE SET followers_count=excluded.followers_count,following_count=excluded.following_count,statuses_count=excluded.statuses_count,display_name=excluded.display_name,bot=excluded.bot,locked=excluded.locked,profile_url=excluded.profile_url""",r)
    conn.commit(); return {"artifact_type":"mastodon_account_metric_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_mastodon_account_metrics(conn,path,dry_run=False): return upsert_mastodon_account_metrics(conn,parse_mastodon_account_metrics(Path(path).read_text()),dry_run=dry_run)
def format_mastodon_account_metric_import_json(s): return dump_json(s)
def format_mastodon_account_metric_import_text(s): return f"Mastodon Account Metric Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _row(i):
    url=text(i.get("profile_url") or i.get("url"))
    instance=text(i.get("instance")) or (urlsplit(url).netloc.lower() if url else "")
    acct=_acct(i.get("acct") or i.get("handle") or i.get("username"))
    account_id=text(i.get("account_id") or i.get("id") or acct)
    snapshot_at=text(i.get("snapshot_at") or i.get("fetched_at") or i.get("observed_at"))
    if not instance or not (account_id or acct) or not snapshot_at: raise ValueError("instance, account_id or acct, and snapshot_at are required")
    return {"instance":instance.lower(),"account_id":account_id or acct,"acct":acct,"snapshot_at":snapshot_at,"followers_count":_int(i.get("followers_count")),"following_count":_int(i.get("following_count")),"statuses_count":_int(i.get("statuses_count")),"display_name":text(i.get("display_name")) or None,"bot":_bool(i.get("bot")),"locked":_bool(i.get("locked")),"profile_url":url or None}
def _acct(v): return text(v).lstrip("@").lower()
def _int(v):
    if v in (None,""): return None
    return int(float(v))
def _bool(v): return 1 if str(v).lower() in {"1","true","yes"} else 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw); return d.get("accounts") or d.get("rows") or ([d] if isinstance(d,dict) else d)
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
