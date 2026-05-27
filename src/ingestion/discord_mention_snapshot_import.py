"""Import Discord mention snapshots."""
from __future__ import annotations
import csv, hashlib, io, json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS discord_mention_snapshots (server_id TEXT, channel_id TEXT, message_id TEXT NOT NULL, author_handle TEXT, message_url TEXT, content TEXT, created_at TEXT, reaction_count INTEGER, reply_count INTEGER, captured_at TEXT NOT NULL, snapshot_key TEXT NOT NULL UNIQUE)"""
def parse_discord_mention_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw,("mentions","messages","items")):
        message_id=text(i.get("message_id") or i.get("id")); captured=text(i.get("captured_at") or i.get("snapshot_at") or i.get("collected_at") or i.get("created_at"))
        if not message_id or not captured: raise ValueError("message_id and captured_at/snapshot time are required")
        server=text(i.get("server_id") or i.get("guild_id")); channel=text(i.get("channel_id")); key=_key(server,channel,message_id,captured)
        rows.append({"server_id":server or None,"channel_id":channel or None,"message_id":message_id,"author_handle":_handle(i.get("author_handle") or i.get("author") or i.get("username")) or None,"message_url":_url(i.get("message_url") or i.get("url")) or None,"content":text(i.get("content") or i.get("text")) or None,"created_at":text(i.get("created_at")) or None,"reaction_count":_int(i.get("reaction_count") or i.get("reactions")),"reply_count":_int(i.get("reply_count") or i.get("replies")),"captured_at":captured,"snapshot_key":key})
    rows.sort(key=lambda r:r["snapshot_key"]); return rows
def upsert_discord_mention_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"discord_mention_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO discord_mention_snapshots VALUES (:server_id,:channel_id,:message_id,:author_handle,:message_url,:content,:created_at,:reaction_count,:reply_count,:captured_at,:snapshot_key) ON CONFLICT(snapshot_key) DO UPDATE SET server_id=excluded.server_id,channel_id=excluded.channel_id,message_id=excluded.message_id,author_handle=excluded.author_handle,message_url=excluded.message_url,content=excluded.content,created_at=excluded.created_at,reaction_count=excluded.reaction_count,reply_count=excluded.reply_count,captured_at=excluded.captured_at""",r)
    conn.commit(); return {"artifact_type":"discord_mention_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_discord_mention_snapshots(conn,path,dry_run=False): return upsert_discord_mention_snapshots(conn,parse_discord_mention_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_discord_mention_snapshot_import_json(s): return dump_json(s)
def format_discord_mention_snapshot_import_text(s): return f"Discord Mention Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _handle(v): return text(v).lstrip("@").strip()
def _url(v):
    raw=text(v)
    if not raw: return ""
    p=urlsplit(raw); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path,p.query,p.fragment))
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _key(*parts): return hashlib.sha256("|".join(text(p) for p in parts).encode()).hexdigest()
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
