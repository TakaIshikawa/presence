"""Import Mastodon status snapshots."""
from __future__ import annotations
import csv, html, io, json, re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS mastodon_status_snapshots (instance TEXT NOT NULL, account_acct TEXT, status_id TEXT NOT NULL, uri TEXT, url TEXT, created_at TEXT, content_text TEXT, replies_count INTEGER, reblogs_count INTEGER, favourites_count INTEGER, language TEXT, visibility TEXT, PRIMARY KEY (instance, status_id))"""
def parse_mastodon_status_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        account=i.get("account") if isinstance(i.get("account"),dict) else {}
        rows.append({"instance":_instance(i),"account_acct":text(i.get("account_acct") or i.get("acct") or account.get("acct") or account.get("username")) or None,"status_id":text(i.get("status_id") or i.get("id")),"uri":text(i.get("uri")) or None,"url":text(i.get("url")) or None,"created_at":text(i.get("created_at")) or None,"content_text":_plain_text(i.get("content_text") if i.get("content_text") is not None else i.get("content")) or None,"replies_count":_int(i.get("replies_count")),"reblogs_count":_int(i.get("reblogs_count") or i.get("boosts_count")),"favourites_count":_int(i.get("favourites_count") or i.get("favorites_count")),"language":text(i.get("language")) or None,"visibility":text(i.get("visibility")) or None})
    rows.sort(key=lambda r:(r["instance"],r["status_id"])); return rows
def upsert_mastodon_status_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"mastodon_status_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO mastodon_status_snapshots VALUES (:instance,:account_acct,:status_id,:uri,:url,:created_at,:content_text,:replies_count,:reblogs_count,:favourites_count,:language,:visibility) ON CONFLICT(instance,status_id) DO UPDATE SET account_acct=excluded.account_acct,uri=excluded.uri,url=excluded.url,created_at=excluded.created_at,content_text=excluded.content_text,replies_count=excluded.replies_count,reblogs_count=excluded.reblogs_count,favourites_count=excluded.favourites_count,language=excluded.language,visibility=excluded.visibility""",r)
    conn.commit(); return {"artifact_type":"mastodon_status_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_mastodon_status_snapshots(conn,path,dry_run=False): return upsert_mastodon_status_snapshots(conn,parse_mastodon_status_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_mastodon_status_snapshot_import_json(s): return dump_json(s)
def format_mastodon_status_snapshot_import_text(s): return f"Mastodon Status Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _instance(i):
    direct=text(i.get("instance") or i.get("server") or i.get("host")).lower()
    if direct: return direct.removeprefix("https://").removeprefix("http://").strip("/")
    for key in ("uri","url"):
        m=re.match(r"https?://([^/]+)",text(i.get(key)))
        if m: return m.group(1).lower()
    return ""
def _plain_text(value):
    if value is None: return ""
    parser=_TextParser(); parser.feed(text(value)); parser.close()
    return re.sub(r"\s+"," ",html.unescape(parser.value())).strip()
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("statuses") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
class _TextParser(HTMLParser):
    def __init__(self):
        super().__init__(); self.parts=[]
    def handle_data(self,data): self.parts.append(data)
    def handle_entityref(self,name): self.parts.append(f"&{name};")
    def handle_charref(self,name): self.parts.append(f"&#{name};")
    def handle_starttag(self,tag,attrs):
        if tag in {"br","p","div","li"}: self.parts.append(" ")
    def value(self): return "".join(self.parts)
