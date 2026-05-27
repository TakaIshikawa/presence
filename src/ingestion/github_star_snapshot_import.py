"""Import GitHub repository star snapshots."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS github_star_snapshots (repo TEXT NOT NULL, snapshot_at TEXT NOT NULL, stargazers_count INTEGER, watchers_count INTEGER, source TEXT, fetched_at TEXT, PRIMARY KEY (repo, snapshot_at))"""
def parse_github_star_snapshots(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        repo=text(i.get("repo") or i.get("repository") or i.get("full_name")).lower(); snapshot=text(i.get("snapshot_at") or i.get("date") or i.get("timestamp"))
        rows.append({"repo":repo,"snapshot_at":snapshot,"stargazers_count":_int(i.get("stargazers_count") or i.get("stars")),"watchers_count":_int(i.get("watchers_count") or i.get("watchers")),"source":text(i.get("source")) or None,"fetched_at":text(i.get("fetched_at")) or None})
    rows.sort(key=lambda r:(r["repo"],r["snapshot_at"])); return rows
def upsert_github_star_snapshots(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"github_star_snapshot_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO github_star_snapshots VALUES (:repo,:snapshot_at,:stargazers_count,:watchers_count,:source,:fetched_at) ON CONFLICT(repo,snapshot_at) DO UPDATE SET stargazers_count=excluded.stargazers_count,watchers_count=excluded.watchers_count,source=excluded.source,fetched_at=excluded.fetched_at""",r)
    conn.commit(); return {"artifact_type":"github_star_snapshot_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_github_star_snapshots(conn,path,dry_run=False): return upsert_github_star_snapshots(conn,parse_github_star_snapshots(Path(path).read_text()),dry_run=dry_run)
def format_github_star_snapshot_import_json(s): return dump_json(s)
def format_github_star_snapshot_import_text(s): return f"GitHub Star Snapshot Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _int(v):
    try: return int(float(v))
    except (TypeError,ValueError): return 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] not in "[{" and "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    if raw[0] in "[{":
        try:
            d=json.loads(raw); return d.get("snapshots") or d.get("items") or d.get("data") or [d] if isinstance(d,dict) else d
        except json.JSONDecodeError: pass
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
