"""Import GitHub release note records."""
from __future__ import annotations
import csv, io, json, sqlite3
from pathlib import Path
from typing import Any
from evaluation._batch_report_utils import dump_json, text
SCHEMA="""CREATE TABLE IF NOT EXISTS github_release_notes (repo TEXT NOT NULL, tag_name TEXT NOT NULL, title TEXT, body TEXT, published_at TEXT, author_login TEXT, html_url TEXT, prerelease INTEGER, draft INTEGER, PRIMARY KEY (repo, tag_name))"""
def parse_github_release_notes(raw:str)->list[dict[str,Any]]:
    rows=[]
    for i in _records(raw):
        repo=text(i.get("repo")).lower(); tag=text(i.get("tag_name") or i.get("tag"))
        if not repo or not tag: raise ValueError("repo and tag_name are required")
        rows.append({"repo":repo,"tag_name":tag,"title":text(i.get("title") or i.get("name")) or None,"body":text(i.get("body")) or None,"published_at":text(i.get("published_at")) or None,"author_login":text(i.get("author_login")) or None,"html_url":text(i.get("html_url")) or None,"prerelease":_bool(i.get("prerelease")),"draft":_bool(i.get("draft"))})
    rows.sort(key=lambda r:(r["repo"],r["tag_name"])); return rows
def upsert_github_release_notes(conn,rows,dry_run=False):
    if dry_run: return {"artifact_type":"github_release_note_import","dry_run":True,"parsed_count":len(rows),"upserted_count":0}
    conn.execute(SCHEMA)
    for r in rows: conn.execute("""INSERT INTO github_release_notes VALUES (:repo,:tag_name,:title,:body,:published_at,:author_login,:html_url,:prerelease,:draft) ON CONFLICT(repo,tag_name) DO UPDATE SET title=excluded.title,body=excluded.body,published_at=excluded.published_at,author_login=excluded.author_login,html_url=excluded.html_url,prerelease=excluded.prerelease,draft=excluded.draft""",r)
    conn.commit(); return {"artifact_type":"github_release_note_import","dry_run":False,"parsed_count":len(rows),"upserted_count":len(rows)}
def import_github_release_notes(conn,path,dry_run=False): return upsert_github_release_notes(conn,parse_github_release_notes(Path(path).read_text()),dry_run=dry_run)
def format_github_release_note_import_json(s): return dump_json(s)
def format_github_release_note_import_text(s): return f"GitHub Release Note Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"
def _bool(v): return 1 if str(v).lower() in {"1","true","yes"} else 0
def _records(raw):
    raw=raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        d=json.loads(raw); return d.get("releases",[d]) if isinstance(d,dict) else d
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(l) for l in raw.splitlines() if l.strip()]
