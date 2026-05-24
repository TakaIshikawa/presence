"""Parse and import knowledge source status snapshots."""
from __future__ import annotations
import csv, json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit
def _clean(v): return "" if v is None else str(v).strip()
def _norm_url(v):
    p=urlsplit(_clean(v)); return urlunsplit((p.scheme.lower(),p.netloc.lower(),p.path or "/",p.query,"")) if p.scheme and p.netloc else _clean(v)
def parse_source_status_snapshots(path:str|Path,*,strict:bool=False)->list[dict[str,Any]]:
    p=Path(path); text=p.read_text().strip()
    if not text: return []
    if p.suffix.lower()==".csv": rows=[dict(r) for r in csv.DictReader(text.splitlines())]
    else:
        data=json.loads(text) if text[:1]=="[" else [json.loads(line) for line in text.splitlines() if line.strip()]
        if not isinstance(data,list): raise ValueError("input must be a JSON array, JSONL records, or CSV")
        rows=[dict(r) for r in data]
    out=[]
    for i,row in enumerate(rows,1):
        rec={"source_url":_norm_url(row.get("source_url") or row.get("url")),"status_code":row.get("status_code"),"content_hash":_clean(row.get("content_hash")) or None,"checked_at":_clean(row.get("checked_at")),"title":_clean(row.get("title")) or None,"canonical_url":_norm_url(row.get("canonical_url")) or None,"fetch_error":_clean(row.get("fetch_error") or row.get("error")) or None,"row_number":i}
        rec["errors"]=_errors(rec)
        if rec["errors"] and strict: raise ValueError(f"row {i}: {', '.join(rec['errors'])}")
        out.append(rec)
    return out
def build_source_status_snapshot_import_preview(records:list[dict[str,Any]],*,strict:bool=False)->dict[str,Any]:
    seen=set(); valid=[]; invalid=[]; dup=[]
    for r in records:
        errs=list(r.get("errors") or _errors(r)); key=(r.get("source_url"),r.get("checked_at"))
        if errs: invalid.append({**r,"errors":errs}); continue
        if key in seen: dup.append(r); continue
        seen.add(key); valid.append(r)
    if strict and invalid: raise ValueError("invalid source status snapshots")
    return {"artifact_type":"knowledge_source_status_snapshot_import","summary":{"input_count":len(records),"valid_count":len(valid),"invalid_count":len(invalid),"duplicate_count":len(dup)},"valid_records":valid,"invalid_records":invalid,"duplicate_records":dup}
def import_source_status_snapshots(conn:sqlite3.Connection,records:list[dict[str,Any]],*,dry_run:bool=False,strict:bool=False)->dict[str,Any]:
    preview=build_source_status_snapshot_import_preview(records,strict=strict); imported=0
    if not dry_run:
        conn.execute("""CREATE TABLE IF NOT EXISTS knowledge_source_status_snapshots (source_url TEXT, status_code INTEGER, content_hash TEXT, checked_at TEXT, title TEXT, canonical_url TEXT, fetch_error TEXT, updated_at TEXT, PRIMARY KEY(source_url, checked_at))""")
        for r in preview["valid_records"]:
            conn.execute("""INSERT INTO knowledge_source_status_snapshots VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(source_url,checked_at) DO UPDATE SET status_code=excluded.status_code, content_hash=excluded.content_hash, title=excluded.title, canonical_url=excluded.canonical_url, fetch_error=excluded.fetch_error, updated_at=excluded.updated_at""",(r["source_url"],r["status_code"],r["content_hash"],r["checked_at"],r["title"],r["canonical_url"],r["fetch_error"],datetime.now(timezone.utc).isoformat()))
            imported+=1
        conn.commit()
    return {**preview,"dry_run":dry_run,"imported_count":0 if dry_run else imported}
def format_source_status_snapshot_import_json(report): return json.dumps(report,indent=2,sort_keys=True)
def format_source_status_snapshot_import_text(report):
    s=report["summary"]; return "\n".join(["Knowledge Source Status Snapshot Import",f"Input: {s['input_count']} valid={s['valid_count']} invalid={s['invalid_count']} duplicates={s['duplicate_count']}",f"Dry run: {report.get('dry_run', False)} imported={report.get('imported_count', 0)}"])
def _errors(r):
    errs=[]
    if not _clean(r.get("source_url")).startswith(("http://","https://")): errs.append("invalid_source_url")
    try: int(r.get("status_code"))
    except (TypeError,ValueError): errs.append("invalid_status_code")
    try: datetime.fromisoformat(_clean(r.get("checked_at")).replace("Z","+00:00"))
    except ValueError: errs.append("invalid_checked_at")
    return errs
