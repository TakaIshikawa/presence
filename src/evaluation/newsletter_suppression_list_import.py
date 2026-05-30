"""Parse and import newsletter suppression records."""
from __future__ import annotations
import csv, json, re, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from ._report_utils import clean, json_dumps
EMAIL_RE=re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
def parse_newsletter_suppression_records(path:str|Path,*,strict:bool=False)->list[dict[str,Any]]:
    p=Path(path); text=p.read_text().strip()
    if not text: return []
    if p.suffix.lower()==".csv":
        rows=[dict(r) for r in csv.DictReader(text.splitlines())]
    else:
        data=json.loads(text) if text[:1]=="[" else [json.loads(line) for line in text.splitlines() if line.strip()]
        if not isinstance(data,list): raise ValueError("input must be a JSON array, JSONL records, or CSV")
        rows=[dict(r) for r in data]
    parsed=[]
    for i,row in enumerate(rows,1):
        rec=_normalize(row); rec["source_file"]=clean(row.get("source_file")) or str(p); rec["row_number"]=i
        errs=_errors(rec)
        if errs and strict: raise ValueError(f"row {i}: {', '.join(errs)}")
        rec["errors"]=errs; parsed.append(rec)
    return parsed
def build_newsletter_suppression_import_preview(records:list[dict[str,Any]],*,strict:bool=False)->dict[str,Any]:
    seen=set(); valid=[]; invalid=[]; duplicates=[]
    for r in records:
        errs=list(r.get("errors") or _errors(r)); key=(r.get("email"),r.get("provider"),r.get("reason"))
        if errs: invalid.append({**r,"errors":errs}); continue
        if key in seen: duplicates.append(r); continue
        seen.add(key); valid.append(r)
    if strict and invalid: raise ValueError("invalid suppression records")
    return {"artifact_type":"newsletter_suppression_list_import","summary":{"input_count":len(records),"valid_count":len(valid),"invalid_count":len(invalid),"duplicate_count":len(duplicates)},"valid_records":valid,"invalid_records":invalid,"duplicate_records":duplicates}
def import_newsletter_suppression_records(conn:sqlite3.Connection,records:list[dict[str,Any]],*,dry_run:bool=False,strict:bool=False)->dict[str,Any]:
    conn.row_factory=sqlite3.Row; preview=build_newsletter_suppression_import_preview(records,strict=strict); imported=updated=0
    if not dry_run:
        conn.execute("""CREATE TABLE IF NOT EXISTS newsletter_suppressions (email TEXT, provider TEXT, reason TEXT, suppressed_at TEXT, source_file TEXT, metadata TEXT, updated_at TEXT, PRIMARY KEY(email, provider, reason))""")
        for r in preview["valid_records"]:
            cur=conn.execute("""INSERT INTO newsletter_suppressions (email,provider,reason,suppressed_at,source_file,metadata,updated_at) VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(email,provider,reason) DO UPDATE SET suppressed_at=excluded.suppressed_at, source_file=excluded.source_file, metadata=excluded.metadata, updated_at=excluded.updated_at""",(r["email"],r["provider"],r["reason"],r["suppressed_at"],r["source_file"],json.dumps(r.get("metadata") or {},sort_keys=True),datetime.now(timezone.utc).isoformat()))
            imported+=1 if cur.rowcount else 0
        conn.commit()
    return {**preview,"dry_run":dry_run,"imported_count":0 if dry_run else imported,"updated_count":updated}
def format_newsletter_suppression_import_json(report): return json_dumps(report)
def format_newsletter_suppression_import_text(report):
    s=report["summary"]; return "\n".join(["Newsletter Suppression List Import",f"Input: {s['input_count']} valid={s['valid_count']} invalid={s['invalid_count']} duplicates={s['duplicate_count']}",f"Dry run: {report.get('dry_run', False)} imported={report.get('imported_count', 0)}"])
def _normalize(row):
    email=clean(row.get("email")).lower(); provider=clean(row.get("provider"),"unknown").lower(); reason=clean(row.get("reason"),"unspecified").lower(); when=clean(row.get("suppressed_at")) or datetime.now(timezone.utc).isoformat()
    return {"email":email,"provider":provider,"reason":reason,"suppressed_at":when,"metadata":{k:v for k,v in row.items() if k not in {"email","provider","reason","suppressed_at","source_file"}}}
def _errors(r):
    errs=[]
    if not EMAIL_RE.match(clean(r.get("email"))): errs.append("invalid_email")
    if not clean(r.get("reason")): errs.append("missing_reason")
    if not clean(r.get("provider")): errs.append("missing_provider")
    try: datetime.fromisoformat(clean(r.get("suppressed_at")).replace("Z","+00:00"))
    except ValueError: errs.append("invalid_suppressed_at")
    return errs
