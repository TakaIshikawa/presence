from __future__ import annotations
import json, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def _clean(v): return '' if v is None else str(v).strip()
def load_redirect_snapshot_rows(path: str | Path) -> list[dict[str, Any]]:
    text=Path(path).read_text().strip()
    if not text: return []
    if text[:1]=='[': data=json.loads(text)
    else: data=[json.loads(line) for line in text.splitlines() if line.strip()]
    if not isinstance(data, list): raise ValueError('input must be a JSON array or JSONL records')
    return [dict(x) for x in data]
def ensure_table(conn: sqlite3.Connection):
    conn.execute("""CREATE TABLE IF NOT EXISTS knowledge_source_redirect_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT, url TEXT, final_url TEXT,
        status_code INTEGER, redirect_count INTEGER, checked_at TEXT, error TEXT,
        UNIQUE(source_id, url, checked_at))""")
def import_knowledge_source_redirect_snapshots(conn: sqlite3.Connection, rows: list[dict[str, Any]], *, dry_run=False, max_redirects: int | None=None) -> dict[str, Any]:
    conn.row_factory=sqlite3.Row; ensure_table(conn); imported=skipped=errored=0; errors=[]
    for row in rows:
        source_id=_clean(row.get('source_id')); url=_clean(row.get('url')); checked=_clean(row.get('checked_at')) or datetime.now(timezone.utc).isoformat()
        try: redirects=int(row.get('redirect_count') or 0)
        except (TypeError,ValueError): redirects=0
        if max_redirects is not None and redirects>max_redirects: skipped+=1; continue
        if not (source_id or url): errored+=1; errors.append({'row':row,'error':'source_id or url required'}); continue
        if dry_run: imported+=1; continue
        try:
            cur=conn.execute("""INSERT OR IGNORE INTO knowledge_source_redirect_snapshots
                (source_id,url,final_url,status_code,redirect_count,checked_at,error) VALUES (?,?,?,?,?,?,?)""",
                (source_id or None, url or None, _clean(row.get('final_url')) or None, row.get('status_code'), redirects, checked, _clean(row.get('error')) or None))
            if cur.rowcount: imported+=1
            else: skipped+=1
        except sqlite3.Error as exc:
            errored+=1; errors.append({'row':row,'error':str(exc)})
    if not dry_run: conn.commit()
    return {'artifact_type':'knowledge_source_redirect_snapshot_import','imported':imported,'skipped':skipped,'errored':errored,'errors':errors}
