"""Small helpers for file-backed batch imports."""
from __future__ import annotations
import csv, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse
def read_records(path: str | Path) -> list[dict[str, Any]]:
    p=Path(path); text=p.read_text(encoding='utf-8')
    if p.suffix.lower()=='.jsonl': return [json.loads(line) for line in text.splitlines() if line.strip()]
    if p.suffix.lower()=='.json':
        data=json.loads(text or '[]'); return data if isinstance(data,list) else data.get('records',[])
    with p.open(newline='',encoding='utf-8') as h: return list(csv.DictReader(h))
def conn(db_or_conn: Any) -> sqlite3.Connection:
    c=getattr(db_or_conn,'conn',db_or_conn); c.row_factory=sqlite3.Row; return c
def truthy(v: Any) -> int:
    return 1 if str(v).strip().lower() in {'1','true','yes','y'} else 0
def intval(v: Any, default: int=0) -> int:
    try: return int(v)
    except (TypeError,ValueError): return default
def floatval(v: Any, default: float=0.0) -> float:
    try: return float(v)
    except (TypeError,ValueError): return default
def clean(v: Any) -> str: return '' if v is None else str(v).strip()
def canonical_path(value: str) -> str:
    text=clean(value); parsed=urlparse(text)
    path=parsed.path if parsed.scheme or parsed.netloc else text.split('?',1)[0]
    return '/' + path.strip('/') if path.strip('/') else '/'
def normalize_url(value: str) -> str:
    p=urlparse(clean(value)); host=(p.netloc or '').lower(); return urlunparse((p.scheme.lower() or 'https',host,p.path or '/', '', p.query, ''))
def summary(name: str, parsed: int, upserted: int, dry_run: bool) -> dict[str, Any]:
    return {'artifact_type':name,'parsed':parsed,'upserted':upserted,'dry_run':dry_run}
def dumps(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True)
