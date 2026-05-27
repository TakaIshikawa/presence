"""Import Search Console query-level blog metrics."""
from __future__ import annotations
import csv, hashlib, io, json, sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS blog_search_console_queries (
id TEXT PRIMARY KEY, canonical_path TEXT NOT NULL, search_query TEXT NOT NULL, metric_date TEXT NOT NULL, country TEXT, device TEXT,
clicks INTEGER, impressions INTEGER, ctr REAL, position REAL)"""


def parse_blog_search_console_queries(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        path = _path(item.get("canonical_path") or item.get("url") or item.get("page") or item.get("path"))
        query = text(item.get("search_query") or item.get("query"))
        metric_date = text(item.get("date") or item.get("metric_date"))
        if not path or not query or not metric_date: raise ValueError("canonical_path, query, and date are required")
        country = text(item.get("country")) or None; device = text(item.get("device")) or None
        row = {"id": _id(path, query, metric_date, country, device), "canonical_path": path, "search_query": query, "metric_date": metric_date, "country": country, "device": device, "clicks": _int(item.get("clicks")), "impressions": _int(item.get("impressions")), "ctr": _float(item.get("ctr")), "position": _float(item.get("position"))}
        rows.append(row)
    rows.sort(key=lambda r: r["id"]); return rows


def upsert_blog_search_console_queries(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False):
    if dry_run: return {"artifact_type": "blog_search_console_query_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for r in rows:
        conn.execute("""INSERT INTO blog_search_console_queries VALUES (:id,:canonical_path,:search_query,:metric_date,:country,:device,:clicks,:impressions,:ctr,:position)
        ON CONFLICT(id) DO UPDATE SET clicks=excluded.clicks,impressions=excluded.impressions,ctr=excluded.ctr,position=excluded.position,country=excluded.country,device=excluded.device""", r)
    conn.commit(); return {"artifact_type": "blog_search_console_query_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}


def import_blog_search_console_queries(conn, path, dry_run=False): return upsert_blog_search_console_queries(conn, parse_blog_search_console_queries(Path(path).read_text()), dry_run=dry_run)
def format_blog_search_console_query_import_json(s): return dump_json(s)
def format_blog_search_console_query_import_text(s): return f"Blog Search Console Query Import\nparsed={s['parsed_count']} upserted={s['upserted_count']} dry_run={s['dry_run']}"


def _records(raw: str):
    raw = raw.strip()
    if not raw: return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict): return data.get("queries") or data.get("items") or [data]
        return data
    if "," in raw.splitlines()[0]: return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _path(value: Any) -> str:
    raw = text(value); p = urlsplit(raw)
    return p.path or raw.split("?", 1)[0].split("#", 1)[0] or "/"
def _int(v):
    try: return max(0, int(float(str(v).strip().replace(",", ""))))
    except (TypeError, ValueError): return 0
def _float(v):
    try:
        t = str(v).strip().replace("%", "")
        f = float(t)
        return f / 100 if "%" in str(v) else f
    except (TypeError, ValueError): return 0.0
def _id(*parts): return hashlib.sha256("|".join(text(p) for p in parts).encode()).hexdigest()[:24]
