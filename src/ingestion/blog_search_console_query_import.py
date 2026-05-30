"""Import Search Console query-level blog metrics."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sqlite3
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
        path = _path(item.get("canonical_path") or item.get("url") or item.get("page_url") or item.get("page") or item.get("path"))
        query = text(item.get("search_query") or item.get("query"))
        metric_date = text(item.get("metric_date") or item.get("date") or item.get("observed_at"))
        if not path or not query or not metric_date:
            raise ValueError("canonical_path, query, and date are required")
        country = text(item.get("country")) or None
        device = text(item.get("device")) or None
        row = {
            "id": _id(path, query, metric_date, country, device),
            "canonical_path": path,
            "search_query": query,
            "metric_date": metric_date,
            "country": country,
            "device": device,
            "clicks": _int(item.get("clicks")),
            "impressions": _int(item.get("impressions")),
            "ctr": _float(item.get("ctr")),
            "position": _float(item.get("position")),
            "observed_at": metric_date,
            "page_url": path,
            "query": query,
        }
        rows.append(row)
    rows.sort(key=lambda r: r["id"])
    return rows


def parse_blog_search_console_query_payload(raw: str) -> list[dict[str, Any]]:
    return parse_blog_search_console_queries(raw)


def upsert_blog_search_console_queries(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False):
    inserted_count, updated_count = _change_counts(conn, rows) if _table_exists(conn) else (len(rows), 0)
    if not dry_run:
        conn.execute(SCHEMA)
        for row in rows:
            conn.execute(
                """INSERT INTO blog_search_console_queries VALUES (:id,:canonical_path,:search_query,:metric_date,:country,:device,:clicks,:impressions,:ctr,:position)
                ON CONFLICT(id) DO UPDATE SET canonical_path=excluded.canonical_path,search_query=excluded.search_query,metric_date=excluded.metric_date,country=excluded.country,device=excluded.device,clicks=excluded.clicks,impressions=excluded.impressions,ctr=excluded.ctr,position=excluded.position""",
                row,
            )
        conn.commit()
    applied_count = 0 if dry_run else len(rows)
    return {
        "artifact_type": "blog_search_console_query_import",
        "dry_run": dry_run,
        "parsed_count": len(rows),
        "upserted_count": applied_count,
        "summary": {
            "parsed_count": len(rows),
            "inserted_count": inserted_count,
            "updated_count": updated_count,
            "applied_count": applied_count,
        },
        "rows": rows,
    }


def import_blog_search_console_queries(conn: sqlite3.Connection, source, dry_run: bool = False, now=None):
    rows = source if isinstance(source, list) else parse_blog_search_console_queries(Path(source).read_text())
    return upsert_blog_search_console_queries(conn, rows, dry_run=dry_run)


def format_blog_search_console_query_import_json(summary):
    return dump_json(summary)


def format_blog_search_console_query_import_text(summary):
    return (
        "Blog Search Console Query Import\n"
        f"parsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"
    )


def _records(raw: str):
    raw = raw.strip()
    if not raw:
        return []
    if raw[0] in "[{":
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return [json.loads(line) for line in raw.splitlines() if line.strip()]
        if isinstance(data, dict):
            return data.get("queries") or data.get("rows") or data.get("items") or [data]
        return data
    if "," in raw.splitlines()[0]:
        return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _path(value: Any) -> str:
    raw = text(value)
    parsed = urlsplit(raw)
    if parsed.scheme or parsed.netloc:
        return parsed.path or "/"
    return raw.split("?", 1)[0].split("#", 1)[0] or "/"


def _int(value):
    try:
        return max(0, int(float(str(value).strip().replace(",", ""))))
    except (TypeError, ValueError):
        return 0


def _float(value):
    try:
        raw = str(value).strip()
        number = float(raw.replace(",", "").replace("%", ""))
        return number / 100 if "%" in raw else number
    except (TypeError, ValueError):
        return 0.0


def _id(*parts):
    return hashlib.sha256("|".join(text(part) for part in parts).encode()).hexdigest()[:24]


def _table_exists(conn: sqlite3.Connection) -> bool:
    return conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'blog_search_console_queries'"
    ).fetchone() is not None


def _change_counts(conn: sqlite3.Connection, rows: list[dict[str, Any]]) -> tuple[int, int]:
    existing = {
        row[0]
        for row in conn.execute("SELECT id FROM blog_search_console_queries")
    }
    inserted_count = sum(1 for row in rows if row["id"] not in existing)
    return inserted_count, len(rows) - inserted_count
