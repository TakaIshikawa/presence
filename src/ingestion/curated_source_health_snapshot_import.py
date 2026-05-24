"""Import curated source health snapshots from JSON or JSONL."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from evaluation._batch_report_utils import dump_json, text


SCHEMA = """CREATE TABLE IF NOT EXISTS curated_source_health_snapshots (
    url TEXT NOT NULL,
    status TEXT,
    checked_at TEXT NOT NULL,
    http_code INTEGER,
    checksum TEXT,
    error TEXT,
    PRIMARY KEY (url, checked_at)
)"""


def parse_curated_source_health_snapshots(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    data: Any
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict):
            data = data.get("snapshots", [data])
    else:
        data = [json.loads(line) for line in raw.splitlines() if line.strip()]
    rows = []
    for item in data:
        url = _normalize_url(item.get("url") or item.get("source_url"))
        checked_at = text(item.get("checked_at") or item.get("crawled_at") or item.get("timestamp"))
        if not url or not checked_at:
            raise ValueError("snapshot url and checked_at are required")
        rows.append({"url": url, "status": text(item.get("status")) or None, "checked_at": checked_at, "http_code": item.get("http_code") or item.get("status_code"), "checksum": text(item.get("checksum")) or None, "error": text(item.get("error")) or None})
    rows.sort(key=lambda r: (r["url"], r["checked_at"]))
    return rows


def upsert_curated_source_health_snapshots(conn: sqlite3.Connection, snapshots: list[dict[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {"artifact_type": "curated_source_health_snapshot_import", "dry_run": True, "parsed_count": len(snapshots), "upserted_count": 0}
    conn.execute(SCHEMA)
    for row in snapshots:
        conn.execute(
            """INSERT INTO curated_source_health_snapshots (url, status, checked_at, http_code, checksum, error)
               VALUES (:url, :status, :checked_at, :http_code, :checksum, :error)
               ON CONFLICT(url, checked_at) DO UPDATE SET status=excluded.status, http_code=excluded.http_code, checksum=excluded.checksum, error=excluded.error""",
            row,
        )
    conn.commit()
    return {"artifact_type": "curated_source_health_snapshot_import", "dry_run": False, "parsed_count": len(snapshots), "upserted_count": len(snapshots)}


def import_curated_source_health_snapshots(conn: sqlite3.Connection, path: str | Path, *, dry_run: bool = False) -> dict[str, Any]:
    snapshots = parse_curated_source_health_snapshots(Path(path).read_text())
    return upsert_curated_source_health_snapshots(conn, snapshots, dry_run=dry_run)


def format_curated_source_health_snapshot_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)


def format_curated_source_health_snapshot_import_text(summary: dict[str, Any]) -> str:
    return f"Curated Source Health Snapshot Import\nparsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"


def _normalize_url(value: Any) -> str:
    raw = text(value)
    if not raw:
        return ""
    parts = urlsplit(raw)
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower()
    path = parts.path.rstrip("/") or "/"
    return urlunsplit((scheme, netloc, path, "", ""))
