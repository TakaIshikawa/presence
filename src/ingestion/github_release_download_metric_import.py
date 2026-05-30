"""Import GitHub release asset download metric snapshots."""
from __future__ import annotations

import csv, io, json, sqlite3
from pathlib import Path
from typing import Any

from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS github_release_download_metrics (
repository TEXT NOT NULL,
release_tag TEXT NOT NULL,
asset_name TEXT NOT NULL,
snapshot_at TEXT NOT NULL,
download_count INTEGER,
asset_size_bytes INTEGER,
content_type TEXT,
browser_download_url TEXT,
fetched_at TEXT,
PRIMARY KEY (repository, release_tag, asset_name, snapshot_at))"""

def parse_github_release_download_metrics(raw: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in _records(raw):
        if isinstance(item.get("assets"), list):
            repo = _repo(item.get("repository") or item.get("repo") or item.get("full_name"))
            release_tag = text(item.get("release_tag") or item.get("tag_name") or item.get("tag"))
            snapshot_at = text(item.get("snapshot_at") or item.get("fetched_at") or item.get("published_at"))
            for asset in item["assets"]:
                rows.append(_row({**asset, "repository": repo, "release_tag": release_tag, "snapshot_at": snapshot_at, "fetched_at": asset.get("fetched_at") or item.get("fetched_at") or snapshot_at}))
        else:
            rows.append(_row(item))
    rows.sort(key=lambda r: (r["repository"], r["release_tag"], r["asset_name"], r["snapshot_at"]))
    return rows

def upsert_github_release_download_metrics(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {"artifact_type": "github_release_download_metric_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for row in rows:
        conn.execute(
            """INSERT INTO github_release_download_metrics VALUES
            (:repository,:release_tag,:asset_name,:snapshot_at,:download_count,:asset_size_bytes,:content_type,:browser_download_url,:fetched_at)
            ON CONFLICT(repository,release_tag,asset_name,snapshot_at) DO UPDATE SET
            download_count=excluded.download_count,asset_size_bytes=excluded.asset_size_bytes,content_type=excluded.content_type,
            browser_download_url=excluded.browser_download_url,fetched_at=excluded.fetched_at""",
            row,
        )
    conn.commit()
    return {"artifact_type": "github_release_download_metric_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}

def import_github_release_download_metrics(conn: sqlite3.Connection, path: str | Path, dry_run: bool = False) -> dict[str, Any]:
    return upsert_github_release_download_metrics(conn, parse_github_release_download_metrics(Path(path).read_text()), dry_run=dry_run)

def format_github_release_download_metric_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)

def format_github_release_download_metric_import_text(summary: dict[str, Any]) -> str:
    return f"GitHub Release Download Metric Import\nparsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"

def _row(item: dict[str, Any]) -> dict[str, Any]:
    repository = _repo(item.get("repository") or item.get("repo") or item.get("full_name"))
    release_tag = text(item.get("release_tag") or item.get("tag_name") or item.get("tag"))
    asset_name = text(item.get("asset_name") or item.get("name"))
    snapshot_at = text(item.get("snapshot_at") or item.get("fetched_at"))
    if not repository or not release_tag or not asset_name or not snapshot_at:
        raise ValueError("repository, release_tag, asset_name, and snapshot_at are required")
    return {
        "repository": repository,
        "release_tag": release_tag,
        "asset_name": asset_name,
        "snapshot_at": snapshot_at,
        "download_count": _int(item.get("download_count") or item.get("downloads")),
        "asset_size_bytes": _int(item.get("asset_size_bytes") or item.get("size") or item.get("size_bytes")),
        "content_type": text(item.get("content_type")) or None,
        "browser_download_url": text(item.get("browser_download_url") or item.get("download_url")) or None,
        "fetched_at": text(item.get("fetched_at") or snapshot_at) or None,
    }

def _repo(value: Any) -> str:
    raw = text(value).strip().removeprefix("https://github.com/").removeprefix("http://github.com/")
    parts = [p for p in raw.split("/") if p]
    return "/".join(parts[:2]).lower() if len(parts) >= 2 else raw.lower()

def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(float(value))

def _records(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    if raw[0] in "[{":
        data = json.loads(raw)
        if isinstance(data, dict):
            return data.get("releases") or data.get("rows") or [data]
        return data
    if "," in raw.splitlines()[0]:
        return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]
