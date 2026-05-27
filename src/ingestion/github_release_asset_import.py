"""Import GitHub release asset snapshot records."""
from __future__ import annotations

import csv, io, json, sqlite3
from pathlib import Path
from typing import Any

from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS github_release_assets (
repo_name TEXT NOT NULL,
release_tag TEXT NOT NULL,
asset_name TEXT NOT NULL,
captured_at TEXT NOT NULL,
download_count INTEGER NOT NULL,
size_bytes INTEGER NOT NULL,
content_type TEXT,
browser_download_url TEXT,
updated_at TEXT,
raw_payload TEXT,
PRIMARY KEY (repo_name, release_tag, asset_name, captured_at)
)"""


def parse_github_release_assets(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        repo = text(item.get("repo_name") or item.get("repo") or item.get("full_name")).lower()
        tag = text(item.get("release_tag") or item.get("tag_name") or item.get("tag"))
        name = text(item.get("asset_name") or item.get("name"))
        captured_at = text(item.get("captured_at"))
        if not repo or not tag or not name or not captured_at:
            raise ValueError("repo_name, release_tag, asset_name, and captured_at are required")
        download_count = _nonnegative_int(item.get("download_count"), "download_count")
        size_bytes = _nonnegative_int(item.get("size_bytes") if item.get("size_bytes") is not None else item.get("size"), "size_bytes")
        rows.append(
            {
                "repo_name": repo,
                "release_tag": tag,
                "asset_name": name,
                "download_count": download_count,
                "size_bytes": size_bytes,
                "content_type": text(item.get("content_type")) or None,
                "browser_download_url": text(item.get("browser_download_url")) or None,
                "updated_at": text(item.get("updated_at")) or None,
                "captured_at": captured_at,
                "raw_payload": item.get("raw_payload") if isinstance(item.get("raw_payload"), str) else json.dumps(item, sort_keys=True),
            }
        )
    rows.sort(key=lambda r: (r["repo_name"], r["release_tag"], r["asset_name"], r["captured_at"]))
    return rows


def upsert_github_release_assets(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {"artifact_type": "github_release_asset_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for row in rows:
        conn.execute(
            """INSERT INTO github_release_assets VALUES (:repo_name,:release_tag,:asset_name,:captured_at,:download_count,:size_bytes,:content_type,:browser_download_url,:updated_at,:raw_payload)
ON CONFLICT(repo_name,release_tag,asset_name,captured_at) DO UPDATE SET download_count=excluded.download_count,size_bytes=excluded.size_bytes,content_type=excluded.content_type,browser_download_url=excluded.browser_download_url,updated_at=excluded.updated_at,raw_payload=excluded.raw_payload""",
            row,
        )
    conn.commit()
    return {"artifact_type": "github_release_asset_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}


def import_github_release_assets(conn: sqlite3.Connection, path: str | Path, dry_run: bool = False) -> dict[str, Any]:
    return upsert_github_release_assets(conn, parse_github_release_assets(Path(path).read_text()), dry_run=dry_run)


def format_github_release_asset_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)


def format_github_release_asset_import_text(summary: dict[str, Any]) -> str:
    return f"GitHub Release Asset Import\nparsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"


def _nonnegative_int(value: Any, field: str) -> int:
    try:
        number = int(value if value not in (None, "") else 0)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be nonnegative") from exc
    if number < 0:
        raise ValueError(f"{field} must be nonnegative")
    return number


def _records(raw: str) -> list[dict[str, Any]]:
    raw = raw.strip()
    if not raw:
        return []
    if raw[0] in "[{":
        data = json.loads(raw)
        return list(_json_records(data))
    if "," in raw.splitlines()[0]:
        return list(csv.DictReader(io.StringIO(raw)))
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def _json_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        source = data
    elif isinstance(data, dict):
        if isinstance(data.get("assets"), list) and (data.get("tag_name") or data.get("release_tag") or data.get("tag")):
            source = [data]
        else:
            source = data.get("release_assets") or data.get("assets") or data.get("records") or data.get("releases") or [data]
    else:
        return []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        assets = item.get("assets")
        if isinstance(assets, list):
            for asset in assets:
                if isinstance(asset, dict):
                    rows.append({**asset, "repo_name": item.get("repo_name") or item.get("repo"), "release_tag": item.get("release_tag") or item.get("tag_name") or item.get("tag"), "captured_at": asset.get("captured_at") or item.get("captured_at")})
        else:
            rows.append(item)
    return rows
