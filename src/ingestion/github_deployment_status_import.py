"""Import GitHub deployment status snapshot records."""
from __future__ import annotations

import csv, io, json, sqlite3
from pathlib import Path
from typing import Any

from evaluation._batch_report_utils import dump_json, text

SCHEMA = """CREATE TABLE IF NOT EXISTS github_deployment_statuses (
repo TEXT NOT NULL,
deployment_id TEXT NOT NULL,
status TEXT NOT NULL,
created_at TEXT NOT NULL,
environment TEXT,
sha TEXT,
creator_login TEXT,
updated_at TEXT,
target_url TEXT,
raw_payload TEXT,
PRIMARY KEY (repo, deployment_id, status, created_at)
)"""


def parse_github_deployment_statuses(raw: str) -> list[dict[str, Any]]:
    rows = []
    for item in _records(raw):
        repo = text(item.get("repo") or item.get("repo_name") or item.get("full_name")).lower()
        deployment_id = text(item.get("deployment_id") or item.get("id"))
        status = text(item.get("status") or item.get("state")).lower()
        created_at = text(item.get("created_at"))
        if not repo or not deployment_id or not status:
            raise ValueError("repo, deployment_id, and status/state are required")
        rows.append(
            {
                "repo": repo,
                "deployment_id": deployment_id,
                "status": status,
                "created_at": created_at,
                "environment": text(item.get("environment")) or None,
                "sha": text(item.get("sha")) or None,
                "creator_login": _creator_login(item),
                "updated_at": text(item.get("updated_at")) or None,
                "target_url": text(item.get("target_url")) or None,
                "raw_payload": item.get("raw_payload") if isinstance(item.get("raw_payload"), str) else json.dumps(item, sort_keys=True),
            }
        )
    rows.sort(key=lambda r: (r["repo"], r["deployment_id"], r["created_at"], r["status"]))
    return rows


def upsert_github_deployment_statuses(conn: sqlite3.Connection, rows: list[dict[str, Any]], dry_run: bool = False) -> dict[str, Any]:
    if dry_run:
        return {"artifact_type": "github_deployment_status_import", "dry_run": True, "parsed_count": len(rows), "upserted_count": 0}
    conn.execute(SCHEMA)
    for row in rows:
        conn.execute(
            """INSERT INTO github_deployment_statuses VALUES (:repo,:deployment_id,:status,:created_at,:environment,:sha,:creator_login,:updated_at,:target_url,:raw_payload)
ON CONFLICT(repo,deployment_id,status,created_at) DO UPDATE SET environment=excluded.environment,sha=excluded.sha,creator_login=excluded.creator_login,updated_at=excluded.updated_at,target_url=excluded.target_url,raw_payload=excluded.raw_payload""",
            row,
        )
    conn.commit()
    return {"artifact_type": "github_deployment_status_import", "dry_run": False, "parsed_count": len(rows), "upserted_count": len(rows)}


def import_github_deployment_statuses(conn: sqlite3.Connection, path: str | Path, dry_run: bool = False) -> dict[str, Any]:
    return upsert_github_deployment_statuses(conn, parse_github_deployment_statuses(Path(path).read_text()), dry_run=dry_run)


def format_github_deployment_status_import_json(summary: dict[str, Any]) -> str:
    return dump_json(summary)


def format_github_deployment_status_import_text(summary: dict[str, Any]) -> str:
    return f"GitHub Deployment Status Import\nparsed={summary['parsed_count']} upserted={summary['upserted_count']} dry_run={summary['dry_run']}"


def _creator_login(item: dict[str, Any]) -> str | None:
    creator = item.get("creator")
    if isinstance(creator, dict):
        return text(creator.get("login")) or None
    return text(item.get("creator_login")) or None


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
        if isinstance(data.get("statuses"), list) and (data.get("deployment_id") or data.get("id")):
            source = [data]
        else:
            source = data.get("deployment_statuses") or data.get("statuses") or data.get("records") or data.get("deployments") or [data]
    else:
        return []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        statuses = item.get("statuses")
        if isinstance(statuses, list):
            for status in statuses:
                if isinstance(status, dict):
                    rows.append({**status, "repo": item.get("repo") or item.get("repo_name"), "deployment_id": item.get("deployment_id") or item.get("id"), "environment": status.get("environment") or item.get("environment"), "sha": status.get("sha") or item.get("sha")})
        else:
            rows.append(item)
    return rows
