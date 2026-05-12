"""Measure GitHub activity conversion into generated and published content."""

from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30


def build_github_activity_conversion_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repository: str | None = None,
    activity_type: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a conversion report grouped by activity type and repository."""
    if days <= 0:
        raise ValueError("days must be positive")
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    activities = _activity_rows(conn, schema, cutoff, repository, activity_type)
    links = _generated_links(conn, schema)
    published_ids = _published_content_ids(conn, schema)

    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for activity in activities:
        key = (activity["activity_type"] or "unknown", activity["repository"] or "unknown")
        group = groups.setdefault(
            key,
            {
                "activity_type": key[0],
                "repository": key[1],
                "ingested": 0,
                "linked_to_content": 0,
                "published": 0,
                "unpublished": 0,
                "conversion_rate": 0.0,
                "sample_activity_ids": [],
            },
        )
        group["ingested"] += 1
        refs = {str(activity["id"]), activity["logical_activity_id"]}
        content_ids = sorted({cid for ref in refs for cid in links.get(ref, set())})
        if content_ids:
            group["linked_to_content"] += 1
            if any(cid in published_ids for cid in content_ids):
                group["published"] += 1
            else:
                group["unpublished"] += 1
        _append_sample(group["sample_activity_ids"], activity["logical_activity_id"])

    rows = []
    for group in groups.values():
        group["conversion_rate"] = (
            round(group["published"] / group["ingested"], 4) if group["ingested"] else 0.0
        )
        rows.append(group)
    rows.sort(key=lambda item: (item["activity_type"], item["repository"]))
    return {
        "artifact_type": "github_activity_conversion",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "repository": repository,
            "activity_type": activity_type,
            "lookback_start": cutoff.isoformat(),
        },
        "totals": {
            "ingested": sum(row["ingested"] for row in rows),
            "linked_to_content": sum(row["linked_to_content"] for row in rows),
            "published": sum(row["published"] for row in rows),
            "unpublished": sum(row["unpublished"] for row in rows),
        },
        "groups": rows,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": "github_activity" in schema,
            "message": "No GitHub activity found for selected filters." if not rows else None,
        },
    }


def format_github_activity_conversion_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_conversion_text(report: dict[str, Any]) -> str:
    lines = [
        "GitHub Activity Conversion",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['days']} days "
            f"repository={report['filters']['repository'] or 'all'} "
            f"activity_type={report['filters']['activity_type'] or 'all'}"
        ),
        (
            "Totals: "
            f"ingested={report['totals']['ingested']} "
            f"linked={report['totals']['linked_to_content']} "
            f"published={report['totals']['published']}"
        ),
    ]
    if not report["groups"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Groups:"])
    for row in report["groups"]:
        lines.append(
            f"- {row['repository']} {row['activity_type']} "
            f"ingested={row['ingested']} linked={row['linked_to_content']} "
            f"published={row['published']} unpublished={row['unpublished']} "
            f"rate={row['conversion_rate']:.4f}"
        )
    return "\n".join(lines)


def _activity_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    repository: str | None,
    activity_type: str | None,
) -> list[dict[str, Any]]:
    columns = schema.get("github_activity")
    if not columns or not {"id", "repo_name", "activity_type", "number"}.issubset(columns):
        return []
    where = []
    params: list[Any] = []
    if "updated_at" in columns:
        where.append("updated_at >= ?")
        params.append(cutoff.isoformat())
    if repository:
        where.append("repo_name = ?")
        params.append(repository)
    if activity_type:
        where.append("activity_type = ?")
        params.append(activity_type)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT id, repo_name, activity_type, number
           FROM github_activity
           {where_sql}
           ORDER BY repo_name ASC, activity_type ASC, number ASC""",
        params,
    ).fetchall()
    return [
        {
            "id": int(row["id"]),
            "repository": row["repo_name"],
            "activity_type": row["activity_type"],
            "logical_activity_id": f"{row['repo_name']}#{row['number']}:{row['activity_type']}",
        }
        for row in rows
    ]


def _generated_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[str, set[int]]:
    columns = schema.get("generated_content")
    if not columns or not {"id", "source_activity_ids"}.issubset(columns):
        return {}
    links: dict[str, set[int]] = defaultdict(set)
    rows = conn.execute(
        """SELECT id, source_activity_ids
           FROM generated_content
           WHERE source_activity_ids IS NOT NULL AND source_activity_ids != ''"""
    ).fetchall()
    for row in rows:
        for ref in _parse_json_list(row["source_activity_ids"]):
            if ref is not None:
                links[str(ref)].add(int(row["id"]))
    return links


def _published_content_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[int]:
    ids: set[int] = set()
    gc_columns = schema.get("generated_content", set())
    if {"id", "published"}.issubset(gc_columns):
        rows = conn.execute("SELECT id FROM generated_content WHERE COALESCE(published, 0) = 1").fetchall()
        ids.update(int(row["id"]) for row in rows)
    cp_columns = schema.get("content_publications", set())
    if {"content_id", "status"}.issubset(cp_columns):
        rows = conn.execute(
            "SELECT content_id FROM content_publications WHERE LOWER(status) = 'published'"
        ).fetchall()
        ids.update(int(row["content_id"]) for row in rows)
    return ids


def _parse_json_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _append_sample(samples: list[str], value: str) -> None:
    if value not in samples and len(samples) < 5:
        samples.append(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
