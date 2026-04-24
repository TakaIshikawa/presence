"""Coverage report for ingested source material used by generated content."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


SOURCE_FIELDS = {
    "commits": "source_commits",
    "messages": "source_messages",
    "github_activity": "source_activity_ids",
}


def summarize_source_coverage(
    db_or_conn: Any,
    *,
    days: int = 30,
    repo: str | None = None,
    limit: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return uncovered source counts and representative uncovered items.

    The report compares source reference JSON stored on generated_content with
    ingested GitHub commits, Claude messages, and GitHub activity records.
    """
    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    since = now - timedelta(days=max(days, 0))
    warnings: list[str] = []
    covered = _covered_source_ids(conn, warnings)

    commits = _commit_coverage(conn, covered["commits"], since, repo, limit)
    messages = _message_coverage(conn, covered["messages"], since, limit)
    activity = _activity_coverage(conn, covered["github_activity"], since, repo, limit)

    return {
        "generated_at": now.isoformat(),
        "window": {
            "days": days,
            "since": since.isoformat(),
            "repo": repo,
            "limit": limit,
        },
        "summary": {
            "uncovered_total": (
                commits["uncovered_count"]
                + messages["uncovered_count"]
                + activity["uncovered_count"]
            ),
            "ingested_total": (
                commits["ingested_count"]
                + messages["ingested_count"]
                + activity["ingested_count"]
            ),
        },
        "commits": commits,
        "messages": messages,
        "github_activity": activity,
        "warnings": warnings,
    }


def format_source_coverage(report: dict[str, Any]) -> str:
    """Format source coverage as terminal text."""
    lines = [
        "=" * 72,
        "SOURCE MATERIAL COVERAGE",
        "=" * 72,
        f"Generated at: {report['generated_at']}",
        f"Window: last {report['window']['days']} days",
    ]
    if report["window"].get("repo"):
        lines.append(f"Repository: {report['window']['repo']}")
    lines.append("")

    for key, title in (
        ("commits", "GitHub commits"),
        ("messages", "Claude messages"),
        ("github_activity", "GitHub activity"),
    ):
        section = report[key]
        lines.append(
            f"{title}: {section['uncovered_count']} uncovered "
            f"of {section['ingested_count']} ingested"
        )
        for item in section["uncovered_items"]:
            lines.append(f"  - {_format_item(key, item)}")
        if not section["uncovered_items"]:
            lines.append("  - none")
        lines.append("")

    if report["warnings"]:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report["warnings"])

    return "\n".join(lines).rstrip()


def _covered_source_ids(
    conn: sqlite3.Connection,
    warnings: list[str],
) -> dict[str, set[str]]:
    covered = {key: set() for key in SOURCE_FIELDS}
    rows = conn.execute(
        """SELECT id, source_commits, source_messages, source_activity_ids
           FROM generated_content
           ORDER BY id ASC"""
    ).fetchall()
    for row in rows:
        content = dict(row)
        content_id = content["id"]
        for key, field in SOURCE_FIELDS.items():
            values = _json_list(content.get(field), field, content_id, warnings)
            covered[key].update(str(value) for value in values if value is not None)
    return covered


def _json_list(
    value: str | None,
    field: str,
    content_id: int,
    warnings: list[str],
) -> list[Any]:
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        warnings.append(
            f"generated_content {content_id} has malformed {field}: {exc.msg}"
        )
        return []
    if not isinstance(parsed, list):
        warnings.append(
            f"generated_content {content_id} has non-list {field}: {type(parsed).__name__}"
        )
        return []
    return parsed


def _commit_coverage(
    conn: sqlite3.Connection,
    covered: set[str],
    since: datetime,
    repo: str | None,
    limit: int,
) -> dict[str, Any]:
    where = ["timestamp >= ?"]
    params: list[Any] = [_db_time(since)]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    rows = _all(conn, "github_commits", where, params, "timestamp ASC, id ASC")
    uncovered = [row for row in rows if str(row["commit_sha"]) not in covered]
    return {
        "ingested_count": len(rows),
        "covered_count": len(rows) - len(uncovered),
        "uncovered_count": len(uncovered),
        "uncovered_items": [
            {
                "id": row["id"],
                "repo_name": row["repo_name"],
                "commit_sha": row["commit_sha"],
                "commit_message": row["commit_message"],
                "timestamp": row["timestamp"],
                "author": row["author"],
            }
            for row in uncovered[: max(limit, 0)]
        ],
    }


def _message_coverage(
    conn: sqlite3.Connection,
    covered: set[str],
    since: datetime,
    limit: int,
) -> dict[str, Any]:
    rows = _all(conn, "claude_messages", ["timestamp >= ?"], [_db_time(since)], "timestamp ASC, id ASC")
    uncovered = [row for row in rows if str(row["message_uuid"]) not in covered]
    return {
        "ingested_count": len(rows),
        "covered_count": len(rows) - len(uncovered),
        "uncovered_count": len(uncovered),
        "uncovered_items": [
            {
                "id": row["id"],
                "session_id": row["session_id"],
                "message_uuid": row["message_uuid"],
                "project_path": row["project_path"],
                "timestamp": row["timestamp"],
                "prompt_text": row["prompt_text"],
            }
            for row in uncovered[: max(limit, 0)]
        ],
    }


def _activity_coverage(
    conn: sqlite3.Connection,
    covered: set[str],
    since: datetime,
    repo: str | None,
    limit: int,
) -> dict[str, Any]:
    where = ["updated_at >= ?"]
    params: list[Any] = [_db_time(since)]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    rows = _all(conn, "github_activity", where, params, "updated_at ASC, id ASC")
    uncovered = [
        row
        for row in rows
        if _activity_id(row["repo_name"], row["number"], row["activity_type"]) not in covered
    ]
    return {
        "ingested_count": len(rows),
        "covered_count": len(rows) - len(uncovered),
        "uncovered_count": len(uncovered),
        "uncovered_items": [
            {
                "id": row["id"],
                "activity_id": _activity_id(row["repo_name"], row["number"], row["activity_type"]),
                "repo_name": row["repo_name"],
                "activity_type": row["activity_type"],
                "number": row["number"],
                "title": row["title"],
                "state": row["state"],
                "updated_at": row["updated_at"],
                "url": row["url"],
            }
            for row in uncovered[: max(limit, 0)]
        ],
    }


def _all(
    conn: sqlite3.Connection,
    table: str,
    where: list[str],
    params: list[Any],
    order_by: str,
) -> list[dict[str, Any]]:
    sql = f"SELECT * FROM {table} WHERE {' AND '.join(where)} ORDER BY {order_by}"
    return [dict(row) for row in conn.execute(sql, tuple(params)).fetchall()]


def _format_item(section: str, item: dict[str, Any]) -> str:
    if section == "commits":
        return (
            f"{item['timestamp']} {item['repo_name']} {item['commit_sha']}: "
            f"{_shorten(item.get('commit_message'))}"
        )
    if section == "messages":
        return (
            f"{item['timestamp']} {item['message_uuid']}: "
            f"{_shorten(item.get('prompt_text'))}"
        )
    return (
        f"{item['updated_at']} {item['activity_id']}: "
        f"{_shorten(item.get('title'))}"
    )


def _shorten(value: Any, width: int = 90) -> str:
    text = "" if value is None else str(value).replace("\n", " ")
    return text if len(text) <= width else text[: width - 3] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _activity_id(repo_name: str, number: int | str, activity_type: str) -> str:
    return f"{repo_name}#{number}:{activity_type}"


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _db_time(value: datetime) -> str:
    return value.isoformat()
