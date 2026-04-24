"""Report GitHub activity that has not been used as generated-content source material."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from storage.db import Database

SUPPORTED_ACTIVITY_TYPES = {"issue", "pull_request", "release"}


@dataclass(frozen=True)
class GitHubActivityCoverageFilters:
    """Filters for the uncovered GitHub activity report."""

    repo: str | None = None
    activity_type: str | None = None
    state: str | None = None
    days: int | None = None


def _parse_json_list(value: Any) -> list[Any]:
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _activity_id(row: dict[str, Any]) -> str:
    return f"{row.get('repo_name')}#{row.get('number')}:{row.get('activity_type')}"


def _covered_activity_refs(db: Database) -> set[str]:
    refs: set[str] = set()
    rows = db.conn.execute(
        """SELECT source_activity_ids
           FROM generated_content
           WHERE source_activity_ids IS NOT NULL
             AND source_activity_ids != ''"""
    ).fetchall()
    for row in rows:
        for ref in _parse_json_list(row["source_activity_ids"]):
            if ref is not None:
                refs.add(str(ref))
    return refs


def _row_to_item(row: Any) -> dict[str, Any]:
    item = dict(row)
    item["activity_id"] = _activity_id(item)
    return {
        "id": item["id"],
        "activity_id": item["activity_id"],
        "repo": item["repo_name"],
        "repo_name": item["repo_name"],
        "activity_type": item["activity_type"],
        "number": item["number"],
        "title": item["title"],
        "state": item["state"],
        "url": item["url"],
        "updated_at": item["updated_at"],
    }


def _summary(items: list[dict[str, Any]], filters: GitHubActivityCoverageFilters) -> dict[str, Any]:
    by_activity_type: dict[str, int] = {}
    by_state: dict[str, int] = {}
    for item in items:
        activity_type = item["activity_type"] or "unknown"
        state = item["state"] or "unknown"
        by_activity_type[activity_type] = by_activity_type.get(activity_type, 0) + 1
        by_state[state] = by_state.get(state, 0) + 1

    return {
        "total": len(items),
        "by_activity_type": dict(sorted(by_activity_type.items())),
        "by_state": dict(sorted(by_state.items())),
        "filters": {
            "repo": filters.repo,
            "activity_type": filters.activity_type,
            "state": filters.state,
            "days": filters.days,
        },
    }


def uncovered_github_activity_report(
    db: Database,
    *,
    repo: str | None = None,
    activity_type: str | None = None,
    state: str | None = None,
    days: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return uncovered GitHub issue, pull request, and release activity.

    A GitHub activity row is considered covered when any generated content row
    references either its logical activity ID (``repo#number:type``) or its
    numeric ``github_activity.id`` in ``source_activity_ids``.
    """
    if activity_type and activity_type not in SUPPORTED_ACTIVITY_TYPES:
        raise ValueError(
            "activity_type must be one of "
            + ", ".join(sorted(SUPPORTED_ACTIVITY_TYPES))
        )
    if days is not None and days <= 0:
        raise ValueError("days must be greater than zero")

    filters = GitHubActivityCoverageFilters(
        repo=repo,
        activity_type=activity_type,
        state=state,
        days=days,
    )
    where = ["activity_type IN ('issue', 'pull_request', 'release')"]
    params: list[Any] = []

    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    if activity_type:
        where.append("activity_type = ?")
        params.append(activity_type)
    if state:
        where.append("LOWER(COALESCE(state, '')) = LOWER(?)")
        params.append(state)
    if days is not None:
        reference_time = now or datetime.now(timezone.utc)
        if reference_time.tzinfo is None:
            reference_time = reference_time.replace(tzinfo=timezone.utc)
        cutoff = (reference_time - timedelta(days=days)).isoformat()
        where.append("updated_at >= ?")
        params.append(cutoff)

    covered_refs = _covered_activity_refs(db)
    cursor = db.conn.execute(
        f"""SELECT id, repo_name, activity_type, number, title, state, url, updated_at
            FROM github_activity
            WHERE {' AND '.join(where)}
            ORDER BY updated_at DESC, id DESC""",
        tuple(params),
    )

    items = []
    for row in cursor.fetchall():
        item = _row_to_item(row)
        if str(item["id"]) in covered_refs or item["activity_id"] in covered_refs:
            continue
        items.append(item)

    return {
        "summary": _summary(items, filters),
        "items": items,
    }


def format_github_activity_coverage_text(report: dict[str, Any]) -> str:
    """Format the uncovered activity report for terminal output."""
    summary = report["summary"]
    items = report["items"]
    if not items:
        return "No uncovered GitHub activity found."

    lines = [
        "Uncovered GitHub Activity",
        f"Total: {summary['total']}",
        "",
        "By activity type:",
    ]
    for activity_type, count in summary["by_activity_type"].items():
        lines.append(f"  {activity_type}: {count}")

    lines.append("")
    lines.append("By state:")
    for state, count in summary["by_state"].items():
        lines.append(f"  {state}: {count}")

    lines.append("")
    lines.append("Items:")
    for item in items:
        lines.append(
            f"- {item['repo']} {item['activity_type']} #{item['number']} "
            f"[{item['state'] or '-'}] {item['updated_at']}: {item['title']} "
            f"({item['url'] or '-'})"
        )
    return "\n".join(lines)
