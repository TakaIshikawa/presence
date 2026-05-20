"""Report platform mismatches between publish_queue and content_publications."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_ALL_PLATFORMS = ("x", "bluesky")
_PUBLISHED_STATUSES = {"published", "success", "succeeded", "sent"}
_FAILED_STATUSES = {"failed", "error", "errored"}
_COMPLETE_QUEUE_STATUSES = {"completed", "complete", "published", "sent", "done", "success", "succeeded"}
_ISSUE_RANK = {
    "failed_required_platform": 0,
    "published_without_queue_completion": 1,
    "missing_publication_record": 2,
    "unexpected_platform_record": 3,
}


def build_publish_queue_platform_mismatch_report(
    rows: list[dict[str, Any]],
    *,
    all_platforms: tuple[str, ...] | list[str] = DEFAULT_ALL_PLATFORMS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] | list[str] = (),
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    required_for_all = tuple(dict.fromkeys(_platform(p) for p in all_platforms if _platform(p)))
    if not required_for_all:
        raise ValueError("all_platforms must include at least one platform")

    generated_at = _utc(now or datetime.now(timezone.utc))
    queue_rows = _collapse(rows, required_for_all=required_for_all)
    issue_groups = [_group_issues(row) for row in queue_rows]
    issue_groups = [group for group in issue_groups if group["issues"]]
    issue_groups.sort(key=_group_sort_key)
    shown = issue_groups[:limit]

    issue_counts = Counter(issue["issue_type"] for group in issue_groups for issue in group["issues"])
    return {
        "artifact_type": "publish_queue_platform_mismatch",
        "generated_at": generated_at.isoformat(),
        "missing_tables": sorted(missing_tables),
        "thresholds": {"all_platforms": list(required_for_all), "limit": limit},
        "summary": {
            "queue_rows_scanned": len(queue_rows),
            "issue_group_count": len(issue_groups),
            "shown_count": len(shown),
            "issue_count": sum(issue_counts.values()),
            "by_issue_type": dict(sorted(issue_counts.items())),
        },
        "issue_groups": shown,
    }


def build_publish_queue_platform_mismatch_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("publish_queue", "content_publications") if table not in schema]
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    rows = [] if "publish_queue" not in schema or "content_publications" not in schema else _load_rows(conn, schema)
    return build_publish_queue_platform_mismatch_report(rows, missing_tables=tuple(missing_tables), **kwargs)


def format_publish_queue_platform_mismatch_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_platform_mismatch_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publish Queue Platform Mismatch",
        f"Generated: {report['generated_at']}",
        "Required for all: " + ", ".join(report["thresholds"]["all_platforms"]),
        (
            f"Totals: scanned={summary['queue_rows_scanned']} groups={summary['issue_group_count']} "
            f"issues={summary['issue_count']} shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_groups"]:
        lines.append("No publish queue platform mismatches found.")
        return "\n".join(lines)
    lines.extend(["", "queue_id | content_id | queue_platform | queue_status | issues"])
    for group in report["issue_groups"]:
        issues = ",".join(issue["issue_type"] + ":" + issue["platform"] for issue in group["issues"])
        lines.append(
            f"{group['queue_id']} | {group['content_id']} | {group['queue_platform']} | "
            f"{group['queue_status']} | {issues}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    cp = schema["content_publications"]
    gc = schema.get("generated_content", set())
    select = [
        _expr(pq, "id", "pq", "queue_id", "pq.rowid"),
        _expr(pq, "content_id", "pq", "content_id", "NULL"),
        _expr(pq, "platform", "pq", "queue_platform", "'unknown'"),
        _expr(pq, "status", "pq", "queue_status", "'unknown'"),
        _expr(pq, "created_at", "pq", "queue_created_at", "NULL"),
        _expr(pq, "updated_at", "pq", "queue_updated_at", "NULL"),
        _expr(cp, "id", "cp", "publication_id", "NULL"),
        _expr(cp, "platform", "cp", "publication_platform", "NULL"),
        _expr(cp, "status", "cp", "publication_status", "NULL"),
        _expr(cp, "published_at", "cp", "published_at", "NULL"),
        _expr(gc, "status", "gc", "content_status", "NULL"),
    ]
    join_publications = "cp.content_id = pq.content_id" if "content_id" in cp and "content_id" in pq else "0"
    content_join = ""
    if gc and "id" in gc and "content_id" in pq:
        content_join = "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
                FROM publish_queue pq
                LEFT JOIN content_publications cp ON {join_publications}
                {content_join}
                ORDER BY pq.rowid ASC, cp.rowid ASC"""
        )
    ]


def _collapse(rows: list[dict[str, Any]], *, required_for_all: tuple[str, ...]) -> list[dict[str, Any]]:
    grouped: dict[tuple[int | None, int | None], dict[str, Any]] = {}
    for row in rows:
        key = (_int_or_none(row.get("queue_id")), _int_or_none(row.get("content_id")))
        group = grouped.setdefault(
            key,
            {
                "queue_id": key[0],
                "content_id": key[1],
                "queue_platform": _platform(row.get("queue_platform")) or "unknown",
                "queue_status": _clean(row.get("queue_status"), "unknown").lower(),
                "queue_created_at": row.get("queue_created_at"),
                "queue_updated_at": row.get("queue_updated_at"),
                "content_status": _clean(row.get("content_status")) or None,
                "publications": {},
            },
        )
        platform = _platform(row.get("publication_platform"))
        if platform:
            group["publications"][platform] = {
                "publication_id": _int_or_none(row.get("publication_id")),
                "platform": platform,
                "status": _clean(row.get("publication_status"), "unknown").lower(),
                "published_at": row.get("published_at"),
            }
    for group in grouped.values():
        group["required_platforms"] = (
            list(required_for_all) if group["queue_platform"] == "all" else [group["queue_platform"]]
        )
    return [grouped[key] for key in sorted(grouped, key=lambda value: ((value[0] or 0), (value[1] or 0)))]


def _group_issues(row: dict[str, Any]) -> dict[str, Any]:
    required = set(row["required_platforms"])
    publications = row["publications"]
    issues: list[dict[str, Any]] = []
    for platform in sorted(required):
        pub = publications.get(platform)
        if not pub:
            issues.append({"issue_type": "missing_publication_record", "platform": platform, "publication_id": None})
        elif pub["status"] in _FAILED_STATUSES:
            issues.append(
                {
                    "issue_type": "failed_required_platform",
                    "platform": platform,
                    "publication_id": pub["publication_id"],
                    "publication_status": pub["status"],
                }
            )
    for platform, pub in sorted(publications.items()):
        if platform not in required:
            issues.append(
                {
                    "issue_type": "unexpected_platform_record",
                    "platform": platform,
                    "publication_id": pub["publication_id"],
                    "publication_status": pub["status"],
                }
            )
        if pub["status"] in _PUBLISHED_STATUSES and row["queue_status"] not in _COMPLETE_QUEUE_STATUSES:
            issues.append(
                {
                    "issue_type": "published_without_queue_completion",
                    "platform": platform,
                    "publication_id": pub["publication_id"],
                    "publication_status": pub["status"],
                }
            )
    issues.sort(key=lambda item: (_ISSUE_RANK[item["issue_type"]], item["platform"], item.get("publication_id") or 0))
    return {
        "queue_id": row["queue_id"],
        "content_id": row["content_id"],
        "queue_platform": row["queue_platform"],
        "queue_status": row["queue_status"],
        "required_platforms": row["required_platforms"],
        "publication_platforms": sorted(publications),
        "issues": issues,
    }


def _group_sort_key(group: dict[str, Any]) -> tuple[Any, ...]:
    rank = min(_ISSUE_RANK[issue["issue_type"]] for issue in group["issues"])
    return (rank, group["queue_id"] or 0, group["content_id"] or 0)


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _platform(value: Any) -> str:
    return _clean(value).lower().replace("twitter", "x")


def _clean(value: Any, default: str = "") -> str:
    text = str(value).strip() if value is not None else ""
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
