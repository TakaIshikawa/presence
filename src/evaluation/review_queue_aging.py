"""Summarize generated content aging in review and publish limbo."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_BUCKET_DAYS = (1, 3, 7, 14)
DEFAULT_LIMIT = 50
REQUIRED_COLUMNS = {"id", "created_at"}
PUBLISHED_STATUSES = {"published", "success", "succeeded"}
RESOLVED_STATUSES = {
    "abandoned",
    "archived",
    "cancelled",
    "complete",
    "completed",
    "deleted",
    "dismissed",
    "rejected",
    "resolved",
}


def build_review_queue_aging_report(
    db_or_conn: Any,
    *,
    bucket_days: tuple[int, ...] = DEFAULT_BUCKET_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return bucketed aging counts for unpublished generated content."""

    bucket_days = _validate_bucket_days(bucket_days)
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"bucket_days": list(bucket_days), "limit": limit}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["generated_content"])

    missing = sorted(REQUIRED_COLUMNS - schema["generated_content"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"generated_content": missing},
        )

    queue_states = _queue_states(conn, schema)
    publication_states = _publication_states(conn, schema)
    rows = _rows(conn, schema)
    items = [
        item
        for row in rows
        if (
            item := _item(
                row,
                now=generated_at,
                bucket_days=bucket_days,
                queue_statuses=queue_states.get(int(row["id"]), []),
                publication_statuses=publication_states.get(int(row["id"]), []),
            )
        )
        is not None
    ]
    groups = _groups(items, bucket_days)[:limit]
    return {
        "artifact_type": "review_queue_aging",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items, bucket_days),
        "groups": groups,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_review_queue_aging_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_review_queue_aging_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Review Queue Aging",
        f"Generated: {report['generated_at']}",
        (
            "Filters: bucket_days="
            + ",".join(str(day) for day in filters["bucket_days"])
            + f" limit={filters['limit']}"
        ),
        (
            f"Totals: pending={totals['pending_count']} "
            f"groups={totals['group_count']} "
            f"oldest_age_days={totals['oldest_age_days']}"
        ),
        "Age buckets: "
        + " ".join(
            f"{bucket}={totals['by_age_bucket'][bucket]}"
            for bucket in _bucket_labels(tuple(filters["bucket_days"]))
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
            )
        )
    if not report["groups"]:
        lines.append("No pending review queue items found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Groups:")
    for group in report["groups"]:
        oldest = group["oldest_item"]
        lines.append(
            f"- status={group['status']} type={group['content_type']} "
            f"count={group['count']} oldest=#{oldest['content_id']} "
            f"age={oldest['age_days']}d bucket={oldest['age_bucket']} "
            f"created_at={oldest['created_at']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    bucket_counts = {label: 0 for label in _bucket_labels(tuple(filters["bucket_days"]))}
    return {
        "artifact_type": "review_queue_aging",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "pending_count": 0,
            "group_count": 0,
            "oldest_age_days": None,
            "by_status": {},
            "by_content_type": {},
            "by_age_bucket": bucket_counts,
        },
        "groups": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    select_columns = [
        "gc.id",
        _column_expr(columns, "content_type", "gc", "content_type", "'unknown'"),
        _column_expr(columns, "content", "gc", "content", "NULL"),
        _column_expr(columns, "created_at", "gc", "created_at", "NULL"),
        _column_expr(columns, "updated_at", "gc", "updated_at", "NULL"),
        _column_expr(columns, "status", "gc", "status", "NULL"),
        _column_expr(columns, "review_status", "gc", "review_status", "NULL"),
        _column_expr(columns, "curation_quality", "gc", "curation_quality", "NULL"),
        _column_expr(columns, "published", "gc", "published", "0"),
        _column_expr(columns, "published_at", "gc", "published_at", "NULL"),
        _column_expr(columns, "published_url", "gc", "published_url", "NULL"),
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select_columns)}
                FROM generated_content gc
                ORDER BY datetime(gc.created_at) ASC, gc.id ASC"""
        ).fetchall()
    ]


def _item(
    row: dict[str, Any],
    *,
    now: datetime,
    bucket_days: tuple[int, ...],
    queue_statuses: list[str],
    publication_statuses: list[str],
) -> dict[str, Any] | None:
    if _is_published(row, publication_statuses) or _is_resolved(row, queue_statuses):
        return None
    created_at = _parse(row.get("created_at"))
    if created_at is None or created_at > now:
        return None
    age_days = max(0, int((now - created_at).total_seconds() // 86400))
    status = _review_status(row, queue_statuses)
    content = _clean(row.get("content")) or ""
    return {
        "content_id": int(row["id"]),
        "status": status,
        "content_type": _clean(row.get("content_type")) or "unknown",
        "age_days": age_days,
        "age_bucket": _age_bucket(age_days, bucket_days),
        "created_at": created_at.isoformat(),
        "updated_at": row.get("updated_at"),
        "queue_status": _combined_status(queue_statuses),
        "publication_status": _combined_status(publication_statuses),
        "content_preview": content[:96],
    }


def _groups(items: list[dict[str, Any]], bucket_days: tuple[int, ...]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for item in items:
        grouped.setdefault((item["status"], item["content_type"]), []).append(item)

    groups: list[dict[str, Any]] = []
    for (status, content_type), group_items in grouped.items():
        group_items.sort(key=lambda item: (-item["age_days"], item["content_id"]))
        counts = Counter(item["age_bucket"] for item in group_items)
        groups.append(
            {
                "status": status,
                "content_type": content_type,
                "count": len(group_items),
                "age_buckets": {
                    label: counts.get(label, 0) for label in _bucket_labels(bucket_days)
                },
                "oldest_item": group_items[0],
            }
        )
    groups.sort(
        key=lambda group: (
            -group["oldest_item"]["age_days"],
            -group["count"],
            group["status"],
            group["content_type"],
        )
    )
    return groups


def _totals(items: list[dict[str, Any]], bucket_days: tuple[int, ...]) -> dict[str, Any]:
    bucket_counts = Counter(item["age_bucket"] for item in items)
    return {
        "pending_count": len(items),
        "group_count": len({(item["status"], item["content_type"]) for item in items}),
        "oldest_age_days": max((item["age_days"] for item in items), default=None),
        "by_status": dict(sorted(Counter(item["status"] for item in items).items())),
        "by_content_type": dict(sorted(Counter(item["content_type"] for item in items).items())),
        "by_age_bucket": {
            label: bucket_counts.get(label, 0) for label in _bucket_labels(bucket_days)
        },
    }


def _queue_states(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, list[str]]:
    columns = schema.get("publish_queue")
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT content_id, status
           FROM publish_queue
           WHERE content_id IS NOT NULL
           ORDER BY content_id ASC, id ASC"""
    ).fetchall()
    states: dict[int, list[str]] = {}
    for row in rows:
        status = _normalize(row["status"])
        if status:
            states.setdefault(int(row["content_id"]), []).append(status)
    return states


def _publication_states(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, list[str]]:
    columns = schema.get("content_publications")
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT content_id, status
           FROM content_publications
           WHERE content_id IS NOT NULL
           ORDER BY content_id ASC"""
    ).fetchall()
    states: dict[int, list[str]] = {}
    for row in rows:
        status = _normalize(row["status"])
        if status:
            states.setdefault(int(row["content_id"]), []).append(status)
    return states


def _review_status(row: dict[str, Any], queue_statuses: list[str]) -> str:
    for field in ("review_status", "status", "curation_quality"):
        status = _normalize(row.get(field))
        if status and status not in PUBLISHED_STATUSES and status not in RESOLVED_STATUSES:
            return status
    queue_status = _combined_status(queue_statuses)
    return queue_status or "unpublished"


def _is_published(row: dict[str, Any], publication_statuses: list[str]) -> bool:
    if any(status in PUBLISHED_STATUSES for status in publication_statuses):
        return True
    published = row.get("published")
    if isinstance(published, str):
        if published.strip().lower() in {"1", "true", "yes", "published"}:
            return True
    elif published:
        return int(published) == 1
    return bool(_clean(row.get("published_at")) or _clean(row.get("published_url")))


def _is_resolved(row: dict[str, Any], queue_statuses: list[str]) -> bool:
    published = row.get("published")
    try:
        if int(published) == -1:
            return True
    except (TypeError, ValueError):
        pass
    statuses = [_normalize(row.get(field)) for field in ("review_status", "status")]
    statuses.extend(queue_statuses)
    return any(status in RESOLVED_STATUSES for status in statuses if status)


def _age_bucket(days: int, bucket_days: tuple[int, ...]) -> str:
    previous = 0
    for threshold in bucket_days:
        if days <= threshold:
            return f"{previous}-{threshold}d" if previous == 0 else f"{previous + 1}-{threshold}d"
        previous = threshold
    return f"{bucket_days[-1] + 1}d+"


def _bucket_labels(bucket_days: tuple[int, ...]) -> tuple[str, ...]:
    labels: list[str] = []
    previous = 0
    for threshold in bucket_days:
        labels.append(f"{previous}-{threshold}d" if previous == 0 else f"{previous + 1}-{threshold}d")
        previous = threshold
    labels.append(f"{bucket_days[-1] + 1}d+")
    return tuple(labels)


def _validate_bucket_days(values: tuple[int, ...]) -> tuple[int, ...]:
    if not values:
        raise ValueError("bucket_days must not be empty")
    if any(value <= 0 for value in values):
        raise ValueError("bucket_days values must be positive")
    ordered = tuple(sorted(values))
    if len(set(ordered)) != len(ordered):
        raise ValueError("bucket_days values must be unique")
    return ordered


def _column_expr(
    columns: set[str],
    column: str,
    alias: str,
    output: str,
    default: str,
) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _combined_status(statuses: list[str]) -> str:
    unique = sorted(dict.fromkeys(status for status in statuses if status))
    return ",".join(unique)


def _normalize(value: Any) -> str:
    return str(value or "").strip().lower()


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
