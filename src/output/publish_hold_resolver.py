"""Read-only resolver for held publish queue items."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 30
DEFAULT_REPEATED_ERROR_THRESHOLD = 3
VALID_PLATFORMS = ("all", "x", "bluesky")
RECOMMENDATIONS = (
    "release_now",
    "reschedule",
    "cancel_duplicate",
    "needs_manual_review",
    "retry_after_error",
)
RETRYABLE_CATEGORIES = {"network", "rate_limit"}
MANUAL_CATEGORIES = {"auth", "media", "validation", "unknown"}
PREVIEW_WIDTH = 96


def build_publish_hold_resolution(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build advisory actions for held publish queue rows without mutating state."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    current = _ensure_aware(now or datetime.now(timezone.utc))
    schema = _schema(conn)
    required = ("publish_queue",)
    optional = ("content_publications", "publication_attempts", "generated_content")
    if "publish_queue" not in schema:
        return _report(current, days, platform, [], schema, required, optional)

    cutoff = current - timedelta(days=days)
    queue_rows = _held_queue_rows(conn, schema, platform=platform, cutoff=cutoff)
    publications = _publication_rows(conn, schema, queue_rows)
    attempts = _attempt_rows(conn, schema, queue_rows)
    items = [
        _resolve_item(
            row,
            publication=publications.get((row["content_id"], row["platform"])),
            attempts=attempts.get((row["queue_id"], row["content_id"], row["platform"]), []),
            now=current,
        )
        for row in queue_rows
    ]
    items.sort(
        key=lambda item: (
            item["recommendation"],
            item["platform"],
            item["hold_age_hours"] if item["hold_age_hours"] is not None else -1,
            item["queue_id"],
        )
    )
    return _report(current, days, platform, items, schema, required, optional)


def format_publish_hold_resolution_json(report: dict[str, Any]) -> str:
    """Render the resolver report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_hold_resolution_text(report: dict[str, Any]) -> str:
    """Render a stable operator-facing hold resolution table."""
    lines = [
        "Publish hold resolver",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} platform={report['filters']['platform']}",
        f"Held items: {report['total_items']}",
        (
            "Recommendations: "
            + ", ".join(
                f"{name}={report['recommendation_counts'].get(name, 0)}"
                for name in RECOMMENDATIONS
            )
        ),
        "",
    ]
    if not report["items"]:
        lines.append("No held publish queue items matched the filters.")
        return "\n".join(lines)

    columns = [
        ("queue_id", "QUEUE", 6),
        ("content_id", "CID", 6),
        ("platform", "PLATFORM", 8),
        ("hold_age_label", "AGE", 10),
        ("recommendation", "RECOMMENDATION", 19),
        ("primary_reason", "REASON", 24),
        ("content_preview", "CONTENT_PREVIEW", 54),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for item in report["items"]:
        lines.append(
            "  ".join(
                _clip(item.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _held_queue_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    gc = schema.get("generated_content", set())
    if not {"id", "content_id"}.issubset(pq):
        return []

    select = {
        "queue_id": "pq.id",
        "content_id": "pq.content_id",
        "scheduled_at": _column_expr(pq, "scheduled_at", alias="pq"),
        "platform": _column_expr(pq, "platform", "'all'", alias="pq"),
        "status": _column_expr(pq, "status", "'queued'", alias="pq"),
        "queue_error": _column_expr(pq, "error", alias="pq"),
        "queue_error_category": _column_expr(pq, "error_category", alias="pq"),
        "hold_reason": _column_expr(pq, "hold_reason", alias="pq"),
        "created_at": _column_expr(pq, "created_at", alias="pq"),
        "content_type": _column_expr(gc, "content_type", alias="gc"),
        "content": _column_expr(gc, "content", alias="gc"),
        "generated_published_at": _column_expr(gc, "published_at", alias="gc"),
        "generated_published": _column_expr(gc, "published", alias="gc"),
    }
    filters = []
    params: list[Any] = []
    if "status" in pq:
        filters.append("pq.status = 'held'")
    if platform != "all" and "platform" in pq:
        filters.append("pq.platform = ?")
        params.append(platform)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    join_clause = (
        "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        if "generated_content" in schema
        else ""
    )

    rows = conn.execute(
        f"""SELECT
               {select['queue_id']} AS queue_id,
               {select['content_id']} AS content_id,
               {select['scheduled_at']} AS scheduled_at,
               {select['platform']} AS platform,
               {select['status']} AS status,
               {select['queue_error']} AS queue_error,
               {select['queue_error_category']} AS queue_error_category,
               {select['hold_reason']} AS hold_reason,
               {select['created_at']} AS created_at,
               {select['content_type']} AS content_type,
               {select['content']} AS content,
               {select['generated_published_at']} AS generated_published_at,
               {select['generated_published']} AS generated_published
           FROM publish_queue pq
           {join_clause}
           {where_clause}
           ORDER BY {select['created_at']} ASC, pq.id ASC""",
        params,
    ).fetchall()

    held_rows = []
    for row in rows:
        item = dict(row)
        if _within_window(item, cutoff):
            held_rows.append(item)
    return held_rows


def _publication_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    queue_rows: list[dict[str, Any]],
) -> dict[tuple[int, str], dict[str, Any]]:
    columns = schema.get("content_publications")
    if not columns or not queue_rows or not {"content_id", "platform"}.issubset(columns):
        return {}

    content_ids = sorted({row["content_id"] for row in queue_rows})
    placeholders = ",".join("?" for _ in content_ids)
    select = {
        "publication_id": _column_expr(columns, "id"),
        "content_id": "content_id",
        "platform": "platform",
        "status": _column_expr(columns, "status"),
        "error": _column_expr(columns, "error"),
        "error_category": _column_expr(columns, "error_category"),
        "attempt_count": _column_expr(columns, "attempt_count", "0"),
        "next_retry_at": _column_expr(columns, "next_retry_at"),
        "last_error_at": _column_expr(columns, "last_error_at"),
        "published_at": _column_expr(columns, "published_at"),
        "updated_at": _column_expr(columns, "updated_at"),
    }
    rows = conn.execute(
        f"""SELECT
               {select['publication_id']} AS publication_id,
               {select['content_id']} AS content_id,
               {select['platform']} AS platform,
               {select['status']} AS status,
               {select['error']} AS error,
               {select['error_category']} AS error_category,
               {select['attempt_count']} AS attempt_count,
               {select['next_retry_at']} AS next_retry_at,
               {select['last_error_at']} AS last_error_at,
               {select['published_at']} AS published_at,
               {select['updated_at']} AS updated_at
           FROM content_publications
           WHERE content_id IN ({placeholders})
           ORDER BY content_id ASC, platform ASC, id ASC""",
        content_ids,
    ).fetchall()
    return {(row["content_id"], row["platform"]): dict(row) for row in rows}


def _attempt_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    queue_rows: list[dict[str, Any]],
) -> dict[tuple[int, int, str], list[dict[str, Any]]]:
    columns = schema.get("publication_attempts")
    if not columns or not queue_rows or not {"content_id", "platform"}.issubset(columns):
        return {}

    queue_ids = sorted({row["queue_id"] for row in queue_rows})
    content_ids = sorted({row["content_id"] for row in queue_rows})
    queue_placeholders = ",".join("?" for _ in queue_ids)
    content_placeholders = ",".join("?" for _ in content_ids)
    select = {
        "queue_id": _column_expr(columns, "queue_id"),
        "content_id": "content_id",
        "platform": "platform",
        "attempted_at": _column_expr(columns, "attempted_at"),
        "success": _column_expr(columns, "success", "0"),
        "error": _column_expr(columns, "error"),
        "error_category": _column_expr(columns, "error_category"),
    }
    rows = conn.execute(
        f"""SELECT
               {select['queue_id']} AS queue_id,
               {select['content_id']} AS content_id,
               {select['platform']} AS platform,
               {select['attempted_at']} AS attempted_at,
               {select['success']} AS success,
               {select['error']} AS error,
               {select['error_category']} AS error_category
           FROM publication_attempts
           WHERE (queue_id IN ({queue_placeholders})
                  OR content_id IN ({content_placeholders}))
           ORDER BY content_id ASC, platform ASC, attempted_at DESC, id DESC""",
        [*queue_ids, *content_ids],
    ).fetchall()
    grouped: dict[tuple[int, int, str], list[dict[str, Any]]] = {}
    queue_id_set = set(queue_ids)
    for row in rows:
        data = dict(row)
        matching_queue_ids = (
            [data["queue_id"]]
            if data.get("queue_id") in queue_id_set
            else [
                queue["queue_id"]
                for queue in queue_rows
                if queue["content_id"] == data["content_id"]
                and queue["platform"] == data["platform"]
            ]
        )
        for queue_id in matching_queue_ids:
            grouped.setdefault((queue_id, data["content_id"], data["platform"]), []).append(data)
    return grouped


def _resolve_item(
    row: dict[str, Any],
    *,
    publication: dict[str, Any] | None,
    attempts: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    hold_age = _age_hours(row.get("created_at"), now)
    category = _dominant_category(row, publication, attempts)
    failed_attempts = [attempt for attempt in attempts if not bool(attempt.get("success"))]
    error_count = max(len(failed_attempts), int((publication or {}).get("attempt_count") or 0))
    primary_error = _first_present(
        (publication or {}).get("error"),
        row.get("queue_error"),
        *(attempt.get("error") for attempt in attempts),
    )
    reasons = _reason_codes(row, publication, category, error_count)
    recommendation = _recommendation(row, publication, category, error_count, now)
    return {
        "queue_id": row["queue_id"],
        "content_id": row["content_id"],
        "publication_id": (publication or {}).get("publication_id"),
        "platform": row["platform"],
        "scheduled_at": row.get("scheduled_at"),
        "created_at": row.get("created_at"),
        "hold_age_hours": round(hold_age, 2) if hold_age is not None else None,
        "hold_age_label": _age_label(hold_age),
        "content_type": row.get("content_type"),
        "content_preview": _content_preview(row.get("content")),
        "hold_reason": row.get("hold_reason"),
        "error": primary_error,
        "error_category": category,
        "attempt_count": error_count,
        "next_retry_at": (publication or {}).get("next_retry_at"),
        "last_error_at": (publication or {}).get("last_error_at"),
        "recommendation": recommendation,
        "primary_reason": reasons[0],
        "reasons": reasons,
        "action": _action_text(recommendation),
    }


def _recommendation(
    row: dict[str, Any],
    publication: dict[str, Any] | None,
    category: str,
    error_count: int,
    now: datetime,
) -> str:
    if row.get("content") is None:
        return "needs_manual_review"
    if error_count >= DEFAULT_REPEATED_ERROR_THRESHOLD:
        return "needs_manual_review"
    if category == "duplicate" or _already_published(row, publication):
        return "cancel_duplicate"
    if category in RETRYABLE_CATEGORIES:
        return "retry_after_error"
    if category in MANUAL_CATEGORIES and _has_error(row, publication):
        return "needs_manual_review"
    scheduled = _parse_timestamp(row.get("scheduled_at"))
    if scheduled is not None and scheduled > now:
        return "reschedule"
    return "release_now"


def _reason_codes(
    row: dict[str, Any],
    publication: dict[str, Any] | None,
    category: str,
    error_count: int,
) -> list[str]:
    reasons: list[str] = []
    if row.get("content") is None:
        reasons.append("missing_content")
    if error_count >= DEFAULT_REPEATED_ERROR_THRESHOLD:
        reasons.append("repeated_errors")
    if category == "duplicate" or _already_published(row, publication):
        reasons.append("duplicate_or_already_published")
    elif category in RETRYABLE_CATEGORIES:
        reasons.append(f"retryable_{category}")
    elif category in MANUAL_CATEGORIES and _has_error(row, publication):
        reasons.append(f"{category}_error")
    if not reasons:
        scheduled = _parse_timestamp(row.get("scheduled_at"))
        reasons.append("future_schedule" if scheduled else "held_without_blocking_error")
    return reasons


def _dominant_category(
    row: dict[str, Any],
    publication: dict[str, Any] | None,
    attempts: list[dict[str, Any]],
) -> str:
    signals: list[tuple[Any, Any]] = [
        ((publication or {}).get("error_category"), (publication or {}).get("error")),
        (row.get("queue_error_category"), row.get("queue_error")),
        (None, row.get("hold_reason")),
    ]
    signals.extend((attempt.get("error_category"), attempt.get("error")) for attempt in attempts)
    categories = [
        _category_for(category, error, platform=row.get("platform"))
        for category, error in signals
    ]
    if "duplicate" in categories:
        return "duplicate"
    for category in ("auth", "media", "validation", "rate_limit", "network", "unknown"):
        if category in categories:
            return category
    return "unknown"


def _category_for(category: Any, error: Any, *, platform: str | None) -> str:
    normalized = normalize_error_category(category)
    if normalized != "unknown":
        return normalized
    return classify_publish_error(error, platform=platform)


def _report(
    now: datetime,
    days: int,
    platform: str,
    items: list[dict[str, Any]],
    schema: dict[str, set[str]],
    required_tables: tuple[str, ...],
    optional_tables: tuple[str, ...],
) -> dict[str, Any]:
    counts = {name: 0 for name in RECOMMENDATIONS}
    for item in items:
        counts[item["recommendation"]] += 1
    return {
        "generated_at": now.isoformat(),
        "filters": {"days": days, "platform": platform},
        "total_items": len(items),
        "recommendation_counts": counts,
        "items": items,
        "missing_tables": [
            table for table in (*required_tables, *optional_tables) if table not in schema
        ],
        "read_only": True,
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _column_expr(
    columns: set[str],
    name: str,
    default: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if name not in columns:
        return default
    return f"{alias}.{name}" if alias else name


def _within_window(row: dict[str, Any], cutoff: datetime) -> bool:
    parsed = [
        _parse_timestamp(value)
        for value in (row.get("created_at"), row.get("scheduled_at"))
    ]
    parsed = [value for value in parsed if value is not None]
    if not parsed:
        return True
    return max(parsed) >= cutoff


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_aware(parsed)


def _age_hours(value: Any, now: datetime) -> float | None:
    parsed = _parse_timestamp(value)
    if parsed is None:
        return None
    return max(0.0, (now - parsed).total_seconds() / 3600)


def _age_label(hours: float | None) -> str:
    if hours is None:
        return "unknown"
    total_minutes = int(hours * 60)
    days, remainder = divmod(total_minutes, 1440)
    hour_count, minute_count = divmod(remainder, 60)
    if days:
        return f"{days}d {hour_count}h"
    if hour_count:
        return f"{hour_count}h {minute_count}m"
    return f"{minute_count}m"


def _content_preview(content: Any, width: int = PREVIEW_WIDTH) -> str:
    if content is None:
        return "[missing generated_content]"
    text = " ".join(str(content).split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _already_published(
    row: dict[str, Any],
    publication: dict[str, Any] | None,
) -> bool:
    if publication and (
        publication.get("status") == "published" or publication.get("published_at")
    ):
        return True
    return bool(row.get("generated_published") == 1 or row.get("generated_published_at"))


def _has_error(row: dict[str, Any], publication: dict[str, Any] | None) -> bool:
    return bool(
        row.get("queue_error")
        or row.get("queue_error_category")
        or (publication or {}).get("error")
        or (publication or {}).get("error_category")
    )


def _action_text(recommendation: str) -> str:
    return {
        "release_now": "Clear the hold and let the item publish now.",
        "reschedule": "Pick a new publish window before clearing the hold.",
        "cancel_duplicate": "Cancel the queue item after confirming the duplicate.",
        "needs_manual_review": "Inspect the content and failure history before changing the queue.",
        "retry_after_error": "Clear the hold after the transient failure or retry window is resolved.",
    }[recommendation]


def _first_present(*values: Any) -> str | None:
    for value in values:
        if value:
            return str(value)
    return None


def _clip(value: Any, width: int) -> str:
    if value is None:
        return "-"
    text = str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
