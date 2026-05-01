"""Build deadline-oriented SLA reports for pending reply drafts."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any


DEFAULT_HIGH_HOURS = 6
DEFAULT_NORMAL_HOURS = 24
DEFAULT_LOW_HOURS = 72
BUCKET_ORDER = ("breached", "due_soon", "within_sla")
PRIORITY_ORDER = ("high", "normal", "low")
PRIORITY_RANK = {priority: rank for rank, priority in enumerate(PRIORITY_ORDER)}
DUE_SOON_RATIO = 0.75


def build_reply_sla_report(
    db: Any,
    *,
    high_hours: float = DEFAULT_HIGH_HOURS,
    normal_hours: float = DEFAULT_NORMAL_HOURS,
    low_hours: float = DEFAULT_LOW_HOURS,
    platform: str | None = None,
    limit: int | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a stable JSON-serializable SLA report for pending reply_queue rows."""
    thresholds = _validate_thresholds(high_hours, normal_hours, low_hours)
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(thresholds, platform, limit, now)

    rows = _pending_rows(conn, columns, platform=platform)
    items = [_build_item(row, columns, thresholds=thresholds, now=now) for row in rows]
    items.sort(key=_next_action_sort_key)
    if limit is not None:
        items = items[:limit]

    buckets: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in BUCKET_ORDER}
    for item in items:
        buckets[item["sla_status"]].append(item)

    return {
        "generated_at": now.isoformat(),
        "filters": {
            "limit": limit,
            "platform": platform,
        },
        "thresholds": thresholds,
        "total_pending": len(items),
        "counts": {bucket: len(buckets[bucket]) for bucket in BUCKET_ORDER},
        "by_platform": dict(Counter(item["platform"] for item in items)),
        "by_priority": dict(Counter(item["priority"] for item in items)),
        "by_relationship_tier": dict(
            Counter(item["relationship_tier"] or "unknown" for item in items)
        ),
        "buckets": buckets,
        "items": items,
    }


def format_json_report(report: dict[str, Any]) -> str:
    """Format a deterministic JSON representation of an SLA report."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_text_report(report: dict[str, Any]) -> str:
    """Format a concise deterministic text report sorted by next action."""
    lines = [
        "Reply SLA Report",
        f"Generated: {report['generated_at']}",
        f"Pending: {report['total_pending']}",
    ]
    filters = report["filters"]
    filter_parts = []
    if filters.get("platform"):
        filter_parts.append(f"platform={filters['platform']}")
    if filters.get("limit") is not None:
        filter_parts.append(f"limit={filters['limit']}")
    lines.append("Filters: " + (", ".join(filter_parts) if filter_parts else "none"))

    thresholds = report["thresholds"]
    lines.append(
        "Thresholds: "
        + ", ".join(
            f"{priority}={_format_hours(thresholds[priority])}h"
            for priority in PRIORITY_ORDER
        )
    )
    counts = report["counts"]
    lines.append(
        "Buckets: "
        + ", ".join(f"{bucket}={counts.get(bucket, 0)}" for bucket in BUCKET_ORDER)
    )
    lines.append(f"Platforms: {_format_counts(report['by_platform'])}")
    lines.append(f"Priorities: {_format_counts(report['by_priority'])}")
    lines.append("")

    if not report["items"]:
        lines.append("No pending replies matched.")
        return "\n".join(lines)

    for bucket in BUCKET_ORDER:
        bucket_items = report["buckets"].get(bucket, [])
        if not bucket_items:
            continue
        lines.append(bucket.replace("_", " ").title())
        for item in bucket_items:
            author = item["author"] or "unknown"
            tier = item["relationship_tier"] or "unknown"
            target = item["inbound_tweet_id"] or f"reply_queue:{item['id']}"
            lines.append(
                f"  #{item['id']} {item['age_hours']:.1f}h/"
                f"{_format_hours(item['sla_hours'])}h "
                f"{item['priority']} {item['platform']} @{author} {tier} "
                f"remaining={item['hours_remaining']:.1f}h target={target}"
            )
        lines.append("")
    return "\n".join(lines).rstrip()


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _validate_thresholds(
    high_hours: float,
    normal_hours: float,
    low_hours: float,
) -> dict[str, float]:
    thresholds = {
        "high": float(high_hours),
        "normal": float(normal_hours),
        "low": float(low_hours),
    }
    for priority, hours in thresholds.items():
        if hours <= 0:
            raise ValueError(f"{priority}_hours must be positive")
    return thresholds


def _empty_report(
    thresholds: dict[str, float],
    platform: str | None,
    limit: int | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "limit": limit,
            "platform": platform,
        },
        "thresholds": thresholds,
        "total_pending": 0,
        "counts": {bucket: 0 for bucket in BUCKET_ORDER},
        "by_platform": {},
        "by_priority": {},
        "by_relationship_tier": {},
        "buckets": {bucket: [] for bucket in BUCKET_ORDER},
        "items": [],
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _pending_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    platform: str | None,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    if "status" in columns:
        filters.append("COALESCE(status, 'pending') = 'pending'")
    if platform and "platform" in columns:
        filters.append("platform = ?")
        params.append(platform)
    elif platform:
        return []

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    cursor = conn.execute(query, params)
    column_names = [description[0] for description in cursor.description]
    return [dict(zip(column_names, row)) for row in cursor.fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "priority" in columns:
        parts.append(
            "CASE priority "
            "WHEN 'high' THEN 0 "
            "WHEN 'normal' THEN 1 "
            "WHEN 'low' THEN 2 "
            "ELSE 3 END"
        )
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _build_item(
    row: dict[str, Any],
    columns: set[str],
    *,
    thresholds: dict[str, float],
    now: datetime,
) -> dict[str, Any]:
    priority = _normalize_priority(_value(row, columns, "priority"))
    sla_hours = thresholds[priority]
    age_hours = round(_age_hours(_value(row, columns, "detected_at"), now), 2)
    hours_remaining = round(sla_hours - age_hours, 2)
    sla_ratio = round(age_hours / sla_hours, 4)
    status = _sla_status(sla_ratio)
    relationship_tier = _relationship_tier(_value(row, columns, "relationship_context"))
    item_id = row.get("id") if "id" in columns else row.get("rowid")
    return {
        "id": int(item_id or 0),
        "sla_status": status,
        "age_hours": age_hours,
        "sla_hours": sla_hours,
        "hours_remaining": hours_remaining,
        "sla_ratio": sla_ratio,
        "priority": priority,
        "platform": _value(row, columns, "platform") or "x",
        "author": _value(row, columns, "inbound_author_handle"),
        "relationship_tier": relationship_tier,
        "relationship_context": _relationship_summary(
            _value(row, columns, "relationship_context")
        ),
        "detected_at": _value(row, columns, "detected_at"),
        "inbound_tweet_id": _value(row, columns, "inbound_tweet_id"),
        "inbound_text_preview": _preview(_value(row, columns, "inbound_text")),
        "draft_preview": _preview(_value(row, columns, "draft_text")),
    }


def _next_action_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        BUCKET_ORDER.index(item["sla_status"]),
        PRIORITY_RANK.get(item["priority"], 3),
        -item["sla_ratio"],
        -item["age_hours"],
        item["id"],
    )


def _value(row: dict[str, Any], columns: set[str], column: str) -> Any:
    return row.get(column) if column in columns else None


def _normalize_priority(priority: Any) -> str:
    value = str(priority or "normal").lower()
    return value if value in PRIORITY_RANK else "normal"


def _sla_status(sla_ratio: float) -> str:
    if sla_ratio >= 1:
        return "breached"
    if sla_ratio >= DUE_SOON_RATIO:
        return "due_soon"
    return "within_sla"


def _age_hours(detected_at: Any, now: datetime) -> float:
    if not detected_at:
        return 0.0
    detected = _parse_datetime(str(detected_at))
    if detected is None:
        return 0.0
    age = (_as_utc(now) - _as_utc(detected)).total_seconds() / 3600
    return max(age, 0.0)


def _parse_datetime(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _relationship_tier(relationship_context: Any) -> str | None:
    context = _parse_json_object(relationship_context)
    if not context:
        return None
    tier_name = context.get("tier_name")
    tier = context.get("dunbar_tier")
    if tier_name and tier is not None:
        return f"{tier_name} (tier {tier})"
    if tier_name:
        return str(tier_name)
    if tier is not None:
        return f"tier {tier}"
    return None


def _relationship_summary(relationship_context: Any) -> dict[str, Any] | None:
    context = _parse_json_object(relationship_context)
    if not context:
        return None
    keys = (
        "tier_name",
        "dunbar_tier",
        "relationship_strength",
        "stage",
        "last_interaction",
    )
    summary = {key: context[key] for key in keys if key in context}
    return summary or None


def _parse_json_object(value: Any) -> dict[str, Any] | None:
    if not value:
        return None
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (json.JSONDecodeError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _preview(value: Any, max_len: int = 96) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "..."


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "{}"
    return ", ".join(f"{key}={counts[key]}" for key in sorted(counts))


def _format_hours(hours: float) -> str:
    return str(int(hours)) if float(hours).is_integer() else f"{hours:g}"
