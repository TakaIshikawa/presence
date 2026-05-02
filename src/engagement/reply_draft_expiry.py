"""Plan regeneration work for stale pending reply drafts."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from typing import Any


DEFAULT_MAX_DRAFT_AGE_HOURS = 48.0
DEFAULT_MAX_CONTEXT_AGE_HOURS = 24.0
DEFAULT_STATUS_FILTER = ("pending",)

ACTION_REGENERATE = "regenerate"
ACTION_RECHECK_CONTEXT = "recheck_context"
ACTION_KEEP = "keep"

REASON_DRAFT_AGE_EXCEEDED = "draft_age_exceeded"
REASON_CONTEXT_AGE_EXCEEDED = "context_age_exceeded"

URGENCY_ORDER = ("high", "normal", "low")
ACTION_RANK = {ACTION_REGENERATE: 0, ACTION_RECHECK_CONTEXT: 1, ACTION_KEEP: 2}

CONTEXT_TIMESTAMP_KEYS = {
    "context_at",
    "context_timestamp",
    "context_updated_at",
    "context_refreshed_at",
    "context_fetched_at",
    "context_imported_at",
    "retrieved_at",
    "refreshed_at",
    "fetched_at",
    "updated_at",
    "imported_at",
    "generated_at",
    "created_at",
}


def build_reply_draft_expiry_plan(
    db: Any,
    *,
    max_draft_age_hours: float = DEFAULT_MAX_DRAFT_AGE_HOURS,
    max_context_age_hours: float = DEFAULT_MAX_CONTEXT_AGE_HOURS,
    status_filter: list[str] | tuple[str, ...] | str | None = DEFAULT_STATUS_FILTER,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a read-only regeneration plan for pending reply drafts."""
    if max_draft_age_hours <= 0:
        raise ValueError("max_draft_age_hours must be positive")
    if max_context_age_hours <= 0:
        raise ValueError("max_context_age_hours must be positive")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    statuses = _normalize_status_filter(status_filter)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_plan(max_draft_age_hours, max_context_age_hours, statuses, now)

    rows = _reply_rows(conn, columns, statuses)
    items = [
        inspect_reply_draft_expiry(
            row,
            columns=columns,
            max_draft_age_hours=max_draft_age_hours,
            max_context_age_hours=max_context_age_hours,
            now=now,
        )
        for row in rows
    ]
    items.sort(key=_item_sort_key)
    groups = _group_items(items)

    return {
        "generated_at": now.isoformat(),
        "status_filter": list(statuses) if statuses is not None else None,
        "thresholds": {
            "max_context_age_hours": max_context_age_hours,
            "max_draft_age_hours": max_draft_age_hours,
        },
        "total": len(items),
        "counts": {
            "stale": sum(1 for item in items if item["stale"]),
            "keep": sum(1 for item in items if item["recommended_action"] == ACTION_KEEP),
            "recheck_context": sum(
                1 for item in items if item["recommended_action"] == ACTION_RECHECK_CONTEXT
            ),
            "regenerate": sum(
                1 for item in items if item["recommended_action"] == ACTION_REGENERATE
            ),
        },
        "by_platform": dict(Counter(item["platform"] for item in items)),
        "by_urgency": dict(Counter(item["urgency"] for item in items)),
        "groups": groups,
        "items": items,
    }


def inspect_reply_draft_expiry(
    row: dict[str, Any],
    *,
    columns: set[str] | None = None,
    max_draft_age_hours: float = DEFAULT_MAX_DRAFT_AGE_HOURS,
    max_context_age_hours: float = DEFAULT_MAX_CONTEXT_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Inspect one reply_queue-style row for draft or context expiry."""
    if max_draft_age_hours <= 0:
        raise ValueError("max_draft_age_hours must be positive")
    if max_context_age_hours <= 0:
        raise ValueError("max_context_age_hours must be positive")

    columns = columns or set(row)
    now = _as_utc(now or datetime.now(timezone.utc))
    draft_age_hours = round(_age_hours(_value(row, columns, "detected_at"), now), 2)
    context_timestamp = _latest_context_timestamp(row, columns)
    context_age_hours = (
        round(max((now - context_timestamp).total_seconds() / 3600, 0.0), 2)
        if context_timestamp is not None
        else None
    )

    reason_codes: list[str] = []
    if draft_age_hours >= max_draft_age_hours:
        reason_codes.append(REASON_DRAFT_AGE_EXCEEDED)
    if context_age_hours is not None and context_age_hours >= max_context_age_hours:
        reason_codes.append(REASON_CONTEXT_AGE_EXCEEDED)

    action = _recommended_action(reason_codes)
    urgency = _urgency(action)
    return {
        "id": _int_or_none(_value(row, columns, "id")),
        "reply_id": _value(row, columns, "inbound_tweet_id"),
        "status": _value(row, columns, "status") or "pending",
        "platform": _value(row, columns, "platform") or "x",
        "author_handle": _value(row, columns, "inbound_author_handle"),
        "age_hours": draft_age_hours,
        "context_age_hours": context_age_hours,
        "context_timestamp": context_timestamp.isoformat() if context_timestamp else None,
        "reason_codes": reason_codes,
        "recommended_action": action,
        "stale": bool(reason_codes),
        "urgency": urgency,
    }


def format_reply_draft_expiry_json(plan: dict[str, Any]) -> str:
    """Format a reply draft expiry plan as stable JSON."""
    return json.dumps(plan, indent=2, sort_keys=True)


def format_reply_draft_expiry_text(plan: dict[str, Any]) -> str:
    """Format a concise reply draft expiry plan grouped by platform and urgency."""
    lines = [
        "Reply Draft Expiry Plan",
        (
            f"Rows: {plan['total']} stale={plan['counts']['stale']} "
            f"regenerate={plan['counts']['regenerate']} "
            f"recheck_context={plan['counts']['recheck_context']} "
            f"keep={plan['counts']['keep']}"
        ),
        (
            f"Thresholds: draft={plan['thresholds']['max_draft_age_hours']:g}h "
            f"context={plan['thresholds']['max_context_age_hours']:g}h"
        ),
        "",
    ]
    groups = plan.get("groups", {})
    wrote_item = False
    for platform in sorted(groups):
        platform_groups = groups[platform]
        lines.append(platform)
        for urgency in URGENCY_ORDER:
            items = platform_groups.get(urgency, [])
            if not items:
                continue
            lines.append(f"  {urgency}")
            for item in items:
                wrote_item = True
                reasons = ",".join(item["reason_codes"]) if item["reason_codes"] else "none"
                context_age = (
                    "n/a"
                    if item["context_age_hours"] is None
                    else f"{item['context_age_hours']:.1f}h"
                )
                lines.append(
                    f"    #{item['id'] or 'unknown'} reply={item['reply_id'] or 'unknown'} "
                    f"@{item['author_handle'] or 'unknown'} age={item['age_hours']:.1f}h "
                    f"context={context_age} action={item['recommended_action']} "
                    f"reasons={reasons}"
                )
        lines.append("")
    if not wrote_item:
        lines.append("No pending reply drafts matched.")
    return "\n".join(lines).rstrip()


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    statuses: tuple[str, ...] | None,
) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if statuses is not None and "status" in columns:
        placeholders = ", ".join("?" for _ in statuses)
        filters.append(f"COALESCE(status, 'pending') IN ({placeholders})")
        params.extend(statuses)

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "platform" in columns:
        parts.append("platform ASC")
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _empty_plan(
    max_draft_age_hours: float,
    max_context_age_hours: float,
    statuses: tuple[str, ...] | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "status_filter": list(statuses) if statuses is not None else None,
        "thresholds": {
            "max_context_age_hours": max_context_age_hours,
            "max_draft_age_hours": max_draft_age_hours,
        },
        "total": 0,
        "counts": {
            "stale": 0,
            "keep": 0,
            "recheck_context": 0,
            "regenerate": 0,
        },
        "by_platform": {},
        "by_urgency": {},
        "groups": {},
        "items": [],
    }


def _normalize_status_filter(
    status_filter: list[str] | tuple[str, ...] | str | None,
) -> tuple[str, ...] | None:
    if status_filter is None:
        return None
    if isinstance(status_filter, str):
        values = [status_filter]
    else:
        values = list(status_filter)
    normalized = tuple(value.strip().lower() for value in values if value and value.strip())
    if not normalized or "all" in normalized:
        return None
    return normalized


def _latest_context_timestamp(row: dict[str, Any], columns: set[str]) -> datetime | None:
    timestamps: list[datetime] = []
    for column in ("platform_metadata", "relationship_context"):
        parsed = _parse_json_object(_value(row, columns, column))
        timestamps.extend(_context_timestamps(parsed))
    if not timestamps:
        return None
    return max(timestamps)


def _context_timestamps(value: Any) -> list[datetime]:
    timestamps: list[datetime] = []
    _collect_context_timestamps(value, "", timestamps)
    return timestamps


def _collect_context_timestamps(value: Any, key: str, timestamps: list[datetime]) -> None:
    if isinstance(value, dict):
        for child_key, child_value in value.items():
            child_key_text = str(child_key)
            if child_key_text in CONTEXT_TIMESTAMP_KEYS:
                parsed = _parse_datetime(child_value)
                if parsed is not None:
                    timestamps.append(parsed)
            _collect_context_timestamps(child_value, child_key_text, timestamps)
    elif isinstance(value, list):
        for item in value:
            _collect_context_timestamps(item, key, timestamps)


def _recommended_action(reason_codes: list[str]) -> str:
    if REASON_DRAFT_AGE_EXCEEDED in reason_codes:
        return ACTION_REGENERATE
    if REASON_CONTEXT_AGE_EXCEEDED in reason_codes:
        return ACTION_RECHECK_CONTEXT
    return ACTION_KEEP


def _urgency(action: str) -> str:
    if action == ACTION_REGENERATE:
        return "high"
    if action == ACTION_RECHECK_CONTEXT:
        return "normal"
    return "low"


def _group_items(items: list[dict[str, Any]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    groups: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for item in items:
        platform_groups = groups.setdefault(
            item["platform"],
            {urgency: [] for urgency in URGENCY_ORDER},
        )
        platform_groups[item["urgency"]].append(item)
    return groups


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        item["platform"],
        ACTION_RANK.get(item["recommended_action"], 9),
        -float(item["age_hours"]),
        item["id"] or 0,
    )


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _age_hours(timestamp: Any, now: datetime) -> float:
    detected = _parse_datetime(timestamp)
    if detected is None:
        return 0.0
    return max((now - detected).total_seconds() / 3600, 0.0)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for parser in (
        lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")),
        lambda v: datetime.strptime(v, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            parsed = parser(text)
        except ValueError:
            continue
        return _as_utc(parsed)
    return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _value(row: dict[str, Any], columns: set[str], key: str) -> Any:
    return row.get(key) if key in columns else None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
