"""Build actionable backlog reports for pending reply drafts."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from engagement.reply_dedup import (
    DEFAULT_LOOKBACK_HOURS,
    DEFAULT_SIMILARITY_THRESHOLD,
    reply_similarity,
)


DEFAULT_DAYS = 7
DEFAULT_OVERDUE_HIGH_PRIORITY_HOURS = 24
DEFAULT_STALE_DRAFT_HOURS = 48
DEFAULT_QUALITY_THRESHOLD = 6.0
BUCKET_ORDER = ("overdue", "needs_regeneration", "duplicate_risk", "stale", "ready")
REGENERATION_FLAGS = {
    "generic",
    "hallucinated",
    "low_quality",
    "needs_regeneration",
    "off_topic",
    "regenerate",
    "sycophantic",
    "too_long",
    "unsafe",
}
PRIORITY_RANK = {"high": 0, "normal": 1, "low": 2}


def build_reply_backlog_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int | None = None,
    min_age_hours: float = 0.0,
    include_low_priority: bool = False,
    now: datetime | None = None,
    stale_hours: float = DEFAULT_STALE_DRAFT_HOURS,
    overdue_high_priority_hours: float = DEFAULT_OVERDUE_HIGH_PRIORITY_HOURS,
    quality_threshold: float = DEFAULT_QUALITY_THRESHOLD,
    duplicate_lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
    duplicate_similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> dict[str, Any]:
    """Return a stable JSON-serializable reply backlog triage report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive")
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(days, limit, min_age_hours, include_low_priority, now)

    rows = _pending_reply_rows(conn, columns, days=days, now=now)
    items = []
    for row in rows:
        item = _build_item(row, columns, now=now, quality_threshold=quality_threshold)
        if item["age_hours"] < min_age_hours:
            continue
        if not include_low_priority and item["priority"] == "low":
            continue
        items.append(item)

    duplicate_matches = _duplicate_matches(
        conn,
        items,
        now=now,
        lookback_hours=duplicate_lookback_hours,
        similarity_threshold=duplicate_similarity_threshold,
    )
    for item in items:
        match = duplicate_matches.get(item["id"])
        item["duplicate_risk"] = match is not None
        item["duplicate_match"] = match

    buckets: dict[str, list[dict[str, Any]]] = {bucket: [] for bucket in BUCKET_ORDER}
    for item in sorted(items, key=_urgency_sort_key):
        bucket = _bucket_for_item(
            item,
            stale_hours=stale_hours,
            overdue_high_priority_hours=overdue_high_priority_hours,
        )
        item["bucket"] = bucket
        buckets[bucket].append(item)

    if limit is not None:
        remaining = limit
        limited = {bucket: [] for bucket in BUCKET_ORDER}
        for bucket in BUCKET_ORDER:
            if remaining <= 0:
                break
            limited[bucket] = buckets[bucket][:remaining]
            remaining -= len(limited[bucket])
        buckets = limited

    visible_items = [item for bucket in BUCKET_ORDER for item in buckets[bucket]]
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "include_low_priority": include_low_priority,
            "limit": limit,
            "min_age_hours": min_age_hours,
        },
        "thresholds": {
            "duplicate_lookback_hours": duplicate_lookback_hours,
            "duplicate_similarity": duplicate_similarity_threshold,
            "overdue_high_priority_hours": overdue_high_priority_hours,
            "quality_score": quality_threshold,
            "stale_hours": stale_hours,
        },
        "total_pending": len(visible_items),
        "counts": {bucket: len(buckets[bucket]) for bucket in BUCKET_ORDER},
        "by_priority": dict(Counter(item["priority"] for item in visible_items)),
        "by_classification": dict(Counter(item["intent"] for item in visible_items)),
        "buckets": buckets,
    }


def format_text_report(report: dict[str, Any]) -> str:
    """Format a concise queue-level triage report sorted by urgency."""
    lines = ["Reply Backlog Triage", f"Pending: {report['total_pending']}"]
    counts = report["counts"]
    lines.append(
        "Buckets: "
        + ", ".join(f"{bucket}={counts.get(bucket, 0)}" for bucket in BUCKET_ORDER)
    )
    lines.append("")

    any_items = False
    for bucket in BUCKET_ORDER:
        items = report["buckets"].get(bucket, [])
        if not items:
            continue
        any_items = True
        lines.append(bucket.replace("_", " ").title())
        for item in items:
            flags = f" flags={','.join(item['quality_flags'])}" if item["quality_flags"] else ""
            duplicate = ""
            if item.get("duplicate_match"):
                match = item["duplicate_match"]
                duplicate = f" dup={match['source_table']}#{match['id']} {match['similarity']:.2f}"
            lines.append(
                f"  #{item['id']} {item['age_hours']:.1f}h {item['priority']} "
                f"{item['platform']} @{item['author'] or 'unknown'} "
                f"{item['intent']} score={_format_score(item['quality_score'])}"
                f"{flags}{duplicate}"
            )
        lines.append("")
    if not any_items:
        lines.append("No pending replies matched.")
    return "\n".join(lines).rstrip()


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _empty_report(
    days: int,
    limit: int | None,
    min_age_hours: float,
    include_low_priority: bool,
    now: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "include_low_priority": include_low_priority,
            "limit": limit,
            "min_age_hours": min_age_hours,
        },
        "thresholds": {
            "duplicate_lookback_hours": DEFAULT_LOOKBACK_HOURS,
            "duplicate_similarity": DEFAULT_SIMILARITY_THRESHOLD,
            "overdue_high_priority_hours": DEFAULT_OVERDUE_HIGH_PRIORITY_HOURS,
            "quality_score": DEFAULT_QUALITY_THRESHOLD,
            "stale_hours": DEFAULT_STALE_DRAFT_HOURS,
        },
        "total_pending": 0,
        "counts": {bucket: 0 for bucket in BUCKET_ORDER},
        "by_priority": {},
        "by_classification": {},
        "buckets": {bucket: [] for bucket in BUCKET_ORDER},
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _pending_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    if "status" in columns:
        filters.append("COALESCE(status, 'pending') = 'pending'")
    if "detected_at" in columns:
        cutoff = now - timedelta(days=days)
        filters.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "priority" in columns:
        parts.append(
            "CASE priority WHEN 'high' THEN 0 WHEN 'normal' THEN 1 WHEN 'low' THEN 2 ELSE 3 END"
        )
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _build_item(
    row: dict[str, Any],
    columns: set[str],
    *,
    now: datetime,
    quality_threshold: float,
) -> dict[str, Any]:
    quality_flags = _parse_flags(row.get("quality_flags") if "quality_flags" in columns else None)
    quality_score = _float_or_none(row.get("quality_score") if "quality_score" in columns else None)
    needs_regeneration = any(flag in REGENERATION_FLAGS for flag in quality_flags)
    if quality_score is not None and quality_score < quality_threshold:
        needs_regeneration = True
    return {
        "id": int(row.get("id") or row.get("rowid") or 0),
        "age_hours": round(_age_hours(row.get("detected_at"), now), 2),
        "author": _value(row, columns, "inbound_author_handle"),
        "detected_at": _value(row, columns, "detected_at"),
        "draft_preview": _preview(_value(row, columns, "draft_text")),
        "draft_text": _value(row, columns, "draft_text") or "",
        "duplicate_match": None,
        "duplicate_risk": False,
        "inbound_text_preview": _preview(_value(row, columns, "inbound_text")),
        "intent": _value(row, columns, "intent") or "other",
        "platform": _value(row, columns, "platform") or "x",
        "platform_target_id": (
            _value(row, columns, "our_platform_id")
            or _value(row, columns, "our_tweet_id")
            or _value(row, columns, "inbound_tweet_id")
        ),
        "priority": _normalize_priority(_value(row, columns, "priority")),
        "quality_flags": quality_flags,
        "quality_score": quality_score,
        "reply_id": _value(row, columns, "inbound_tweet_id"),
        "needs_regeneration": needs_regeneration,
    }


def _duplicate_matches(
    conn: sqlite3.Connection,
    items: list[dict[str, Any]],
    *,
    now: datetime,
    lookback_hours: int,
    similarity_threshold: float,
) -> dict[int, dict[str, Any]]:
    if lookback_hours <= 0:
        return {}
    candidates = _reply_queue_duplicate_candidates(conn, now, lookback_hours)
    candidates.extend(_proactive_duplicate_candidates(conn, now, lookback_hours))
    matches = {}
    for item in items:
        best = None
        best_similarity = 0.0
        for candidate in candidates:
            if candidate["source_table"] == "reply_queue" and candidate["id"] == item["id"]:
                continue
            if not _same_author_or_target(item, candidate):
                continue
            similarity = reply_similarity(item["draft_text"], candidate.get("draft_text"))
            if similarity < similarity_threshold or similarity < best_similarity:
                continue
            best_similarity = similarity
            best = {
                "draft_preview": _preview(candidate.get("draft_text")),
                "id": candidate["id"],
                "reason": _duplicate_reason(item, candidate),
                "similarity": round(similarity, 4),
                "source_table": candidate["source_table"],
            }
        if best is not None:
            matches[item["id"]] = best
    return matches


def _reply_queue_duplicate_candidates(
    conn: sqlite3.Connection,
    now: datetime,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    columns = _table_columns(conn, "reply_queue")
    if not columns or "draft_text" not in columns:
        return []
    cutoff = now - timedelta(hours=lookback_hours)
    filters = ["draft_text IS NOT NULL", "TRIM(draft_text) != ''"]
    params: list[Any] = []
    if "detected_at" in columns:
        filters.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    rows = conn.execute(
        f"SELECT * FROM reply_queue WHERE {' AND '.join(filters)}",
        params,
    ).fetchall()
    return [_candidate_from_reply_row(dict(row), columns) for row in rows]


def _proactive_duplicate_candidates(
    conn: sqlite3.Connection,
    now: datetime,
    lookback_hours: int,
) -> list[dict[str, Any]]:
    if not _table_exists(conn, "proactive_actions"):
        return []
    columns = _table_columns(conn, "proactive_actions")
    if "draft_text" not in columns:
        return []
    cutoff = now - timedelta(hours=lookback_hours)
    filters = ["draft_text IS NOT NULL", "TRIM(draft_text) != ''"]
    params: list[Any] = []
    if "created_at" in columns:
        filters.append("(created_at IS NULL OR datetime(created_at) >= datetime(?))")
        params.append(cutoff.isoformat())
    if "action_type" in columns:
        filters.append("COALESCE(action_type, 'reply') = 'reply'")
    rows = conn.execute(
        f"SELECT * FROM proactive_actions WHERE {' AND '.join(filters)}",
        params,
    ).fetchall()
    return [_candidate_from_proactive_row(dict(row), columns) for row in rows]


def _candidate_from_reply_row(row: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    return {
        "author": _value(row, columns, "inbound_author_handle"),
        "draft_text": _value(row, columns, "draft_text") or "",
        "id": int(row.get("id") or row.get("rowid") or 0),
        "platform_target_id": (
            _value(row, columns, "our_platform_id")
            or _value(row, columns, "our_tweet_id")
            or _value(row, columns, "inbound_tweet_id")
        ),
        "source_table": "reply_queue",
    }


def _candidate_from_proactive_row(row: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    return {
        "author": _value(row, columns, "target_author_handle"),
        "draft_text": _value(row, columns, "draft_text") or "",
        "id": int(row.get("id") or row.get("rowid") or 0),
        "platform_target_id": _value(row, columns, "target_tweet_id"),
        "source_table": "proactive_actions",
    }


def _same_author_or_target(item: dict[str, Any], candidate: dict[str, Any]) -> bool:
    return _handle(item.get("author")) == _handle(candidate.get("author")) or (
        bool(item.get("platform_target_id"))
        and item.get("platform_target_id") == candidate.get("platform_target_id")
    )


def _duplicate_reason(item: dict[str, Any], candidate: dict[str, Any]) -> str:
    if _handle(item.get("author")) == _handle(candidate.get("author")):
        return "same_author"
    if item.get("platform_target_id") and item.get("platform_target_id") == candidate.get(
        "platform_target_id"
    ):
        return "same_platform_target"
    return "same_author_or_platform_target"


def _bucket_for_item(
    item: dict[str, Any],
    *,
    stale_hours: float,
    overdue_high_priority_hours: float,
) -> str:
    if item["priority"] == "high" and item["age_hours"] >= overdue_high_priority_hours:
        return "overdue"
    if item["needs_regeneration"]:
        return "needs_regeneration"
    if item["duplicate_risk"]:
        return "duplicate_risk"
    if item["age_hours"] >= stale_hours:
        return "stale"
    return "ready"


def _urgency_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        PRIORITY_RANK.get(item["priority"], 3),
        -float(item["age_hours"]),
        item["id"],
    )


def _age_hours(detected_at: Any, now: datetime) -> float:
    detected = _parse_datetime(detected_at)
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


def _parse_flags(value: Any) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return [str(value)]
    if isinstance(parsed, list):
        return [str(item) for item in parsed]
    if isinstance(parsed, str):
        return [parsed]
    return []


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_priority(value: Any) -> str:
    priority = str(value or "normal").lower()
    return priority if priority in PRIORITY_RANK else "normal"


def _value(row: dict[str, Any], columns: set[str], key: str) -> Any:
    return row.get(key) if key in columns else None


def _preview(value: Any, length: int = 120) -> str:
    text = " ".join(str(value or "").split())
    return text[:length]


def _handle(value: Any) -> str:
    return str(value or "").lstrip("@").casefold()


def _format_score(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.1f}"
