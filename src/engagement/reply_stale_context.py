"""Detect queued reply drafts whose source context may be stale."""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any


DEFAULT_MAX_AGE_HOURS = 24.0
DEFAULT_STATUS_FILTER = ("pending",)

ACTION_READY = "ready_for_review"
ACTION_REFRESH = "refresh_context"
ACTION_REDRAFT = "redraft"
ACTION_HOLD = "hold_for_manual_review"

TEMPORAL_PHRASE_RE = re.compile(
    r"\b("
    r"today|yesterday|tonight|tomorrow|"
    r"this\s+(morning|afternoon|evening|week|month)|"
    r"just\s+now|right\s+now|earlier\s+today"
    r")\b",
    re.IGNORECASE,
)

UNAVAILABLE_METADATA_KEYS = (
    "deleted",
    "unavailable",
    "not_found",
    "tombstone",
    "blocked",
)


def build_reply_stale_context_report(
    db: Any,
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    status_filter: list[str] | tuple[str, ...] | str | None = DEFAULT_STATUS_FILTER,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return stale-context findings for reply_queue rows without mutating them."""
    if max_age_hours <= 0:
        raise ValueError("max_age_hours must be positive")

    conn = _connection(db)
    now = _as_utc(now or datetime.now(timezone.utc))
    statuses = _normalize_status_filter(status_filter)
    columns = _table_columns(conn, "reply_queue")
    if not columns:
        return _empty_report(max_age_hours, statuses, now)

    rows = _reply_rows(conn, columns, statuses)
    findings = [
        inspect_reply_stale_context(row, max_age_hours=max_age_hours, now=now)
        for row in rows
    ]
    findings.sort(key=_finding_sort_key)
    return {
        "generated_at": now.isoformat(),
        "max_age_hours": max_age_hours,
        "status_filter": list(statuses) if statuses is not None else None,
        "total": len(findings),
        "counts": {
            "stale": sum(1 for item in findings if item["stale"]),
            "fresh": sum(1 for item in findings if not item["stale"]),
            "hold_for_manual_review": sum(
                1 for item in findings if item["recommended_action"] == ACTION_HOLD
            ),
            "redraft": sum(1 for item in findings if item["recommended_action"] == ACTION_REDRAFT),
            "refresh_context": sum(
                1 for item in findings if item["recommended_action"] == ACTION_REFRESH
            ),
            "ready_for_review": sum(
                1 for item in findings if item["recommended_action"] == ACTION_READY
            ),
        },
        "findings": findings,
    }


def inspect_reply_stale_context(
    row: dict[str, Any],
    *,
    max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Inspect one reply_queue-style row for stale or unsafe review context."""
    if max_age_hours <= 0:
        raise ValueError("max_age_hours must be positive")

    now = _as_utc(now or datetime.now(timezone.utc))
    age_hours = round(_row_age_hours(row, now), 2)
    metadata = _parse_metadata(row.get("platform_metadata"))
    reasons: list[str] = []

    if age_hours >= max_age_hours:
        reasons.append(f"inbound reply is {age_hours:g}h old")

    if not _has_text(row.get("our_post_text")) and not _metadata_source_text(metadata):
        reasons.append("original post text is missing")

    temporal_phrases = _temporal_phrases(row.get("draft_text"))
    if temporal_phrases and age_hours >= max_age_hours:
        reasons.append("draft uses outdated temporal language: " + ", ".join(temporal_phrases))

    metadata_flags = _unavailable_metadata_flags(metadata)
    if metadata_flags:
        reasons.append("parent context unavailable: " + ", ".join(metadata_flags))

    action = _recommended_action(
        reasons=reasons,
        metadata_flags=metadata_flags,
        temporal_phrases=temporal_phrases,
        age_hours=age_hours,
        max_age_hours=max_age_hours,
    )
    stale = action != ACTION_READY
    return {
        "id": _int_or_none(row.get("id")),
        "reply_id": row.get("inbound_tweet_id"),
        "status": row.get("status") or "pending",
        "platform": row.get("platform") or "x",
        "author": row.get("inbound_author_handle"),
        "age_hours": age_hours,
        "stale": stale,
        "stale_status": "stale" if stale else "fresh",
        "reasons": reasons,
        "recommended_action": action,
        "detected_at": row.get("detected_at"),
        "draft_preview": _preview(row.get("draft_text")),
        "inbound_text_preview": _preview(row.get("inbound_text")),
        "our_post_text_present": _has_text(row.get("our_post_text"))
        or _metadata_source_text(metadata),
        "metadata_flags": metadata_flags,
        "temporal_phrases": temporal_phrases,
    }


def format_reply_stale_context_json(report: dict[str, Any]) -> str:
    """Format a stale-context report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_stale_context_text(report: dict[str, Any]) -> str:
    """Format a concise stale-context report for review queues."""
    lines = [
        "Reply stale-context audit",
        f"Rows: {report['total']} stale={report['counts']['stale']} fresh={report['counts']['fresh']}",
        f"Max age: {report['max_age_hours']:g}h",
        "",
    ]
    if not report["findings"]:
        lines.append("No reply drafts matched.")
        return "\n".join(lines).rstrip()

    for item in report["findings"]:
        reasons = "; ".join(item["reasons"]) if item["reasons"] else "no stale-context risk"
        lines.append(
            f"#{item['id']} {item['stale_status']} {item['age_hours']:.1f}h "
            f"{item['recommended_action']} {item['platform']} "
            f"@{item['author'] or 'unknown'} reply={item['reply_id'] or 'unknown'}"
        )
        lines.append(f"  {reasons}")
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
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _empty_report(
    max_age_hours: float,
    statuses: tuple[str, ...] | None,
    now: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "max_age_hours": max_age_hours,
        "status_filter": list(statuses) if statuses is not None else None,
        "total": 0,
        "counts": {
            "stale": 0,
            "fresh": 0,
            "hold_for_manual_review": 0,
            "redraft": 0,
            "refresh_context": 0,
            "ready_for_review": 0,
        },
        "findings": [],
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
    normalized = tuple(
        value.strip().lower() for value in values if value and value.strip()
    )
    if not normalized or "all" in normalized:
        return None
    return normalized


def _recommended_action(
    *,
    reasons: list[str],
    metadata_flags: list[str],
    temporal_phrases: list[str],
    age_hours: float,
    max_age_hours: float,
) -> str:
    if metadata_flags:
        return ACTION_HOLD
    if any(reason == "original post text is missing" for reason in reasons):
        return ACTION_REFRESH
    if temporal_phrases and age_hours >= max_age_hours:
        return ACTION_REDRAFT
    if age_hours >= max_age_hours:
        return ACTION_REFRESH
    return ACTION_READY


def _row_age_hours(row: dict[str, Any], now: datetime) -> float:
    if row.get("age_hours") is not None:
        return max(float(row["age_hours"]), 0.0)
    detected = _parse_datetime(row.get("detected_at"))
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


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _metadata_source_text(metadata: dict[str, Any]) -> bool:
    return _has_text(metadata.get("root_post_text")) or _has_text(
        metadata.get("parent_post_text")
    )


def _unavailable_metadata_flags(metadata: dict[str, Any]) -> list[str]:
    flags: list[str] = []
    _collect_unavailable_flags(metadata, "", flags)
    return sorted(set(flags))


def _collect_unavailable_flags(value: Any, path: str, flags: list[str]) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            next_path = f"{path}.{key}" if path else str(key)
            key_text = str(key).casefold()
            if any(flag in key_text for flag in UNAVAILABLE_METADATA_KEYS) and _truthy(item):
                flags.append(next_path)
            if key_text in {"status", "reason", "error", "error_type"} and _unavailable_text(
                item
            ):
                flags.append(next_path)
            _collect_unavailable_flags(item, next_path, flags)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _collect_unavailable_flags(item, f"{path}[{index}]", flags)


def _unavailable_text(value: Any) -> bool:
    text = str(value or "").casefold()
    return any(flag in text for flag in UNAVAILABLE_METADATA_KEYS)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() not in {"", "0", "false", "no", "none"}
    return bool(value)


def _temporal_phrases(value: Any) -> list[str]:
    text = str(value or "")
    phrases = []
    for match in TEMPORAL_PHRASE_RE.finditer(text):
        phrase = " ".join(match.group(0).lower().split())
        if phrase not in phrases:
            phrases.append(phrase)
    return phrases


def _finding_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    action_rank = {
        ACTION_HOLD: 0,
        ACTION_REDRAFT: 1,
        ACTION_REFRESH: 2,
        ACTION_READY: 3,
    }
    return (
        action_rank.get(item["recommended_action"], 9),
        -float(item["age_hours"]),
        item["id"] or 0,
    )


def _has_text(value: Any) -> bool:
    return bool(str(value or "").strip())


def _preview(value: Any, length: int = 120) -> str:
    text = " ".join(str(value or "").split())
    return text[:length]


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
