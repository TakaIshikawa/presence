"""Find held publish queue items with stale or weak hold reasons."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 7
DEFAULT_LIMIT = 100
HELD_STATUS_MARKERS = ("hold", "held", "block", "blocked", "review", "needs_review", "waiting")
GENERIC_REASONS = {"hold", "held", "blocked", "review", "pending", "todo", "tbd", "n/a", "none"}


def build_publish_queue_hold_reason_freshness_report(
    rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_before = generated_at - timedelta(days=stale_days)
    held_items = [_item(row, generated_at, stale_before) for row in rows if _is_held(row)]
    stale_holds = [item for item in held_items if item["reason_flags"]]
    missing_reason = [item for item in stale_holds if "missing_reason" in item["reason_flags"]]
    stale_holds.sort(key=lambda item: (-(item["age_days"] or 0), str(item["queue_id"] or "")))
    return {
        "artifact_type": "publish_queue_hold_reason_freshness",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "stale_days": stale_days,
            "stale_before": stale_before.isoformat(),
            "limit": limit,
            "minimum_reason_length": 6,
        },
        "stale_holds": stale_holds[:limit],
        "missing_reason_items": missing_reason[:limit],
        "age_buckets": _age_buckets(held_items),
        "summary": {
            "held_items_count": len(held_items),
            "stale_holds_count": len(stale_holds),
            "missing_reason_count": len(missing_reason),
            "flag_counts": dict(sorted(Counter(flag for item in stale_holds for flag in item["reason_flags"]).items())),
        },
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_publish_queue_hold_reason_freshness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    rows = _load(conn, schema["publish_queue"]) if "publish_queue" in schema else []
    return build_publish_queue_hold_reason_freshness_report(rows, missing_schema=missing_schema, **kwargs)


def format_publish_queue_hold_reason_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_hold_reason_freshness_text(report: dict[str, Any]) -> str:
    lines = ["Publish Queue Hold Reason Freshness", f"Generated: {report['generated_at']}"]
    if report["missing_schema"]["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_schema"]["missing_tables"]))
    lines.append(f"Stale holds: {len(report['stale_holds'])}")
    for item in report["stale_holds"]:
        lines.append(
            f"  - queue_id={item['queue_id'] or '-'} status={item['status']} "
            f"age_days={item['age_days']} flags={','.join(item['reason_flags'])}"
        )
    return "\n".join(lines)


def _item(row: dict[str, Any], now: datetime, stale_before: datetime) -> dict[str, Any]:
    metadata = _json_obj(_first(row, "metadata", "meta", "details"))
    reason, source = _reason(row, metadata)
    reference = _parse_dt(_first(row, "hold_reason_updated_at", "reason_updated_at")) or _parse_dt(_first(metadata, "hold_reason_updated_at", "reason_updated_at")) or _parse_dt(_first(row, "updated_at", "created_at", "scheduled_at"))
    flags = []
    if not reason:
        flags.append("missing_reason")
    elif len(reason) < 6 or reason.lower() in GENERIC_REASONS:
        flags.append("generic_or_too_short_reason")
    if reference is None:
        flags.append("missing_hold_timestamp")
    elif reference < stale_before:
        flags.append("stale_hold")
    return {
        "queue_id": _first(row, "queue_id", "id"),
        "content_id": _first(row, "content_id", "generated_content_id"),
        "platform": _clean(_first(row, "platform", "channel")) or None,
        "status": _clean(_first(row, "status", "state")) or "unknown",
        "scheduled_at": _first(row, "scheduled_at"),
        "hold_reference_at": _iso(reference),
        "age_days": _age_days(reference, now),
        "hold_reason": reason or None,
        "reason_source": source,
        "reason_flags": flags,
    }


def _is_held(row: dict[str, Any]) -> bool:
    status = _clean(_first(row, "status", "state")).lower()
    return any(marker in status for marker in HELD_STATUS_MARKERS)


def _reason(row: dict[str, Any], metadata: dict[str, Any]) -> tuple[str, str | None]:
    for key in ("hold_reason", "reason", "blocked_reason", "review_reason", "error", "message", "status_message"):
        value = _clean(row.get(key))
        if value:
            return value, key
    for key in ("hold_reason", "reason", "blocked_reason", "review_reason", "error", "message"):
        value = _clean(metadata.get(key))
        if value:
            return value, f"metadata.{key}"
    return "", None


def _age_buckets(items: list[dict[str, Any]]) -> dict[str, int]:
    buckets = {"unknown": 0, "0-2": 0, "3-7": 0, "8-30": 0, "31+": 0}
    for item in items:
        age = item["age_days"]
        if age is None:
            buckets["unknown"] += 1
        elif age <= 2:
            buckets["0-2"] += 1
        elif age <= 7:
            buckets["3-7"] += 1
        elif age <= 30:
            buckets["8-30"] += 1
        else:
            buckets["31+"] += 1
    return buckets


def _load(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = ", ".join(f"{column} AS {column}" for column in sorted(columns)) or "rowid"
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM publish_queue ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    return {"missing_tables": [] if "publish_queue" in schema else ["publish_queue"], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_dt(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(value: datetime | None, now: datetime) -> int | None:
    return None if value is None else int((_utc(now) - _utc(value)).total_seconds() // 86400)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()
