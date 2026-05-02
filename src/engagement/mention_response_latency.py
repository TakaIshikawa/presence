"""Measure first-response latency for inbound mention reply handling."""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable


DEFAULT_DAYS = 14
CSV_FIELDS = (
    "mention_id",
    "received_at",
    "first_reply_draft_at",
    "published_reply_at",
    "latency_minutes",
    "status",
    "relationship_context_present",
)


def build_mention_response_latency_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return first-response latency rows and summaries for inbound mentions."""
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    schema = _schema(conn)
    filters = {
        "days": days,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    if "reply_queue" not in schema:
        return _empty_report(generated_at, filters, ["reply_queue"])

    rows = _load_mention_rows(
        conn,
        schema["reply_queue"],
        cutoff=cutoff,
        now=generated_at,
    )
    items = [_build_item(row, schema["reply_queue"]) for row in rows]
    items.sort(key=_item_sort_key)

    return {
        "artifact_type": "mention_response_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _summarize_items("overall", items),
        "by_day": _group_by_day(items),
        "by_relationship_tier": _group_by_relationship_tier(items),
        "rows": items,
        "missing_tables": [],
    }


def format_mention_response_latency_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_mention_response_latency_csv(report: dict[str, Any]) -> str:
    """Render item-level mention response latency rows as CSV."""
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_FIELDS, extrasaction="ignore")
    writer.writeheader()
    for row in report.get("rows", []):
        writer.writerow({field: _csv_value(row.get(field)) for field in CSV_FIELDS})
    return buffer.getvalue().rstrip("\r\n")


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "mention_response_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _summarize_items("overall", []),
        "by_day": [],
        "by_relationship_tier": [],
        "rows": [],
        "missing_tables": missing_tables,
    }


def _load_mention_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    received_column = _received_column(columns)
    if received_column:
        filters.extend(
            [
                f"{received_column} IS NOT NULL",
                f"datetime({received_column}) >= datetime(?)",
                f"datetime({received_column}) <= datetime(?)",
            ]
        )
        params.extend([cutoff.isoformat(), now.isoformat()])

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns, received_column)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _order_clause(columns: set[str], received_column: str | None) -> str:
    parts = []
    if received_column:
        parts.append(f"datetime({received_column}) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _build_item(row: dict[str, Any], columns: set[str]) -> dict[str, Any]:
    mention_id = int(row.get("id") or row.get("rowid") or 0)
    received_at = _parse_datetime(_first_value(row, columns, "received_at", "detected_at"))
    first_reply_draft_at = _first_reply_draft_at(row, columns, received_at)
    published_reply_at = _published_reply_at(row, columns)
    first_response_at = _earliest_datetime(first_reply_draft_at, published_reply_at)
    relationship_tier = _relationship_tier(_value(row, columns, "relationship_context"))

    return {
        "mention_id": mention_id,
        "received_at": received_at.isoformat() if received_at else None,
        "first_reply_draft_at": (
            first_reply_draft_at.isoformat() if first_reply_draft_at else None
        ),
        "published_reply_at": published_reply_at.isoformat() if published_reply_at else None,
        "latency_minutes": _round_or_none(_elapsed_minutes(received_at, first_response_at)),
        "status": _response_status(first_reply_draft_at, published_reply_at),
        "relationship_context_present": bool(_value(row, columns, "relationship_context")),
        "relationship_tier": relationship_tier,
    }


def _first_reply_draft_at(
    row: dict[str, Any],
    columns: set[str],
    received_at: datetime | None,
) -> datetime | None:
    if not _value(row, columns, "draft_text"):
        return None
    explicit = _parse_datetime(
        _first_value(row, columns, "first_reply_draft_at", "draft_created_at")
    )
    return explicit or received_at


def _published_reply_at(row: dict[str, Any], columns: set[str]) -> datetime | None:
    if not _has_published_reply(row, columns):
        return None
    return _parse_datetime(_first_value(row, columns, "published_reply_at", "posted_at"))


def _has_published_reply(row: dict[str, Any], columns: set[str]) -> bool:
    if _value(row, columns, "published_reply_at") or _value(row, columns, "posted_at"):
        return True
    if _value(row, columns, "posted_tweet_id") or _value(row, columns, "posted_platform_id"):
        return True
    return str(_value(row, columns, "status") or "").lower() == "posted"


def _response_status(
    first_reply_draft_at: datetime | None,
    published_reply_at: datetime | None,
) -> str:
    if published_reply_at:
        return "published"
    if first_reply_draft_at:
        return "drafted"
    return "pending"


def _group_by_day(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        day = str(item["received_at"] or "")[:10] if item["received_at"] else "unknown"
        grouped[day].append(item)
    return [_summarize_items(day, grouped[day]) for day in sorted(grouped)]


def _group_by_relationship_tier(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item["relationship_tier"] or "unknown"].append(item)
    return [_summarize_items(tier, grouped[tier]) for tier in sorted(grouped)]


def _summarize_items(name: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = sorted(
        float(item["latency_minutes"])
        for item in items
        if item["latency_minutes"] is not None
    )
    counts = Counter(item["status"] for item in items)
    return {
        "group": name,
        "counts": {
            "total": len(items),
            "drafted": counts.get("drafted", 0),
            "published": counts.get("published", 0),
            "pending": counts.get("pending", 0),
            "responded": counts.get("drafted", 0) + counts.get("published", 0),
        },
        "latency_minutes": {
            "count": len(latencies),
            "median": _round_or_none(float(median(latencies)) if latencies else None),
            "p90": _round_or_none(_percentile(latencies, 0.9)),
        },
    }


def _received_column(columns: set[str]) -> str | None:
    for column in ("received_at", "detected_at", "created_at"):
        if column in columns:
            return column
    return None


def _relationship_tier(value: Any) -> str | None:
    if not value:
        return None
    try:
        context = json.loads(str(value))
    except (TypeError, ValueError):
        return "present"
    if not isinstance(context, dict):
        return "present"
    tier_name = context.get("tier_name") or context.get("stage_name")
    dunbar_tier = context.get("dunbar_tier") or context.get("tier")
    if tier_name and dunbar_tier is not None:
        return f"{tier_name} (tier {dunbar_tier})"
    if tier_name:
        return str(tier_name)
    if dunbar_tier is not None:
        return f"tier {dunbar_tier}"
    return "present"


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["received_at"] or "", item["mention_id"])


def _first_value(row: dict[str, Any], columns: set[str], *names: str) -> Any:
    for name in names:
        value = _value(row, columns, name)
        if value:
            return value
    return None


def _value(row: dict[str, Any], columns: set[str], column: str) -> Any:
    return row.get(column) if column in columns else None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    normalized = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _earliest_datetime(*values: datetime | None) -> datetime | None:
    present = [_as_utc(value) for value in values if value]
    return min(present) if present else None


def _elapsed_minutes(start: datetime | None, end: datetime | None) -> float | None:
    if not start or not end:
        return None
    elapsed = (_as_utc(end) - _as_utc(start)).total_seconds() / 60
    if elapsed < 0:
        return None
    return elapsed


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    weight = position - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    return value
