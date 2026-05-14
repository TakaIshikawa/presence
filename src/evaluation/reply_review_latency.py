"""Measure reply review latency from queue detection to review outcomes."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any, Iterable


DEFAULT_DAYS = 14
DEFAULT_SLA_HOURS = 24.0
GROUP_FIELDS = ("platform", "priority", "intent")
REVIEW_EVENT_TYPES = {"approved", "edited", "rejected", "expired"}


def build_reply_review_latency_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    sla_hours: float = DEFAULT_SLA_HOURS,
    group_by: str = "platform",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic read-only latency report for reply review events."""
    if days <= 0:
        raise ValueError("days must be positive")
    if sla_hours <= 0:
        raise ValueError("sla_hours must be positive")
    if group_by not in GROUP_FIELDS:
        raise ValueError(f"group_by must be one of: {', '.join(GROUP_FIELDS)}")

    conn = _connection(db)
    schema = _schema(conn)
    generated_at = _as_utc(now or _latest_reply_timestamp(conn, schema) or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    if "reply_queue" not in schema:
        return _empty_report(days, sla_hours, group_by, generated_at, cutoff)

    rows = _reply_rows(conn, schema["reply_queue"], cutoff, generated_at)
    events = _review_events(conn, schema, [int(row.get("id") or 0) for row in rows])
    items = [
        _build_item(
            row,
            schema["reply_queue"],
            events.get(int(row.get("id") or 0), []),
            generated_at,
            sla_hours,
        )
        for row in rows
    ]
    items.sort(key=_item_sort_key)
    groups = _group_items(items, group_by, sla_hours)

    return {
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "group_by": group_by,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "sla_hours": float(sla_hours),
        },
        "overall": _summarize_group("overall", items, sla_hours),
        "groups": groups,
        "items": items,
    }


def format_reply_review_latency_json(report: dict[str, Any]) -> str:
    """Serialize the report as stable JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_review_latency_text(report: dict[str, Any]) -> str:
    """Format a concise operator-readable reply review latency report."""
    overall = report["overall"]
    filters = report["filters"]
    lines = [
        "Reply Review Latency Report",
        f"Generated: {report['generated_at']}",
        (
            f"Lookback: {filters['days']} days "
            f"({filters['lookback_start']} to {filters['lookback_end']})"
        ),
        f"SLA: {filters['sla_hours']:g}h first review",
        f"Group by: {filters['group_by']}",
        (
            f"Rows: {overall['counts']['total']} reviewed={overall['counts']['reviewed']} "
            f"posted={overall['counts']['posted']} rejected={overall['counts']['rejected']} "
            f"pending={overall['counts']['pending']} breached={overall['counts']['breached']}"
        ),
        (
            "Overall latency h: "
            f"review median={_format_number(overall['latency_hours']['first_review']['median'])} "
            f"p90={_format_number(overall['latency_hours']['first_review']['p90'])}; "
            f"approval median={_format_number(overall['latency_hours']['approval']['median'])} "
            f"p90={_format_number(overall['latency_hours']['approval']['p90'])}; "
            f"rejection median={_format_number(overall['latency_hours']['rejection']['median'])} "
            f"p90={_format_number(overall['latency_hours']['rejection']['p90'])}; "
            f"posting median={_format_number(overall['latency_hours']['posting']['median'])} "
            f"p90={_format_number(overall['latency_hours']['posting']['p90'])}"
        ),
        "Breached ids: " + _format_ids(overall["breached_item_ids"]),
        "",
    ]

    if not report["groups"]:
        lines.append("No reply rows matched.")
        return "\n".join(lines)

    lines.append("Groups:")
    lines.append(
        f"  {'Group':<16} {'Total':>5} {'Rev':>5} {'Post':>5} {'Reject':>6} "
        f"{'Pend':>5} {'Breach':>6} {'Med h':>7} {'P90 h':>7}  Breached ids"
    )
    lines.append("  " + "-" * 86)
    for group in report["groups"]:
        counts = group["counts"]
        latency = group["latency_hours"]["first_review"]
        lines.append(
            f"  {group['group'][:16]:<16} {counts['total']:>5} {counts['reviewed']:>5} "
            f"{counts['posted']:>5} {counts['rejected']:>6} {counts['pending']:>5} "
            f"{counts['breached']:>6} {_format_number(latency['median']):>7} "
            f"{_format_number(latency['p90']):>7}  {_format_ids(group['breached_item_ids'])}"
        )
    return "\n".join(lines)


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    schema: dict[str, set[str]] = {}
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    except sqlite3.Error:
        return schema
    for row in tables:
        table = str(row[0])
        try:
            schema[table] = {
                str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
            }
        except sqlite3.Error:
            schema[table] = set()
    return schema


def _reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    filters = []
    params: list[Any] = []
    if "detected_at" in columns:
        filters.append("detected_at IS NOT NULL")
        filters.append("datetime(detected_at) >= datetime(?)")
        filters.append("datetime(detected_at) <= datetime(?)")
        params.extend([cutoff.isoformat(), now.isoformat()])

    query = "SELECT * FROM reply_queue"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + _order_clause(columns)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _latest_reply_timestamp(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> datetime | None:
    columns = schema.get("reply_queue")
    if not columns or "detected_at" not in columns:
        return None
    try:
        row = conn.execute("SELECT MAX(detected_at) AS latest FROM reply_queue").fetchone()
    except sqlite3.Error:
        return None
    latest = row["latest"] if hasattr(row, "keys") else row[0]
    parsed = _parse_datetime(latest)
    if parsed is None:
        return None
    return parsed + timedelta(seconds=1)


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _review_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    reply_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    columns = schema.get("reply_review_events")
    if not reply_ids or not columns or "reply_queue_id" not in columns:
        return {}
    placeholders = ", ".join("?" for _ in reply_ids)
    query = (
        "SELECT * FROM reply_review_events "
        f"WHERE reply_queue_id IN ({placeholders}) "
        "ORDER BY reply_queue_id ASC"
    )
    if "created_at" in columns:
        query += ", datetime(created_at) ASC"
    if "id" in columns:
        query += ", id ASC"
    indexed: dict[int, list[dict[str, Any]]] = defaultdict(list)
    cursor = conn.execute(query, reply_ids)
    names = [description[0] for description in cursor.description]
    for row in cursor.fetchall():
        event = dict(zip(names, row))
        indexed[int(event["reply_queue_id"])].append(event)
    return indexed


def _build_item(
    row: dict[str, Any],
    columns: set[str],
    events: list[dict[str, Any]],
    now: datetime,
    sla_hours: float,
) -> dict[str, Any]:
    detected_at = _parse_datetime(_value(row, columns, "detected_at"))
    first_review_at = _event_timestamp(events, "review")
    approval_at = _event_timestamp(events, "approval")
    rejection_at = _event_timestamp(events, "rejection")
    posting_at = _event_timestamp(events, "posting")
    review_latency = _elapsed_hours(detected_at, first_review_at)
    pending_age = _elapsed_hours(detected_at, now)
    if review_latency is None:
        review_latency = pending_age
    status = _derived_status(first_review_at, approval_at, rejection_at, posting_at)
    breached = review_latency is not None and review_latency >= sla_hours
    item_id = int(row.get("id") or row.get("rowid") or 0)
    return {
        "id": item_id,
        "platform": _value(row, columns, "platform") or "x",
        "priority": _value(row, columns, "priority") or "normal",
        "intent": _value(row, columns, "intent") or "other",
        "status": status,
        "detected_at": detected_at.isoformat() if detected_at else None,
        "first_review_at": first_review_at.isoformat() if first_review_at else None,
        "approval_at": approval_at.isoformat() if approval_at else None,
        "rejection_at": rejection_at.isoformat() if rejection_at else None,
        "posting_at": posting_at.isoformat() if posting_at else None,
        "pending": first_review_at is None,
        "breached": breached,
        "latency_hours": {
            "first_review": _round_or_none(review_latency),
            "approval": _round_or_none(_elapsed_hours(detected_at, approval_at)),
            "rejection": _round_or_none(_elapsed_hours(detected_at, rejection_at)),
            "posting": _round_or_none(_elapsed_hours(detected_at, posting_at)),
        },
    }


def _event_timestamp(events: list[dict[str, Any]], kind: str) -> datetime | None:
    for event in events:
        event_type = str(event.get("event_type") or "")
        new_status = str(event.get("new_status") or "")
        if kind == "posting" and (event_type == "posted" or new_status == "posted"):
            return _parse_datetime(event.get("created_at"))
        if kind == "approval" and (event_type == "approved" or new_status == "approved"):
            return _parse_datetime(event.get("created_at"))
        if kind == "rejection" and (
            event_type in {"rejected", "expired"} or new_status == "dismissed"
        ):
            return _parse_datetime(event.get("created_at"))
        if kind == "review" and (
            event_type in REVIEW_EVENT_TYPES or new_status in {"approved", "dismissed"}
        ):
            return _parse_datetime(event.get("created_at"))
    return None


def _derived_status(
    first_review_at: datetime | None,
    approval_at: datetime | None,
    rejection_at: datetime | None,
    posting_at: datetime | None,
) -> str:
    if posting_at:
        return "posted"
    if rejection_at:
        return "rejected"
    if approval_at:
        return "approved"
    if first_review_at:
        return "reviewed"
    return "pending"


def _group_items(
    items: list[dict[str, Any]],
    field: str,
    sla_hours: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[str(item[field])].append(item)
    return [_summarize_group(key, grouped[key], sla_hours) for key in sorted(grouped)]


def _summarize_group(
    name: str,
    items: list[dict[str, Any]],
    sla_hours: float,
) -> dict[str, Any]:
    counts = Counter(item["status"] for item in items)
    breached_ids = [item["id"] for item in items if item["breached"]]
    return {
        "group": name,
        "counts": {
            "total": len(items),
            "reviewed": sum(1 for item in items if not item["pending"]),
            "approved": counts.get("approved", 0),
            "posted": counts.get("posted", 0),
            "rejected": counts.get("rejected", 0),
            "pending": counts.get("pending", 0),
            "breached": len(breached_ids),
        },
        "sla_hours": float(sla_hours),
        "latency_hours": {
            "first_review": _latency_stats(
                item["latency_hours"]["first_review"] for item in items
            ),
            "approval": _latency_stats(item["latency_hours"]["approval"] for item in items),
            "rejection": _latency_stats(item["latency_hours"]["rejection"] for item in items),
            "posting": _latency_stats(item["latency_hours"]["posting"] for item in items),
        },
        "breached_item_ids": breached_ids,
    }


def _latency_stats(values: Iterable[float | None]) -> dict[str, float | None]:
    numeric = sorted(float(value) for value in values if value is not None)
    return {
        "count": len(numeric),
        "median": _round_or_none(float(median(numeric)) if numeric else None),
        "p90": _round_or_none(_percentile(numeric, 0.9)),
    }


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


def _empty_report(
    days: int,
    sla_hours: float,
    group_by: str,
    generated_at: datetime,
    cutoff: datetime,
) -> dict[str, Any]:
    return {
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "group_by": group_by,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "sla_hours": float(sla_hours),
        },
        "overall": _summarize_group("overall", [], sla_hours),
        "groups": [],
        "items": [],
    }


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    latency = item["latency_hours"]["first_review"]
    return (
        not item["breached"],
        item["platform"],
        item["priority"],
        item["intent"],
        -(latency or 0.0),
        item["id"],
    )


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


def _elapsed_hours(start: datetime | None, end: datetime | None) -> float | None:
    if not start or not end:
        return None
    elapsed = (_as_utc(end) - _as_utc(start)).total_seconds() / 3600
    if elapsed < 0:
        return None
    return elapsed


def _round_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 2)


def _format_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"


def _format_ids(ids: list[int]) -> str:
    return ", ".join(str(item_id) for item_id in ids) if ids else "none"
