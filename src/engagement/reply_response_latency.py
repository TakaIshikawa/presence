"""Measure latency from inbound mention detection to reply draft creation."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import mean, median
from typing import Any, cast


DEFAULT_DAYS = 14
DEFAULT_THRESHOLD_MINUTES = 60
DEFAULT_LIMIT = 50

TABLE = "reply_queue"
BUCKETS = (
    ("0-15m", 0.0, 15.0),
    ("16-60m", 15.0, 60.0),
    ("61-240m", 60.0, 240.0),
    (">240m", 240.0, None),
)


def build_reply_response_latency_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold_minutes: int = DEFAULT_THRESHOLD_MINUTES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return draft response latency rows, bucket counts, and flagged mentions."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold_minutes <= 0:
        raise ValueError("threshold_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "threshold_minutes": threshold_minutes,
    }

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        columns = set().union(*(row.keys() for row in raw_rows)) if raw_rows else set()
        source_table: str | None = "rows"
        missing_tables: list[str] = []
    else:
        conn = _connection(db_or_rows)
        columns = _table_columns(conn, TABLE)
        if not columns:
            return _empty_report(generated_at, filters, missing_tables=[TABLE])
        raw_rows = _load_rows(conn, columns, cutoff=cutoff, now=generated_at)
        source_table = TABLE
        missing_tables = []

    missing_columns = _missing_columns(columns)
    items = [
        _build_item(row, columns, threshold_minutes=threshold_minutes)
        for row in raw_rows
    ]
    filtered_items = []
    for item in items:
        if item["detected_at"]:
            detected = _parse_datetime(item["detected_at"])
            if detected is not None and cutoff <= detected <= generated_at:
                filtered_items.append(item)
    items = filtered_items
    items.sort(key=_item_sort_key)
    flagged = [item for item in items if item["flagged"]]
    flagged.sort(key=_flagged_sort_key)

    return {
        "artifact_type": "reply_response_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items, flagged),
        "summary": _summary(items),
        "latency_buckets": _latency_buckets(items),
        "rows": items,
        "flagged_mentions": flagged[:limit],
        "source_table": source_table,
        "missing_tables": missing_tables,
        "missing_columns": {"reply_queue": missing_columns} if missing_columns else {},
    }


def format_reply_response_latency_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_response_latency_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable latency report."""
    filters = report["filters"]
    totals = report["totals"]
    summary = report["summary"]
    lines = [
        "Reply Response Latency Report",
        f"Generated: {report['generated_at']}",
        (
            f"Lookback: {filters['days']} days "
            f"({filters['lookback_start']} to {filters['lookback_end']})"
        ),
        f"Delayed threshold: >{filters['threshold_minutes']}m",
        (
            f"Totals: mentions={totals['mention_count']} drafted={totals['drafted_count']} "
            f"missing_draft={totals['missing_draft_count']} delayed={totals['delayed_count']} "
            f"flagged={totals['flagged_count']}"
        ),
        (
            "Latency minutes: "
            f"count={summary['count']} median={_text_number(summary['median'])} "
            f"p90={_text_number(summary['p90'])} avg={_text_number(summary['average'])}"
        ),
        "Buckets: "
        + ", ".join(f"{bucket['bucket']}={bucket['count']}" for bucket in report["latency_buckets"]),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing optional columns: " + "; ".join(formatted))
    if not report["flagged_mentions"]:
        lines.append("No missing or delayed reply drafts.")
        return "\n".join(lines)

    lines.extend(["", "Flagged mentions:"])
    for item in report["flagged_mentions"]:
        handle = item["inbound_author_handle"] or "unknown"
        latency = (
            "missing"
            if item["latency_minutes"] is None
            else f"{item['latency_minutes']:.2f}m"
        )
        lines.append(
            f"- reply_queue:{item['mention_id']} @{handle} "
            f"detected={item['detected_at']} draft={item['draft_created_at'] or '-'} "
            f"latency={latency} reason={item['flag_reason']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    return cast(sqlite3.Connection, conn)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(row)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    detected_column = _detected_column(columns)
    where = []
    params: list[Any] = []
    if detected_column:
        where.extend(
            [
                f"{detected_column} IS NOT NULL",
                f"datetime({detected_column}) >= datetime(?)",
                f"datetime({detected_column}) <= datetime(?)",
            ]
        )
        params.extend([cutoff.isoformat(), now.isoformat()])
    query = "SELECT * FROM reply_queue"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns, detected_column)
    cursor = conn.execute(query, params)
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _build_item(
    row: dict[str, Any],
    columns: set[str],
    *,
    threshold_minutes: int,
) -> dict[str, Any]:
    detected_at = _parse_datetime(_first_value(row, columns, "detected_at", "received_at"))
    has_draft = bool(str(_value(row, columns, "draft_text") or "").strip())
    draft_created_at = _draft_created_at(row, columns, detected_at, has_draft)
    latency = _round_or_none(_elapsed_minutes(detected_at, draft_created_at))
    missing_draft = not has_draft
    delayed = latency is not None and latency > threshold_minutes
    flag_reason = None
    if missing_draft:
        flag_reason = "missing_draft"
    elif delayed:
        flag_reason = "delayed_draft"

    return {
        "mention_id": _int_or_none(row.get("id") or row.get("reply_queue_id")),
        "platform": str(_value(row, columns, "platform") or "x"),
        "inbound_tweet_id": _value(row, columns, "inbound_tweet_id"),
        "inbound_author_handle": _value(row, columns, "inbound_author_handle"),
        "detected_at": detected_at.isoformat() if detected_at else None,
        "draft_created_at": draft_created_at.isoformat() if draft_created_at else None,
        "latency_minutes": latency,
        "latency_bucket": _bucket_name(latency),
        "status": str(_value(row, columns, "status") or "pending"),
        "missing_draft": missing_draft,
        "delayed": delayed,
        "flagged": bool(flag_reason),
        "flag_reason": flag_reason,
    }


def _draft_created_at(
    row: dict[str, Any],
    columns: set[str],
    detected_at: datetime | None,
    has_draft: bool,
) -> datetime | None:
    if not has_draft:
        return None
    explicit = _parse_datetime(
        _first_value(row, columns, "draft_created_at", "first_reply_draft_at", "created_at")
    )
    return explicit or detected_at


def _missing_columns(columns: set[str]) -> list[str]:
    missing = []
    if "draft_text" not in columns:
        missing.append("draft_text")
    if not _detected_column(columns):
        missing.append("detected_at")
    return missing


def _detected_column(columns: set[str]) -> str | None:
    for column in ("detected_at", "received_at", "created_at"):
        if column in columns:
            return column
    return None


def _order_clause(columns: set[str], detected_column: str | None) -> str:
    parts = []
    if detected_column:
        parts.append(f"datetime({detected_column}) ASC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _totals(items: list[dict[str, Any]], flagged: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["flag_reason"] for item in flagged)
    return {
        "mention_count": len(items),
        "drafted_count": sum(1 for item in items if not item["missing_draft"]),
        "missing_draft_count": counts["missing_draft"],
        "delayed_count": counts["delayed_draft"],
        "flagged_count": len(flagged),
    }


def _summary(items: list[dict[str, Any]]) -> dict[str, float | int | None]:
    latencies = sorted(
        float(item["latency_minutes"])
        for item in items
        if item["latency_minutes"] is not None
    )
    return {
        "count": len(latencies),
        "minimum": _round_or_none(latencies[0] if latencies else None),
        "median": _round_or_none(float(median(latencies)) if latencies else None),
        "p90": _round_or_none(_percentile(latencies, 0.9)),
        "maximum": _round_or_none(latencies[-1] if latencies else None),
        "average": _round_or_none(float(mean(latencies)) if latencies else None),
    }


def _latency_buckets(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(item["latency_bucket"] for item in items)
    return [
        {"bucket": name, "count": counts[name], "min_minutes": low, "max_minutes": high}
        for name, low, high in BUCKETS
    ] + [
        {
            "bucket": "missing_draft",
            "count": counts["missing_draft"],
            "min_minutes": None,
            "max_minutes": None,
        }
    ]


def _bucket_name(latency: float | None) -> str:
    if latency is None:
        return "missing_draft"
    for name, low, high in BUCKETS:
        if high is None:
            if latency > low:
                return name
        elif low <= latency <= high:
            return name
    return BUCKETS[-1][0]


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str],
) -> dict[str, Any]:
    return {
        "artifact_type": "reply_response_latency",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals([], []),
        "summary": _summary([]),
        "latency_buckets": _latency_buckets([]),
        "rows": [],
        "flagged_mentions": [],
        "source_table": None,
        "missing_tables": missing_tables,
        "missing_columns": {},
    }


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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _item_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["detected_at"] or "", item["mention_id"] or 0)


def _flagged_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    latency = item["latency_minutes"]
    missing_rank = 0 if item["flag_reason"] == "missing_draft" else 1
    latency_rank = float("inf") if latency is None else -float(latency)
    return (missing_rank, latency_rank, item["detected_at"] or "", item["mention_id"] or 0)


def _text_number(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"
