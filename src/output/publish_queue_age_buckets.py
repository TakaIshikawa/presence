"""Bucket open publish queue items by queue age."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_BUCKET_HOURS = (1.0, 6.0, 24.0, 72.0)
DEFAULT_STALE_THRESHOLD_HOURS = 72.0
OPEN_STATUSES = {"queued", "held", "failed"}
CLOSED_STATUSES = {"published", "posted", "cancelled", "canceled", "dismissed"}


def age_bucket(
    age_hours: float,
    bucket_hours: Iterable[float] = DEFAULT_BUCKET_HOURS,
) -> str:
    """Return a deterministic bucket label for an item age in hours."""
    thresholds = _validate_bucket_hours(tuple(bucket_hours))
    if age_hours < 0:
        return "future"
    lower = 0.0
    for threshold in thresholds:
        if lower <= age_hours < threshold:
            return f"{_duration_label(lower)}-{_duration_label(threshold)}"
        lower = threshold
    return f"{_duration_label(thresholds[-1])}+"


def build_publish_queue_age_bucket_report(
    db_or_rows: Any,
    *,
    bucket_hours: Iterable[float] = DEFAULT_BUCKET_HOURS,
    stale_threshold_hours: float = DEFAULT_STALE_THRESHOLD_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return open publish queue items grouped by age bucket, platform, and status."""
    buckets = _validate_bucket_hours(tuple(bucket_hours))
    if stale_threshold_hours < 0:
        raise ValueError("stale_threshold_hours must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _maybe_connection(db_or_rows)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    if conn is not None:
        schema = _schema(conn)
        missing_tables, missing_columns = _schema_gaps(schema)
        raw_rows = [] if missing_tables or missing_columns else _load_rows(conn, schema)
    else:
        raw_rows = [dict(row) for row in db_or_rows]

    items = [
        item
        for item in (
            _classify_row(
                row,
                now=generated_at,
                bucket_hours=buckets,
                stale_threshold_hours=stale_threshold_hours,
            )
            for row in raw_rows
        )
        if item is not None
    ]
    items.sort(
        key=lambda item: (-item["age_hours"], item["platform"], item["status"], item["queue_id"])
    )

    bucket_order = [
        "future",
        *[_range_label(index, buckets) for index in range(len(buckets))],
        f"{_duration_label(buckets[-1])}+",
    ]
    bucket_reports = {
        label: {
            "label": label,
            "count": 0,
            "by_platform": {},
            "by_status": {},
        }
        for label in bucket_order
    }
    for item in items:
        bucket = bucket_reports[item["bucket"]]
        bucket["count"] += 1
        _increment(bucket["by_platform"], item["platform"])
        _increment(bucket["by_status"], item["status"])

    stale_items = [item for item in items if item["stale"]]
    by_platform = Counter(item["platform"] for item in items)
    by_status = Counter(item["status"] for item in items)
    by_bucket = Counter(item["bucket"] for item in items)

    return {
        "artifact_type": "publish_queue_age_buckets",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "bucket_hours": list(buckets),
            "open_statuses": sorted(OPEN_STATUSES),
            "stale_threshold_hours": stale_threshold_hours,
        },
        "counts": {
            "rows_scanned": len(raw_rows),
            "items": len(items),
            "stale_items": len(stale_items),
            "by_bucket": {label: by_bucket.get(label, 0) for label in bucket_order},
            "by_platform": dict(sorted(by_platform.items())),
            "by_status": dict(sorted(by_status.items())),
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "buckets": [
            {
                **bucket_reports[label],
                "by_platform": dict(sorted(bucket_reports[label]["by_platform"].items())),
                "by_status": dict(sorted(bucket_reports[label]["by_status"].items())),
            }
            for label in bucket_order
        ],
        "stale_items": stale_items,
    }


def format_publish_queue_age_bucket_json(report: dict[str, Any]) -> str:
    """Render the publish queue age bucket report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    gc = schema.get("generated_content", set())
    join_generated_content = "generated_content" in schema and "content_id" in pq and "id" in gc
    select_columns = [
        "pq.id",
        _column_expr(pq, "content_id", alias="pq"),
        _column_expr(pq, "platform", "'unknown'", alias="pq"),
        _column_expr(pq, "status", "'queued'", alias="pq"),
        _column_expr(pq, "created_at", alias="pq"),
        _column_expr(pq, "scheduled_at", alias="pq"),
        _column_expr(pq, "published_at", alias="pq"),
        _column_expr(
            gc if join_generated_content else set(),
            "content_type",
            alias="gc" if join_generated_content else None,
        ),
    ]
    join = (
        "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        if join_generated_content
        else ""
    )
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM publish_queue pq
                {join}
                ORDER BY pq.id ASC"""
        ).fetchall()
    ]


def _classify_row(
    row: dict[str, Any],
    *,
    now: datetime,
    bucket_hours: tuple[float, ...],
    stale_threshold_hours: float,
) -> dict[str, Any] | None:
    status = _status(row.get("status")) or "queued"
    if status in CLOSED_STATUSES or status not in OPEN_STATUSES:
        return None
    if _clean(row.get("published_at")):
        return None

    queued_at = (
        _parse_datetime(row.get("created_at"))
        or _parse_datetime(row.get("queued_at"))
        or _parse_datetime(row.get("scheduled_at"))
    )
    if queued_at is None:
        return None

    age_hours = round((now - queued_at).total_seconds() / 3600, 2)
    bucket = age_bucket(age_hours, bucket_hours)
    queue_id = _int_value(row.get("queue_id") or row.get("id"))
    content_id = _int_value(row.get("content_id"))
    return {
        "queue_id": queue_id,
        "content_id": content_id,
        "content_type": _clean(row.get("content_type")),
        "platform": _clean(row.get("platform")) or "unknown",
        "status": status,
        "created_at": queued_at.isoformat(),
        "scheduled_at": _iso_datetime(row.get("scheduled_at")),
        "age_hours": age_hours,
        "bucket": bucket,
        "stale": age_hours >= stale_threshold_hours,
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {"publish_queue": {"id"}}
    missing_tables = [table for table in sorted(required) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _maybe_connection(db_or_rows: Any) -> sqlite3.Connection | None:
    conn = getattr(db_or_rows, "conn", db_or_rows)
    if isinstance(conn, sqlite3.Connection):
        conn.row_factory = sqlite3.Row
        return conn
    return None


def _validate_bucket_hours(bucket_hours: tuple[float, ...]) -> tuple[float, ...]:
    if not bucket_hours:
        raise ValueError("bucket_hours must not be empty")
    parsed = tuple(float(value) for value in bucket_hours)
    if any(value <= 0 for value in parsed):
        raise ValueError("bucket_hours values must be positive")
    return tuple(sorted(dict.fromkeys(parsed)))


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str | None = None,
) -> str:
    if column in columns:
        prefix = f"{alias}." if alias else ""
        return f"{prefix}{column} AS {column}"
    return f"{fallback} AS {column}"


def _parse_datetime(value: Any) -> datetime | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(cleaned))
    except ValueError:
        return None


def _iso_datetime(value: Any) -> str | None:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed is not None else None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _status(value: Any) -> str | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    return cleaned.lower().replace("-", "_").replace(" ", "_")


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _duration_label(hours: float) -> str:
    if hours == 0:
        return "0h"
    if hours % 24 == 0:
        days = hours / 24
        if days >= 1:
            return f"{days:g}d"
    return f"{hours:g}h"


def _range_label(index: int, thresholds: tuple[float, ...]) -> str:
    lower = 0.0 if index == 0 else thresholds[index - 1]
    upper = thresholds[index]
    return f"{_duration_label(lower)}-{_duration_label(upper)}"


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
