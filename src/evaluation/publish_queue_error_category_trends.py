"""Summarize failed publish_queue rows by error category and platform."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_MIN_COUNT = 1
DEFAULT_LIMIT = 25


def build_publish_queue_error_category_trends_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    platform: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    normalized = [_normalize_row(row, generated_at) for row in rows]
    category_buckets = _category_buckets(normalized, min_count=min_count, limit=limit)
    return {
        "artifact_type": "publish_queue_error_category_trends",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "lookback_days": lookback_days,
            "lookback_start": lookback_start.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "min_count": min_count,
            "platform": _norm(platform) if platform else None,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "failed_rows_scanned": len(rows),
            "category_bucket_count": len(category_buckets),
            "platform_bucket_count": len(set(row["platform"] for row in normalized)),
        },
        "category_buckets": category_buckets,
        "platform_buckets": _platform_buckets(normalized),
        "representative_examples": _examples(normalized, limit),
    }


def build_publish_queue_error_category_trends_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("publish_queue",) if table not in schema)
    if missing_tables:
        return build_publish_queue_error_category_trends_report([], missing_tables=missing_tables, **kwargs)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=kwargs.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    rows = _load_rows(
        conn,
        schema["publish_queue"],
        cutoff=cutoff,
        window_end=now,
        platform=kwargs.get("platform"),
    )
    return build_publish_queue_error_category_trends_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_publish_queue_error_category_trends_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_error_category_trends_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Publish Queue Error Category Trends",
        f"Generated: {report['generated_at']}",
        f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} end={thresholds['lookback_end']}",
        f"Thresholds: min_count={thresholds['min_count']} platform={thresholds['platform'] or '-'} limit={thresholds['limit']}",
        f"Totals: failed_rows={summary['failed_rows_scanned']} category_buckets={summary['category_bucket_count']} platform_buckets={summary['platform_bucket_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["category_buckets"]:
        lines.extend(["", "Category buckets:"])
        for bucket in report["category_buckets"]:
            lines.append(
                f"- category={bucket['error_category']} platform={bucket['platform']} "
                f"count={bucket['count']} max_age_hours={bucket['max_age_hours']:.1f} examples={','.join(str(item) for item in bucket['example_queue_ids'])}"
            )
    elif not report["missing_tables"]:
        lines.append("No failed publish queue error category buckets exceeded thresholds.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
    platform: str | None,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", default="rowid"),
        _column_expr(columns, "platform", default="'unknown'"),
        _column_expr(columns, "status", default="'failed'"),
        _column_expr(columns, "error_category", default="NULL"),
        _column_expr(columns, "error_message", default="NULL"),
        _column_expr(columns, "created_at", default="NULL"),
        _column_expr(columns, "scheduled_at", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "status" in columns:
        filters.append("LOWER(COALESCE(status, '')) = 'failed'")
    if "created_at" in columns or "scheduled_at" in columns:
        time_expr = "COALESCE(created_at, scheduled_at)" if {"created_at", "scheduled_at"} <= columns else ("created_at" if "created_at" in columns else "scheduled_at")
        filters.append(f"datetime({time_expr}) >= datetime(?) AND datetime({time_expr}) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    if platform and "platform" in columns:
        filters.append("LOWER(TRIM(platform)) = ?")
        params.append(_norm(platform))
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM publish_queue {where} ORDER BY id", params)]


def _normalize_row(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    created = _parse_dt(row.get("created_at")) or _parse_dt(row.get("scheduled_at"))
    return {
        "queue_id": _int(row.get("id")),
        "platform": _norm(row.get("platform")),
        "status": _norm(row.get("status")),
        "error_category": _norm(row.get("error_category")),
        "error_message": row.get("error_message") or "",
        "created_at": row.get("created_at"),
        "scheduled_at": row.get("scheduled_at"),
        "age_hours": round((now - created).total_seconds() / 3600, 2) if created else None,
    }


def _category_buckets(rows: list[dict[str, Any]], *, min_count: int, limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["error_category"], row["platform"])].append(row)
    buckets = []
    for (category, platform), bucket in grouped.items():
        if len(bucket) < min_count:
            continue
        ages = [row["age_hours"] for row in bucket if row["age_hours"] is not None]
        buckets.append(
            {
                "error_category": category,
                "platform": platform,
                "count": len(bucket),
                "max_age_hours": max(ages) if ages else 0.0,
                "example_queue_ids": [row["queue_id"] for row in bucket[:5]],
                "example_error_messages": [row["error_message"] for row in bucket if row["error_message"]][:3],
            }
        )
    buckets.sort(key=lambda item: (-item["count"], item["error_category"], item["platform"]))
    return buckets[:limit]


def _platform_buckets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter(row["platform"] for row in rows)
    return [{"platform": platform, "count": count} for platform, count in sorted(counts.items())]


def _examples(rows: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    return [
        {
            "queue_id": row["queue_id"],
            "platform": row["platform"],
            "error_category": row["error_category"],
            "error_message": row["error_message"],
            "age_hours": row["age_hours"],
        }
        for row in rows[:limit]
    ]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, *, default: str) -> str:
    return name if name in columns else f"{default} AS {name}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _norm(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or "unknown"


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
