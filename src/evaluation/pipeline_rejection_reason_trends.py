"""Summarize non-published pipeline run rejection reasons."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_HOURS = 168
DEFAULT_MIN_COUNT = 1
DEFAULT_LIMIT = 25


def build_pipeline_rejection_reason_trends_report(
    rows: list[dict[str, Any]],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int = DEFAULT_LIMIT,
    include_published: bool = False,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(hours=window_hours)
    normalized = [_normalize_row(row) for row in rows]
    buckets = _buckets(normalized, min_count=min_count, limit=limit)
    return {
        "artifact_type": "pipeline_rejection_reason_trends",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "window_hours": window_hours,
            "window_start": window_start.isoformat(),
            "window_end": generated_at.isoformat(),
            "min_count": min_count,
            "limit": limit,
            "include_published": include_published,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "rows_scanned": len(rows),
            "bucket_count": len(buckets),
            "non_published_rows": sum(1 for row in normalized if row["outcome"] != "published"),
        },
        "rejection_buckets": buckets,
    }


def build_pipeline_rejection_reason_trends_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("pipeline_runs",) if table not in schema)
    if missing_tables:
        return build_pipeline_rejection_reason_trends_report([], missing_tables=missing_tables, **kwargs)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(hours=kwargs.get("window_hours", DEFAULT_WINDOW_HOURS))
    rows = _load_rows(
        conn,
        schema["pipeline_runs"],
        cutoff=cutoff,
        window_end=now,
        include_published=bool(kwargs.get("include_published", False)),
    )
    return build_pipeline_rejection_reason_trends_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_pipeline_rejection_reason_trends_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_rejection_reason_trends_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Pipeline Rejection Reason Trends",
        f"Generated: {report['generated_at']}",
        f"Window: hours={thresholds['window_hours']} start={thresholds['window_start']} end={thresholds['window_end']}",
        f"Thresholds: min_count={thresholds['min_count']} limit={thresholds['limit']} include_published={thresholds['include_published']}",
        f"Totals: rows={summary['rows_scanned']} non_published={summary['non_published_rows']} buckets={summary['bucket_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["rejection_buckets"]:
        lines.extend(["", "Rejection buckets:"])
        for bucket in report["rejection_buckets"]:
            lines.append(
                f"- outcome={bucket['outcome']} reason={bucket['rejection_reason']} "
                f"count={bucket['count']} examples={','.join(str(item) for item in bucket['example_run_ids'])}"
            )
    elif not report["missing_tables"]:
        lines.append("No pipeline rejection reason buckets exceeded thresholds.")
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
    include_published: bool,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", default="rowid"),
        _column_expr(columns, "run_id", default="NULL"),
        _column_expr(columns, "outcome", default="'unknown'"),
        _column_expr(columns, "rejection_reason", default="NULL"),
        _column_expr(columns, "created_at", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "created_at" in columns:
        filters.append("datetime(created_at) >= datetime(?) AND datetime(created_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    if not include_published and "outcome" in columns:
        filters.append("LOWER(COALESCE(outcome, '')) != 'published'")
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM pipeline_runs {where} ORDER BY datetime(created_at) DESC, id DESC", params)]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _int(row.get("id")),
        "run_id": row.get("run_id") or row.get("id"),
        "outcome": _norm(row.get("outcome")),
        "rejection_reason": _reason(row.get("rejection_reason")),
        "created_at": row.get("created_at"),
    }


def _buckets(rows: list[dict[str, Any]], *, min_count: int, limit: int) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(row["outcome"], row["rejection_reason"])].append(row)
    buckets = []
    for (outcome, reason), bucket in grouped.items():
        if len(bucket) < min_count:
            continue
        buckets.append(
            {
                "outcome": outcome,
                "rejection_reason": reason,
                "count": len(bucket),
                "example_run_ids": [row["run_id"] for row in bucket[:5]],
                "latest_created_at": bucket[0]["created_at"],
            }
        )
    buckets.sort(key=lambda item: (-item["count"], item["outcome"], item["rejection_reason"]))
    return buckets[:limit]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, *, default: str) -> str:
    return name if name in columns else f"{default} AS {name}"


def _reason(value: Any) -> str:
    text = _norm(value)
    if text == "unknown":
        return "unknown"
    return " ".join(text.replace("_", " ").split())


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
