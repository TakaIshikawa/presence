"""Report active publish queue rows whose schedule has drifted past due."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_GRACE_HOURS = 2
DEFAULT_LIMIT = 100
DEFAULT_STATUS = ("queued", "held", "publishing")
DEFAULT_PLATFORM = "all"
SUCCESS_STATUSES = {"success", "succeeded", "published", "sent", "ok"}


def build_publication_queue_schedule_drift_report(
    rows: list[dict[str, Any]],
    *,
    grace_hours: int = DEFAULT_GRACE_HOURS,
    status: str | Iterable[str] = DEFAULT_STATUS,
    platform: str = DEFAULT_PLATFORM,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if grace_hours < 0:
        raise ValueError("grace_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=grace_hours)
    statuses = _normalize_filter(status, default=DEFAULT_STATUS)
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    drift_items: list[dict[str, Any]] = []
    scanned = 0

    for row in rows:
        row_status = _clean(row.get("status"), "unknown").lower()
        row_platform = _clean(row.get("platform"), "unknown").lower()
        if statuses != ("all",) and row_status not in statuses:
            continue
        if normalized_platform != "all" and row_platform != normalized_platform:
            continue
        scheduled_at = _parse_dt(row.get("scheduled_at"))
        if scheduled_at is None:
            continue
        scanned += 1
        if scheduled_at >= cutoff:
            continue
        if _parse_dt(row.get("published_at")) is not None or _truthy(row.get("has_successful_attempt")):
            continue
        drift_hours = round((generated_at - scheduled_at).total_seconds() / 3600, 2)
        drift_bucket = _drift_bucket(drift_hours)
        drift_items.append(
            {
                "queue_id": _int_or_none(row.get("queue_id") or row.get("id")),
                "content_id": _int_or_none(row.get("content_id")),
                "platform": row_platform,
                "status": row_status,
                "scheduled_at": scheduled_at.isoformat(),
                "drift_hours": drift_hours,
                "drift_bucket": drift_bucket,
                "attempt_count": _int_or_none(row.get("attempt_count")) or 0,
                "last_attempt_status": row.get("last_attempt_status"),
            }
        )

    drift_items.sort(key=lambda item: (-item["drift_hours"], item["platform"], item["queue_id"] or 0))
    shown = drift_items[:limit]
    return {
        "artifact_type": "publication_queue_schedule_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "grace_hours": grace_hours,
            "overdue_before": cutoff.isoformat(),
            "status": list(statuses),
            "platform": normalized_platform,
            "limit": limit,
        },
        "drift_items": shown,
        "bucket_summary": _bucket_summary(drift_items),
        "summary": {
            "rows_scanned": scanned,
            "drift_count": len(drift_items),
            "shown_count": len(shown),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_publication_queue_schedule_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, schema, missing_tables, missing_columns)
    return build_publication_queue_schedule_drift_report(
        rows,
        missing_tables=sorted(set(missing_tables)),
        missing_columns=missing_columns,
        **kwargs,
    )


def format_publication_queue_schedule_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_queue_schedule_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    thresholds = report["thresholds"]
    lines = [
        "Publication Queue Schedule Drift",
        f"Generated: {report['generated_at']}",
        f"Grace: {thresholds['grace_hours']} hours",
        f"Status: {', '.join(thresholds['status'])}",
        f"Platform: {thresholds['platform']}",
        f"Totals: scanned={summary['rows_scanned']} drift={summary['drift_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["drift_items"]:
        lines.append("No publication queue schedule drift found.")
        return "\n".join(lines)
    lines.extend(["", "platform | status | bucket | count"])
    for bucket in report["bucket_summary"]:
        lines.append(f"{bucket['platform']} | {bucket['status']} | {bucket['drift_bucket']} | {bucket['count']}")
    lines.extend(["", "queue_id | content_id | platform | status | drift_hours | scheduled_at"])
    for item in report["drift_items"]:
        lines.append(
            f"{item['queue_id'] or '-'} | {item['content_id'] or '-'} | {item['platform']} | "
            f"{item['status']} | {item['drift_hours']} | {item['scheduled_at']}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        missing_tables.append("publish_queue")
        return []
    pq = schema["publish_queue"]
    required = {"id", "scheduled_at"}
    missing = sorted(required - pq)
    if missing:
        missing_columns["publish_queue"] = missing
        return []
    attempts = schema.get("publication_attempts", set())
    attempt_join = ""
    attempt_select = ["0 AS has_successful_attempt", "0 AS attempt_count", "NULL AS last_attempt_status"]
    if attempts and ("queue_id" in attempts or "publish_queue_id" in attempts):
        q_col = "queue_id" if "queue_id" in attempts else "publish_queue_id"
        status_expr = _expr(attempts, "status", "pa", "attempt_status", "'unknown'").removesuffix(" AS attempt_status")
        attempt_join = (
            "LEFT JOIN publication_attempts pa ON pa.id = ("
            "SELECT pa2.id FROM publication_attempts pa2 "
            f"WHERE pa2.{q_col} = pq.id ORDER BY pa2.rowid DESC LIMIT 1)"
        )
        success_exists = (
            f"EXISTS (SELECT 1 FROM publication_attempts ps WHERE ps.{q_col} = pq.id "
            f"AND lower(COALESCE({_attempt_status_expr(attempts, 'ps')}, '')) IN ({_quoted(SUCCESS_STATUSES)}))"
        )
        attempt_count = f"(SELECT COUNT(*) FROM publication_attempts pc WHERE pc.{q_col} = pq.id)"
        attempt_select = [f"{success_exists} AS has_successful_attempt", f"{attempt_count} AS attempt_count", f"{status_expr} AS last_attempt_status"]
    select = [
        "pq.id AS queue_id",
        _expr(pq, "content_id", "pq", "content_id", "NULL"),
        _expr(pq, "platform", "pq", "platform", "'unknown'"),
        _expr(pq, "status", "pq", "status", "'queued'"),
        "pq.scheduled_at AS scheduled_at",
        _expr(pq, "published_at", "pq", "published_at", "NULL"),
        *attempt_select,
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue pq {attempt_join} ORDER BY pq.rowid ASC")]


def _bucket_summary(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts: Counter[tuple[str, str, str]] = Counter((i["platform"], i["status"], i["drift_bucket"]) for i in items)
    return [
        {"platform": platform, "status": status, "drift_bucket": bucket, "count": count}
        for (platform, status, bucket), count in sorted(counts.items())
    ]


def _drift_bucket(hours: float) -> str:
    if hours < 6:
        return "lt_6h"
    if hours < 24:
        return "6_24h"
    if hours < 72:
        return "1_3d"
    return "gt_3d"


def _attempt_status_expr(columns: set[str], alias: str) -> str:
    for col in ("status", "outcome", "result"):
        if col in columns:
            return f"{alias}.{col}"
    return "NULL"


def _quoted(values: set[str]) -> str:
    return ", ".join(f"'{value}'" for value in sorted(values))


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _normalize_filter(value: str | Iterable[str], *, default: Iterable[str]) -> tuple[str, ...]:
    raw = value.split(",") if isinstance(value, str) else list(value)
    parts = tuple(sorted({str(part).strip().lower() for part in raw if str(part).strip()}))
    return parts or tuple(default)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _truthy(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
