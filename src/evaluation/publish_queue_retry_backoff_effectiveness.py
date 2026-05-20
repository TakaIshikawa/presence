"""Evaluate publish queue retry backoff effectiveness."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_MIN_BACKOFF_MINUTES = 5.0
DEFAULT_MAX_BACKOFF_HOURS = 24.0
DEFAULT_STALE_FAILED_HOURS = 24.0
DEFAULT_LIMIT = 100
RETRY_STATUSES = {"failed", "queued", "retry", "retrying"}


def build_publish_queue_retry_backoff_effectiveness_report(
    rows: list[dict[str, Any]],
    *,
    min_backoff_minutes: float = DEFAULT_MIN_BACKOFF_MINUTES,
    max_backoff_hours: float = DEFAULT_MAX_BACKOFF_HOURS,
    stale_failed_hours: float = DEFAULT_STALE_FAILED_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if min_backoff_minutes < 0:
        raise ValueError("min_backoff_minutes must be non-negative")
    if max_backoff_hours <= 0 or stale_failed_hours < 0:
        raise ValueError("hour thresholds must be non-negative and max_backoff_hours positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    min_backoff = timedelta(minutes=min_backoff_minutes)
    max_backoff = timedelta(hours=max_backoff_hours)
    stale_failed = timedelta(hours=stale_failed_hours)
    violations: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        status = _norm(row.get("status"), "unknown")
        if status not in RETRY_STATUSES:
            continue
        scanned += 1
        created_at = _parse_dt(row.get("created_at"))
        scheduled_at = _parse_dt(row.get("scheduled_at"))
        platform = _norm(row.get("platform"), "unknown")
        error_category = _norm(row.get("error_category"), "unknown")
        backoff = (scheduled_at - created_at) if created_at and scheduled_at else None
        categories = []
        if scheduled_at and scheduled_at <= generated_at and status in {"queued", "retry", "retrying"}:
            categories.append("overdue_retry")
        if status == "failed" and not scheduled_at and created_at and generated_at - created_at >= stale_failed:
            categories.append("stale_failed_unscheduled")
        if backoff is not None and backoff < min_backoff:
            categories.append("immediate_retry")
        if backoff is not None and backoff > max_backoff:
            categories.append("excessive_delay")
        for category in categories:
            violations.append(
                {
                    "queue_id": row.get("queue_id"),
                    "platform": platform,
                    "status": status,
                    "error_category": error_category,
                    "violation_type": category,
                    "created_at": created_at.isoformat() if created_at else None,
                    "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
                    "backoff_hours": round(backoff.total_seconds() / 3600, 2) if backoff is not None else None,
                }
            )
    violations.sort(key=lambda item: (item["platform"], item["status"], item["error_category"], item["violation_type"], item["queue_id"] or 0))
    return {
        "artifact_type": "publish_queue_retry_backoff_effectiveness",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "min_backoff_minutes": min_backoff_minutes,
            "max_backoff_hours": max_backoff_hours,
            "stale_failed_hours": stale_failed_hours,
            "limit": limit,
        },
        "summary": {
            "rows_scanned": len(rows),
            "retry_rows_scanned": scanned,
            "violation_count": len(violations),
            "shown_count": min(len(violations), limit),
            "by_violation_type": dict(sorted(Counter(item["violation_type"] for item in violations).items())),
        },
        "grouped_violations": _grouped(violations),
        "violations": violations[:limit],
        "missing_tables": list(missing_tables),
    }


def build_publish_queue_retry_backoff_effectiveness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing = tuple(table for table in ("publish_queue",) if table not in schema)
    rows = [] if missing else _load_rows(conn, schema["publish_queue"])
    return build_publish_queue_retry_backoff_effectiveness_report(rows, missing_tables=missing, **kwargs)


def format_publish_queue_retry_backoff_effectiveness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_retry_backoff_effectiveness_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publish Queue Retry Backoff Effectiveness",
        f"Generated: {report['generated_at']}",
        f"Thresholds: min_backoff_minutes={report['thresholds']['min_backoff_minutes']} max_backoff_hours={report['thresholds']['max_backoff_hours']} stale_failed_hours={report['thresholds']['stale_failed_hours']}",
        f"Totals: retry_rows={summary['retry_rows_scanned']} violations={summary['violation_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["grouped_violations"]:
        lines.extend(["", "platform | status | error_category | violation_type | count"])
        for row in report["grouped_violations"]:
            lines.append(f"{row['platform']} | {row['status']} | {row['error_category']} | {row['violation_type']} | {row['count']}")
    elif not report["missing_tables"]:
        lines.append("No publish queue retry backoff violations found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "queue_id", "rowid"),
        _expr(columns, "platform", "platform", "'unknown'"),
        _expr(columns, "status", "status", "'unknown'"),
        _expr(columns, "error_category", "error_category", "'unknown'"),
        _expr(columns, "created_at", "created_at", "NULL"),
        _expr(columns, "scheduled_at", "scheduled_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue ORDER BY rowid")]


def _grouped(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["platform"], item["status"], item["error_category"], item["violation_type"]) for item in items)
    return [
        {"platform": platform, "status": status, "error_category": error_category, "violation_type": violation_type, "count": count}
        for (platform, status, error_category, violation_type), count in sorted(counts.items())
    ]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {row[0]: {col[1] for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _norm(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or default
