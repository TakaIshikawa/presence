"""Report publish queue to attempt outcome latency SLA breaches."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_FIRST_ATTEMPT_MINUTES = 30
DEFAULT_SUCCESS_MINUTES = 180
DEFAULT_TERMINAL_FAILURE_MINUTES = 180
PLATFORM_THRESHOLDS = {
    "x": {"first_attempt": 15, "success": 120, "terminal_failure": 120},
    "bluesky": {"first_attempt": 30, "success": 180, "terminal_failure": 180},
    "default": {
        "first_attempt": DEFAULT_FIRST_ATTEMPT_MINUTES,
        "success": DEFAULT_SUCCESS_MINUTES,
        "terminal_failure": DEFAULT_TERMINAL_FAILURE_MINUTES,
    },
}
SUCCESS_STATUSES = {"success", "succeeded", "published", "posted", "complete", "completed", "ok"}
FAILURE_STATUSES = {"failed", "failure", "error", "errored", "terminal_failure", "dead_letter", "cancelled"}


def build_publish_attempt_latency_sla_report(
    queue_rows: list[dict[str, Any]],
    attempt_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic queue-to-attempt SLA breach report."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    attempts_by_queue: dict[str, list[dict[str, Any]]] = {}
    attempts_by_content_platform: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in attempt_rows:
        attempt = _normalize_attempt(row)
        if attempt["queue_item_id"]:
            attempts_by_queue.setdefault(attempt["queue_item_id"], []).append(attempt)
        attempts_by_content_platform.setdefault((attempt["content_id"], attempt["platform"]), []).append(attempt)

    findings: list[dict[str, Any]] = []
    stage_counts: Counter[str] = Counter()
    missing_timestamp_count = 0
    evaluated_count = 0

    for raw in queue_rows:
        queue = _normalize_queue(raw)
        ready_at = queue["ready_at_dt"]
        if ready_at is None:
            missing_timestamp_count += 1
            continue
        evaluated_count += 1
        attempts = attempts_by_queue.get(queue["queue_item_id"], [])
        if not attempts:
            attempts = attempts_by_content_platform.get((queue["content_id"], queue["platform"]), [])
        attempts = [attempt for attempt in attempts if attempt["attempted_at_dt"] is not None and attempt["attempted_at_dt"] >= ready_at]
        attempts.sort(key=lambda attempt: (attempt["attempted_at_dt"], attempt["attempt_id"]))

        first_attempt = attempts[0] if attempts else None
        success = next((attempt for attempt in attempts if attempt["success"]), None)
        terminal_failure = _terminal_failure(queue, attempts)
        for stage, at_dt in (
            ("first_attempt", first_attempt["attempted_at_dt"] if first_attempt else None),
            ("success", success["attempted_at_dt"] if success else queue["published_at_dt"]),
            ("terminal_failure", terminal_failure),
        ):
            if at_dt is None:
                continue
            latency = _minutes_between(ready_at, at_dt)
            threshold = _threshold(queue["platform"], stage)
            if latency is not None and latency > threshold:
                stage_counts[stage] += 1
                findings.append(_finding(queue, stage, latency, threshold, ready_at, at_dt))

    findings.sort(key=lambda item: (-item["latency_minutes"], item["platform"], item["queue_item_id"], item["breached_stage"]))
    shown = findings[:limit]
    return {
        "artifact_type": "publish_attempt_latency_sla",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "platform_minutes": PLATFORM_THRESHOLDS,
            "limit": limit,
        },
        "summary": {
            "queue_item_count": len(queue_rows),
            "attempt_count": len(attempt_rows),
            "evaluated_queue_item_count": evaluated_count,
            "missing_ready_timestamp_count": missing_timestamp_count,
            "breach_count": len(findings),
            "shown_count": len(shown),
            "by_stage": dict(sorted(stage_counts.items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": shown,
    }


def build_publish_attempt_latency_sla_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"publish_queue": {"id", "content_id"}, "publication_attempts": {"content_id", "platform"}}
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns: dict[str, list[str]] = {}
    if "publish_queue" in schema:
        pq_missing = set()
        if "id" not in schema["publish_queue"]:
            pq_missing.add("id")
        if "content_id" not in schema["publish_queue"]:
            pq_missing.add("content_id")
        if not _has_any(schema["publish_queue"], ("scheduled_at", "ready_at", "created_at")):
            pq_missing.add("scheduled_at|ready_at|created_at")
        if pq_missing:
            missing_columns["publish_queue"] = sorted(pq_missing)
    if "publication_attempts" in schema:
        pa_missing = set()
        if "content_id" not in schema["publication_attempts"]:
            pa_missing.add("content_id")
        if "platform" not in schema["publication_attempts"]:
            pa_missing.add("platform")
        if not _has_any(schema["publication_attempts"], ("attempted_at", "started_at", "created_at")):
            pa_missing.add("attempted_at|started_at|created_at")
        if pa_missing:
            missing_columns["publication_attempts"] = sorted(pa_missing)
    return build_publish_attempt_latency_sla_report(
        _load_queue_rows(conn, schema) if "publish_queue" in schema and "id" in schema["publish_queue"] and "content_id" in schema["publish_queue"] else [],
        _load_attempt_rows(conn, schema) if "publication_attempts" in schema else [],
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_publish_attempt_latency_sla_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_attempt_latency_sla_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publish Attempt Latency SLA",
        f"Generated: {report['generated_at']}",
        f"Totals: queue_items={summary['queue_item_count']} attempts={summary['attempt_count']} breaches={summary['breach_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No publish attempt latency SLA breaches found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for finding in report["findings"]:
        lines.append(
            f"  - queue={finding['queue_item_id']} content={finding['content_id']} "
            f"platform={finding['platform']} type={finding['content_type']} "
            f"stage={finding['breached_stage']} latency={finding['latency_minutes']}m "
            f"threshold={finding['threshold_minutes']}m bucket={finding['latency_bucket']}"
        )
    return "\n".join(lines)


def _load_queue_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    gc = schema.get("generated_content", set())
    select = [
        _expr(pq, "id", fallback="rowid", alias="pq") + " AS queue_item_id",
        _expr(pq, "content_id", fallback="NULL", alias="pq") + " AS content_id",
        _expr(pq, "platform", fallback="'all'", alias="pq") + " AS platform",
        _expr(pq, "status", fallback="NULL", alias="pq") + " AS status",
        _expr(pq, "scheduled_at", "ready_at", "created_at", fallback="NULL", alias="pq") + " AS ready_at",
        _expr(pq, "published_at", fallback="NULL", alias="pq") + " AS published_at",
        _expr(gc, "content_type", fallback="NULL", alias="gc") + " AS content_type",
    ]
    join = "LEFT JOIN generated_content gc ON gc.id = pq.content_id" if "generated_content" in schema and "id" in gc else ""
    order = _expr(pq, "scheduled_at", "ready_at", "created_at", fallback="pq.rowid", alias="pq")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue pq {join} ORDER BY {order}, pq.rowid").fetchall()]


def _load_attempt_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pa = schema["publication_attempts"]
    select = [
        _expr(pa, "id", fallback="rowid") + " AS attempt_id",
        _expr(pa, "queue_id", fallback="NULL") + " AS queue_item_id",
        _expr(pa, "content_id", fallback="NULL") + " AS content_id",
        _expr(pa, "platform", fallback="'unknown'") + " AS platform",
        _expr(pa, "attempted_at", "started_at", "created_at", fallback="NULL") + " AS attempted_at",
        _expr(pa, "success", fallback="NULL") + " AS success",
        _expr(pa, "status", fallback="NULL") + " AS status",
        _expr(pa, "failed_at", fallback="NULL") + " AS failed_at",
        _expr(pa, "succeeded_at", fallback="NULL") + " AS succeeded_at",
    ]
    order = _expr(pa, "attempted_at", "started_at", "created_at", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publication_attempts ORDER BY {order}, rowid").fetchall()]


def _normalize_queue(row: dict[str, Any]) -> dict[str, Any]:
    platform = _clean(row.get("platform")).lower() or "all"
    return {
        "queue_item_id": _clean(row.get("queue_item_id")),
        "content_id": _clean(row.get("content_id")),
        "platform": platform,
        "content_type": _clean(row.get("content_type")) or "unknown",
        "status": _clean(row.get("status")).lower(),
        "ready_at": row.get("ready_at"),
        "ready_at_dt": _parse_time(row.get("ready_at")),
        "published_at_dt": _parse_time(row.get("published_at")),
    }


def _normalize_attempt(row: dict[str, Any]) -> dict[str, Any]:
    status = _clean(row.get("status")).lower()
    success = _is_success(row.get("success"), status)
    attempted_at = _parse_time(row.get("succeeded_at")) if success and row.get("succeeded_at") else _parse_time(row.get("attempted_at"))
    return {
        "attempt_id": _clean(row.get("attempt_id")),
        "queue_item_id": _clean(row.get("queue_item_id")),
        "content_id": _clean(row.get("content_id")),
        "platform": _clean(row.get("platform")).lower() or "unknown",
        "status": status,
        "success": success,
        "failed": _is_failure(row.get("success"), status),
        "attempted_at_dt": attempted_at,
        "failed_at_dt": _parse_time(row.get("failed_at")) or attempted_at if _is_failure(row.get("success"), status) else None,
    }


def _terminal_failure(queue: dict[str, Any], attempts: list[dict[str, Any]]) -> datetime | None:
    failures = [attempt for attempt in attempts if attempt["failed"] and attempt["failed_at_dt"] is not None]
    if _clean(queue.get("status")).lower() in FAILURE_STATUSES and failures:
        return failures[-1]["failed_at_dt"]
    if attempts and all(attempt["failed"] for attempt in attempts):
        return attempts[-1]["failed_at_dt"]
    return None


def _finding(queue: dict[str, Any], stage: str, latency: float, threshold: int, ready_at: datetime, stage_at: datetime) -> dict[str, Any]:
    return {
        "queue_item_id": queue["queue_item_id"],
        "content_id": queue["content_id"],
        "platform": queue["platform"],
        "content_type": queue["content_type"],
        "breached_stage": stage,
        "latency_minutes": round(latency, 2),
        "latency_bucket": _bucket(latency),
        "threshold_minutes": threshold,
        "ready_at": ready_at.isoformat(),
        "stage_at": stage_at.isoformat(),
    }


def _threshold(platform: str, stage: str) -> int:
    return PLATFORM_THRESHOLDS.get(platform, PLATFORM_THRESHOLDS["default"])[stage]


def _bucket(minutes: float) -> str:
    if minutes <= 15:
        return "<=15m"
    if minutes <= 60:
        return "15m-1h"
    if minutes <= 360:
        return "1h-6h"
    if minutes <= 1440:
        return "6h-24h"
    return ">24h"


def _minutes_between(start: datetime, end: datetime) -> float | None:
    minutes = (end - start).total_seconds() / 60
    return minutes if minutes >= 0 else None


def _is_success(value: Any, status: str) -> bool:
    if status in SUCCESS_STATUSES:
        return True
    return str(value).strip().lower() in {"1", "true", "yes"}


def _is_failure(value: Any, status: str) -> bool:
    if status in FAILURE_STATUSES:
        return True
    return str(value).strip().lower() in {"0", "false", "no"} and status not in SUCCESS_STATUSES


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _expr(columns: set[str], *names: str, fallback: str, alias: str | None = None) -> str:
    prefix = f"{alias}." if alias else ""
    found = [f"{prefix}{name}" for name in names if name in columns]
    if not found:
        return fallback
    return found[0] if len(found) == 1 else "COALESCE(" + ", ".join(found) + ")"


def _has_any(columns: set[str], names: tuple[str, ...]) -> bool:
    return any(name in columns for name in names)


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
