"""Report publication attempts that appear stuck in-flight."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_HOURS = 2.0
DEFAULT_LIMIT = 100
TERMINAL_STATUSES = {
    "cancelled",
    "complete",
    "completed",
    "done",
    "error",
    "failed",
    "failure",
    "ok",
    "published",
    "sent",
    "success",
    "succeeded",
}
NON_TERMINAL_STATUS_ALIASES = {
    "created": "created",
    "in_flight": "in_flight",
    "in-flight": "in_flight",
    "pending": "pending",
    "processing": "processing",
    "queued": "queued",
    "running": "running",
    "started": "started",
}
TIMESTAMP_COLUMNS = ("started_at", "created_at", "attempted_at")
SUCCESS_TIMESTAMP_COLUMNS = ("succeeded_at", "success_at", "published_at", "completed_at")
FAILURE_TIMESTAMP_COLUMNS = ("failed_at", "failure_at", "error_at")


def build_publication_attempt_timeout_risk_report(
    attempt_rows: list[dict[str, Any]],
    *,
    hours: float = DEFAULT_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report from normalized publication attempt rows."""
    if hours <= 0:
        raise ValueError("hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=hours)
    findings = [
        _finding(row, generated_at)
        for row in attempt_rows
        if _is_timeout_risk(row, cutoff)
    ]
    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    grouped_counter = Counter((finding["platform"], finding["status"]) for finding in findings)
    groups = [
        {
            "platform": platform,
            "status": status,
            "count": count,
            "example_attempt_ids": [
                finding["attempt_id"]
                for finding in findings
                if finding["platform"] == platform and finding["status"] == status
            ][:3],
        }
        for (platform, status), count in sorted(grouped_counter.items())
    ]
    return {
        "artifact_type": "publication_attempt_timeout_risk",
        "generated_at": generated_at.isoformat(),
        "filters": {"hours": hours, "limit": limit},
        "summary": {
            "attempt_count": len(attempt_rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_platform_status": groups,
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempts appear stuck in-flight." if not findings else None,
        },
    }


def build_publication_attempt_timeout_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the timeout risk report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "publication_attempts" not in schema:
        return build_publication_attempt_timeout_risk_report(
            [],
            missing_tables=["publication_attempts"],
            **kwargs,
        )

    columns = schema["publication_attempts"]
    missing = _missing_attempt_columns(columns)
    if missing:
        return build_publication_attempt_timeout_risk_report(
            [],
            missing_columns={"publication_attempts": missing},
            **kwargs,
        )

    hours = float(kwargs.get("hours", DEFAULT_HOURS))
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(hours=hours)
    return build_publication_attempt_timeout_risk_report(
        _load_attempt_rows(conn, schema, cutoff=cutoff),
        **kwargs,
    )


def format_publication_attempt_timeout_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_timeout_risk_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publication Attempt Timeout Risk",
        f"Generated: {report['generated_at']}",
        f"Threshold: {report['filters']['hours']} hours",
        f"Limit: {report['filters']['limit']}",
        (
            "Totals: "
            f"attempts={summary['attempt_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in report["missing_columns"].items()
                for column in columns
            )
        )
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Groups:")
    for group in summary["by_platform_status"]:
        examples = ", ".join(str(item) for item in group["example_attempt_ids"])
        lines.append(
            f"  - {group['platform']} / {group['status']}: "
            f"count={group['count']} examples={examples}"
        )
    lines.append("Findings:")
    for finding in report["findings"]:
        lines.append(
            f"  - attempt_id={finding['attempt_id']} platform={finding['platform']} "
            f"status={finding['status']} age_hours={finding['age_hours']} "
            f"content_id={finding['content_id'] or '-'} queue_id={finding['queue_id'] or '-'}"
        )
    return "\n".join(lines)


def _load_attempt_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    pa = schema["publication_attempts"]
    pq = schema.get("publish_queue", set())
    timestamp_expr = _column_expr(pa, *TIMESTAMP_COLUMNS, fallback="NULL", alias="pa")
    terminal_success = _column_expr(pa, *SUCCESS_TIMESTAMP_COLUMNS, fallback="NULL", alias="pa")
    terminal_failure = _column_expr(pa, *FAILURE_TIMESTAMP_COLUMNS, fallback="NULL", alias="pa")
    queue_id = _column_expr(pa, "queue_id", fallback="NULL", alias="pa")
    content_id = _column_expr(pa, "content_id", fallback="NULL", alias="pa")
    queue_content = _column_expr(pq, "content_id", fallback="NULL", alias="pq")
    queue_status = _column_expr(pq, "status", fallback="NULL", alias="pq")
    queue_join = (
        "LEFT JOIN publish_queue pq ON pq.id = pa.queue_id"
        if "publish_queue" in schema and {"id", "queue_id"} <= pa | pq
        and "queue_id" in pa and "id" in pq
        else ""
    )
    rows = conn.execute(
        f"""SELECT
               pa.id AS attempt_id,
               pa.platform AS platform,
               pa.status AS status,
               {timestamp_expr} AS started_at,
               {terminal_success} AS terminal_success_at,
               {terminal_failure} AS terminal_failure_at,
               {queue_id} AS queue_id,
               {content_id} AS content_id,
               {queue_content} AS queue_content_id,
               {queue_status} AS queue_status
           FROM publication_attempts pa
           {queue_join}
           WHERE datetime({timestamp_expr}) < datetime(?)
           ORDER BY datetime({timestamp_expr}) ASC, pa.id ASC""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _is_timeout_risk(row: dict[str, Any], cutoff: datetime) -> bool:
    started_at = _parse_timestamp(row.get("started_at"))
    if started_at is None or started_at >= cutoff:
        return False
    if _parse_timestamp(row.get("terminal_success_at")) or _parse_timestamp(row.get("terminal_failure_at")):
        return False
    status = _status(row.get("status"))
    return bool(status) and status not in TERMINAL_STATUSES


def _finding(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    started_at = _parse_timestamp(row.get("started_at"))
    age_hours = round((now - started_at).total_seconds() / 3600, 2) if started_at else None
    return {
        "attempt_id": row.get("attempt_id"),
        "platform": _text(row.get("platform")) or "unknown",
        "status": _status(row.get("status")) or "unknown",
        "age_hours": age_hours,
        "started_at": started_at.isoformat() if started_at else None,
        "content_id": row.get("content_id"),
        "queue_id": row.get("queue_id"),
        "queue_content_id": row.get("queue_content_id"),
        "queue_status": row.get("queue_status"),
    }


def _missing_attempt_columns(columns: set[str]) -> list[str]:
    missing = [column for column in ("id", "platform", "status") if column not in columns]
    if not set(TIMESTAMP_COLUMNS) & columns:
        missing.append("started_at|created_at|attempted_at")
    return sorted(missing)


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str, alias: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return f"{alias}.{column}"
    return fallback


def _parse_timestamp(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _status(value: Any) -> str:
    text = _text(value).lower()
    return NON_TERMINAL_STATUS_ALIASES.get(text, text)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        -float(finding["age_hours"] or 0),
        finding["platform"],
        finding["status"],
        _int_or_text(finding["attempt_id"]),
    )


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
