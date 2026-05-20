"""Report newsletter sends with suspicious retry behavior."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_RETRY_THRESHOLD = 3
DEFAULT_LIMIT = 100
EXPECTED_RETRY_WINDOW_HOURS = 24.0
REQUIRED_COLUMNS = {"id", "status"}
ISSUE_COLUMNS = ("issue_id", "newsletter_issue_id", "issue_identifier")
RECIPIENT_COLUMNS = ("subscriber_id", "segment_id", "recipient_id", "recipient_email")
ATTEMPT_COLUMNS = ("attempted_at", "created_at", "sent_at", "updated_at")
ERROR_COLUMNS = ("error", "error_message", "last_error")
FAILED_STATUSES = {"failed", "failure", "error", "errored", "bounced", "retry_failed"}


def build_newsletter_send_retry_anomalies_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    retry_threshold: int = DEFAULT_RETRY_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if retry_threshold <= 0:
        raise ValueError("retry_threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    buckets: dict[tuple[Any, Any], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        attempt_at = _parse_timestamp(row.get("attempted_at"))
        if attempt_at is None or attempt_at < cutoff:
            continue
        key = (row.get("issue_id") or row.get("newsletter_issue_id"), row.get("recipient_id") or row.get("segment_id") or row.get("subscriber_id"))
        if key[0] is None:
            key = (row.get("issue_identifier") or row.get("issue_id"), key[1])
        buckets[key].append(row)

    findings = [
        _finding(key, attempts, retry_threshold)
        for key, attempts in buckets.items()
        if _is_anomalous(attempts, retry_threshold)
    ]
    findings.sort(key=lambda item: (-len(item["gap_reasons"]), -item["retry_count"], str(item["newsletter_issue_id"]), str(item["recipient_identifier"])))
    shown = findings[:limit]
    return {
        "artifact_type": "newsletter_send_retry_anomalies",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "retry_threshold": retry_threshold, "limit": limit},
        "summary": {
            "attempt_count": len(rows),
            "bucket_count": len(buckets),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_latest_status": dict(sorted(Counter(f["latest_status"] for f in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items()) if cols},
        "empty_state": {"is_empty": not findings, "message": "No newsletter retry anomalies found." if not findings else None},
    }


def build_newsletter_send_retry_anomalies_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = _first_table(schema, ("newsletter_send_attempts", "newsletter_sends", "newsletter_deliveries"))
    if table is None:
        return build_newsletter_send_retry_anomalies_report([], missing_tables=["newsletter_send_attempts|newsletter_sends"], **kwargs)
    columns = schema[table]
    missing = _missing_columns(columns)
    if missing:
        return build_newsletter_send_retry_anomalies_report([], missing_columns={table: missing}, **kwargs)
    days = int(kwargs.get("days", DEFAULT_DAYS))
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    return build_newsletter_send_retry_anomalies_report(
        _load_rows(conn, table, columns, now - timedelta(days=days)),
        **kwargs,
    )


def format_newsletter_send_retry_anomalies_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_send_retry_anomalies_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = [
        "Newsletter Send Retry Anomalies",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} retry_threshold={report['filters']['retry_threshold']} limit={report['filters']['limit']}",
        f"Totals: attempts={s['attempt_count']} buckets={s['bucket_count']} findings={s['finding_count']} shown={s['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for f in report["findings"]:
        lines.append(
            f"  - issue={f['newsletter_issue_id']} recipient={f['recipient_identifier'] or '-'} "
            f"retries={f['retry_count']} status={f['latest_status']} first={f['first_attempt_at']} latest={f['latest_attempt_at']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], cutoff: datetime) -> list[dict[str, Any]]:
    aliases = [
        ("id", ("id",)),
        ("issue_id", ISSUE_COLUMNS),
        ("recipient_id", RECIPIENT_COLUMNS),
        ("status", ("status",)),
        ("attempted_at", ATTEMPT_COLUMNS),
        ("error", ERROR_COLUMNS),
    ]
    select = ", ".join(f"{_coalesce(columns, names)} AS {alias}" for alias, names in aliases)
    rows = conn.execute(
        f"SELECT {select} FROM {table} WHERE datetime({_coalesce(columns, ATTEMPT_COLUMNS)}) >= datetime(?) ORDER BY datetime({_coalesce(columns, ATTEMPT_COLUMNS)}) ASC, id ASC",
        (cutoff.isoformat(),),
    ).fetchall()
    return [dict(row) for row in rows]


def _is_anomalous(attempts: list[dict[str, Any]], retry_threshold: int) -> bool:
    failed = sum(_normal_status(row.get("status")) in FAILED_STATUSES for row in attempts)
    first, latest = _bounds(attempts)
    return len(attempts) > 1 and (
        failed > 1
        or len(attempts) >= retry_threshold
        or (first and latest and (latest - first).total_seconds() / 3600 > EXPECTED_RETRY_WINDOW_HOURS)
    )


def _finding(key: tuple[Any, Any], attempts: list[dict[str, Any]], retry_threshold: int) -> dict[str, Any]:
    ordered = sorted(attempts, key=lambda row: (_parse_timestamp(row.get("attempted_at")) or datetime.min.replace(tzinfo=timezone.utc), _int_or_text(row.get("id"))))
    first, latest = _bounds(ordered)
    failed = sum(_normal_status(row.get("status")) in FAILED_STATUSES for row in ordered)
    reasons = []
    if failed > 1:
        reasons.append("repeated_failed_attempts")
    if len(ordered) >= retry_threshold:
        reasons.append("retry_count_above_threshold")
    if first and latest and (latest - first).total_seconds() / 3600 > EXPECTED_RETRY_WINDOW_HOURS:
        reasons.append("retry_window_exceeds_expected_cadence")
    latest_row = ordered[-1]
    return {
        "newsletter_issue_id": key[0],
        "recipient_identifier": key[1],
        "retry_count": len(ordered),
        "failed_retry_count": failed,
        "latest_status": _normal_status(latest_row.get("status")),
        "latest_error": latest_row.get("error"),
        "first_attempt_at": first.isoformat() if first else None,
        "latest_attempt_at": latest.isoformat() if latest else None,
        "retry_window_hours": round((latest - first).total_seconds() / 3600, 2) if first and latest else None,
        "gap_reasons": reasons,
        "attempt_ids": [row.get("id") for row in ordered],
    }


def _bounds(rows: list[dict[str, Any]]) -> tuple[datetime | None, datetime | None]:
    stamps = [stamp for stamp in (_parse_timestamp(row.get("attempted_at")) for row in rows) if stamp]
    return (min(stamps), max(stamps)) if stamps else (None, None)


def _missing_columns(columns: set[str]) -> list[str]:
    missing = sorted(REQUIRED_COLUMNS - columns)
    if not set(ISSUE_COLUMNS) & columns:
        missing.append("|".join(ISSUE_COLUMNS))
    if not set(ATTEMPT_COLUMNS) & columns:
        missing.append("|".join(ATTEMPT_COLUMNS))
    return missing


def _first_table(schema: dict[str, set[str]], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in schema), None)


def _coalesce(columns: set[str], names: tuple[str, ...]) -> str:
    available = [name for name in names if name in columns]
    return "COALESCE(" + ", ".join(available) + ")" if len(available) > 1 else (available[0] if available else "NULL")


def _normal_status(value: Any) -> str:
    text = "" if value is None else str(value).strip().lower()
    return text or "unknown"


def _parse_timestamp(value: Any) -> datetime | None:
    text = "" if value is None else str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


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
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
