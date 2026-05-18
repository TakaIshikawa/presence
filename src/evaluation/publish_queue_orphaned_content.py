"""Audit publish queue rows that cannot safely publish."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LIMIT = 100
DEFAULT_STATUS = "queued,held"
DEFAULT_PLATFORM = "all"
ISSUE_TYPES = (
    "missing_generated_content",
    "abandoned_generated_content",
    "already_published_queued",
    "malformed_scheduled_at",
)


def build_publish_queue_orphaned_content_report(
    queue_rows: list[dict[str, Any]],
    *,
    status: str | Iterable[str] = DEFAULT_STATUS,
    platform: str = DEFAULT_PLATFORM,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    statuses = _normalize_status_filter(status)
    normalized_platform = _clean(platform).lower() or DEFAULT_PLATFORM
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    scanned = 0

    for row in queue_rows:
        row_status = _clean(row.get("queue_status") or row.get("status")).lower() or "queued"
        row_platform = _clean(row.get("platform")).lower() or "unknown"
        if statuses and "all" not in statuses and row_status not in statuses:
            continue
        if normalized_platform != "all" and row_platform != normalized_platform:
            continue
        scanned += 1
        for issue_type in _issues_for_row(row, row_status):
            finding = {
                "queue_id": _first(row, "queue_id", "id"),
                "content_id": row.get("content_id"),
                "platform": row_platform,
                "queue_status": row_status,
                "scheduled_at": row.get("scheduled_at"),
                "content_status": row.get("content_status"),
                "content_published": row.get("content_published"),
                "issue_type": issue_type,
            }
            findings.append(finding)
            counts[issue_type] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "publish_queue_orphaned_content",
        "generated_at": generated_at.isoformat(),
        "filters": {"status": list(statuses), "platform": normalized_platform, "limit": limit},
        "summary": {
            "queue_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No publish queue orphaned content issues found." if not findings else None,
        },
    }


def build_publish_queue_orphaned_content_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"publish_queue", "generated_content"}
    missing = sorted(required - set(schema))
    if missing:
        return build_publish_queue_orphaned_content_report([], missing_tables=missing, **kwargs)
    return build_publish_queue_orphaned_content_report(_load_queue_rows(conn, schema), **kwargs)


def format_publish_queue_orphaned_content_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_orphaned_content_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publish Queue Orphaned Content",
        f"Generated: {report['generated_at']}",
        f"Status: {', '.join(report['filters']['status'])}",
        f"Platform: {report['filters']['platform']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: queue_rows={summary['queue_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "queue_id | scheduled_at | platform | status | content_id | issue_type"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['queue_id'] or '-'} | {finding['scheduled_at'] or '-'} | {finding['platform']} | "
            f"{finding['queue_status']} | {finding['content_id'] or '-'} | {finding['issue_type']}"
        )
    return "\n".join(lines)


def _issues_for_row(row: dict[str, Any], queue_status: str) -> list[str]:
    issues: list[str] = []
    content_exists = row.get("resolved_content_id") is not None
    if not content_exists:
        issues.append("missing_generated_content")
    else:
        content_status = _clean(row.get("content_status")).lower()
        published = _int(row.get("content_published"))
        if published == -1 or content_status in {"abandoned", "rejected"}:
            issues.append("abandoned_generated_content")
        if queue_status == "queued" and (published == 1 or content_status == "published"):
            issues.append("already_published_queued")
    if row.get("scheduled_at") not in (None, "") and _parse_datetime(row.get("scheduled_at")) is None:
        issues.append("malformed_scheduled_at")
    return issues


def _load_queue_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pq_cols = schema["publish_queue"]
    gc_cols = schema["generated_content"]
    if not {"id", "content_id"}.issubset(pq_cols):
        return []
    rows = conn.execute(
        f"""SELECT
               pq.id AS queue_id,
               pq.content_id AS content_id,
               gc.id AS resolved_content_id,
               {_column_expr(pq_cols, "status", fallback="'queued'", alias="pq")} AS queue_status,
               {_column_expr(pq_cols, "platform", "target_platform", fallback="'unknown'", alias="pq")} AS platform,
               {_column_expr(pq_cols, "scheduled_at", fallback="NULL", alias="pq")} AS scheduled_at,
               {_column_expr(gc_cols, "status", fallback="NULL", alias="gc")} AS content_status,
               {_column_expr(gc_cols, "published", fallback="NULL", alias="gc")} AS content_published
           FROM publish_queue pq
           LEFT JOIN generated_content gc ON gc.id = pq.content_id
           ORDER BY {_column_expr(pq_cols, "scheduled_at", fallback="pq.id", alias="pq")} ASC, pq.id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str, alias: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return f"{alias}.{column}"
    return fallback


def _normalize_status_filter(status: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(status, str):
        values = status.split(",")
    else:
        values = list(status)
    normalized = tuple(value for value in (_clean(item).lower() for item in values) if value)
    return normalized or tuple(DEFAULT_STATUS.split(","))


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    parsed = _parse_datetime(finding.get("scheduled_at"))
    return (
        parsed or datetime.max.replace(tzinfo=timezone.utc),
        _int_or_text(finding.get("queue_id")),
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
    )


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
