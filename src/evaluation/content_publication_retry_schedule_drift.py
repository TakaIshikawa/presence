"""Report retry schedule inconsistencies in content_publications."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_OVERDUE_HOURS = 24
DEFAULT_LIMIT = 100
DEFAULT_PLATFORM = "all"
DEFAULT_STATUS = "all"
PUBLISHED_STATUSES = {"published", "success", "succeeded", "sent"}
FAILED_STATUSES = {"failed", "error", "retry", "retrying"}
ISSUE_TYPES = ("missing_retry", "overdue_retry", "stale_retry_on_published", "inverted_error_retry")


def build_content_publication_retry_schedule_drift_report(
    rows: list[dict[str, Any]],
    *,
    overdue_hours: int = DEFAULT_OVERDUE_HOURS,
    platform: str = DEFAULT_PLATFORM,
    status: str = DEFAULT_STATUS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if overdue_hours < 0:
        raise ValueError("overdue_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    overdue_before = generated_at - timedelta(hours=overdue_hours)
    platform_filter = _clean(platform).lower() or DEFAULT_PLATFORM
    status_filter = _clean(status).lower() or DEFAULT_STATUS
    drift_items: list[dict[str, Any]] = []
    scanned = 0

    for row in rows:
        row_platform = _clean(row.get("platform"), "unknown").lower()
        row_status = _clean(row.get("status"), "unknown").lower()
        if platform_filter != "all" and row_platform != platform_filter:
            continue
        if status_filter != "all" and row_status != status_filter:
            continue
        scanned += 1
        issues = _issue_types(row, overdue_before=overdue_before)
        if not issues:
            continue
        next_retry_at = _parse_dt(row.get("next_retry_at"))
        last_error_at = _parse_dt(row.get("last_error_at"))
        retry_age_hours = round((generated_at - next_retry_at).total_seconds() / 3600, 2) if next_retry_at else None
        for issue_type in issues:
            drift_items.append(
                {
                    "publication_id": _int_or_none(row.get("publication_id") or row.get("id")),
                    "content_id": _int_or_none(row.get("content_id")),
                    "platform": row_platform,
                    "status": row_status,
                    "attempt_count": _int_or_none(row.get("attempt_count")) or 0,
                    "error_category": _clean(row.get("error_category"), "unknown").lower(),
                    "issue_type": issue_type,
                    "next_retry_at": next_retry_at.isoformat() if next_retry_at else None,
                    "last_error_at": last_error_at.isoformat() if last_error_at else None,
                    "retry_age_hours": retry_age_hours,
                }
            )

    drift_items.sort(key=_sort_key)
    shown = drift_items[:limit]
    return {
        "artifact_type": "content_publication_retry_schedule_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "overdue_hours": overdue_hours,
            "overdue_before": overdue_before.isoformat(),
            "platform": platform_filter,
            "status": status_filter,
            "limit": limit,
        },
        "drift_items": shown,
        "grouped_counts": _grouped_counts(drift_items),
        "summary": {
            "rows_scanned": scanned,
            "drift_count": len(drift_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in drift_items).items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_content_publication_retry_schedule_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, schema, missing_tables, missing_columns)
    return build_content_publication_retry_schedule_drift_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_content_publication_retry_schedule_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_publication_retry_schedule_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    thresholds = report["thresholds"]
    lines = [
        "Content Publication Retry Schedule Drift",
        f"Generated: {report['generated_at']}",
        f"Overdue: {thresholds['overdue_hours']} hours",
        f"Platform: {thresholds['platform']}",
        f"Status: {thresholds['status']}",
        f"Totals: scanned={summary['rows_scanned']} drift={summary['drift_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["drift_items"]:
        lines.append("No content publication retry schedule drift found.")
        return "\n".join(lines)
    lines.extend(["", "platform | error_category | issue_type | count"])
    for group in report["grouped_counts"]:
        lines.append(f"{group['platform']} | {group['error_category']} | {group['issue_type']} | {group['count']}")
    lines.extend(["", "publication_id | content_id | platform | status | issue | next_retry_at | retry_age_hours"])
    for item in report["drift_items"]:
        lines.append(
            f"{item['publication_id'] or '-'} | {item['content_id'] or '-'} | {item['platform']} | "
            f"{item['status']} | {item['issue_type']} | {item['next_retry_at'] or '-'} | {item['retry_age_hours']}"
        )
    return "\n".join(lines)


def _issue_types(row: dict[str, Any], *, overdue_before: datetime) -> list[str]:
    status = _clean(row.get("status")).lower()
    attempts = _int_or_none(row.get("attempt_count")) or 0
    next_retry_at = _parse_dt(row.get("next_retry_at"))
    last_error_at = _parse_dt(row.get("last_error_at"))
    issues: list[str] = []
    if status in FAILED_STATUSES and attempts > 0 and next_retry_at is None:
        issues.append("missing_retry")
    if status in FAILED_STATUSES and next_retry_at is not None and next_retry_at < overdue_before:
        issues.append("overdue_retry")
    if status in PUBLISHED_STATUSES and next_retry_at is not None:
        issues.append("stale_retry_on_published")
    if last_error_at is not None and next_retry_at is not None and last_error_at > next_retry_at:
        issues.append("inverted_error_retry")
    return issues


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "content_publications" not in schema:
        missing_tables.append("content_publications")
        return []
    cols = schema["content_publications"]
    if "id" not in cols:
        missing_columns["content_publications"] = ["id"]
        return []
    select = [
        "id AS publication_id",
        _expr(cols, "content_id", "content_id", "NULL"),
        _expr(cols, "platform", "platform", "'unknown'"),
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "attempt_count", "attempt_count", "0"),
        _expr(cols, "next_retry_at", "next_retry_at", "NULL"),
        _expr(cols, "last_error_at", "last_error_at", "NULL"),
        _expr(cols, "error_category", "error_category", "'unknown'"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM content_publications ORDER BY rowid ASC")]


def _grouped_counts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["platform"], item["error_category"], item["issue_type"]) for item in items)
    return [
        {"platform": platform, "error_category": category, "issue_type": issue, "count": count}
        for (platform, category, issue), count in sorted(counts.items())
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    age = item["retry_age_hours"]
    return (item["platform"], item["error_category"], item["issue_type"], -(age if age is not None else -1), item["publication_id"] or 0)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


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


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
