"""Report publish queue schedule and state integrity problems."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_GRACE_HOURS = 2
DEFAULT_LIMIT = 100


def build_publish_queue_schedule_integrity_report(
    rows: list[dict[str, Any]],
    *,
    grace_hours: int = DEFAULT_GRACE_HOURS,
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
    overdue_before = generated_at - timedelta(hours=grace_hours)
    findings: list[dict[str, Any]] = []

    for row in rows:
        status = _clean(row.get("status"), "unknown").lower()
        platform = _clean(row.get("platform"), "unknown").lower()
        scheduled_at = _parse_dt(row.get("scheduled_at"))
        if status == "queued" and scheduled_at and scheduled_at < overdue_before:
            findings.append(_item(row, "queued_scheduled_in_past", generated_at))
        if status == "published" and not _parse_dt(row.get("published_at")):
            findings.append(_item(row, "published_missing_published_at", generated_at))
        if status == "failed" and not (_clean(row.get("error")) or _clean(row.get("error_category"))):
            findings.append(_item(row, "failed_missing_error", generated_at))
        if status == "held" and not _clean(row.get("hold_reason")):
            findings.append(_item(row, "held_missing_hold_reason", generated_at))
        content_status = _clean(row.get("generated_content_status")).lower()
        if status == "queued" and content_status in {"abandoned", "published"}:
            findings.append(_item(row, f"queued_content_{content_status}", generated_at))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "publish_queue_schedule_integrity",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"grace_hours": grace_hours, "overdue_before": overdue_before.isoformat(), "limit": limit},
        "summary": {
            "rows_scanned": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in findings).items())),
            "by_platform": dict(sorted(Counter(item["platform"] for item in findings).items())),
        },
        "groups": _groups(findings),
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_publish_queue_schedule_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("generated_content", "publish_queue") if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if "publish_queue" in schema:
        rows = _load_rows(conn, schema, missing_columns)
    return build_publish_queue_schedule_integrity_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_publish_queue_schedule_integrity_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_schedule_integrity_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publish Queue Schedule Integrity",
        f"Generated: {report['generated_at']}",
        f"Grace: {report['thresholds']['grace_hours']} hours",
        f"Totals: scanned={summary['rows_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["findings"]:
        lines.append("No publish queue schedule integrity issues found.")
        return "\n".join(lines)
    lines.extend(["", "issue_type | platform | count"])
    for group in report["groups"]:
        lines.append(f"{group['issue_type']} | {group['platform']} | {group['count']}")
    lines.extend(["", "queue_id | content_id | platform | status | issue_type | age_hours"])
    for item in report["findings"]:
        lines.append(
            f"{item['queue_id'] or '-'} | {item['content_id'] or '-'} | {item['platform']} | "
            f"{item['status']} | {item['issue_type']} | {item['age_hours'] if item['age_hours'] is not None else '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    pq = schema["publish_queue"]
    if "id" not in pq:
        missing_columns["publish_queue"] = ["id"]
        return []
    gc = schema.get("generated_content", set())
    can_join = "generated_content" in schema and "content_id" in pq and "id" in gc
    if "generated_content" in schema and "id" not in gc:
        missing_columns["generated_content"] = ["id"]
    join = "LEFT JOIN generated_content gc ON gc.id = pq.content_id" if can_join else ""
    select = [
        "pq.id AS queue_id",
        _expr(pq, "content_id", "pq", "content_id", "NULL"),
        _expr(pq, "platform", "pq", "platform", "'unknown'"),
        _expr(pq, "status", "pq", "status", "'unknown'"),
        _expr(pq, "scheduled_at", "pq", "scheduled_at", "NULL"),
        _expr(pq, "published_at", "pq", "published_at", "NULL"),
        _expr(pq, "error", "pq", "error", "NULL"),
        _expr(pq, "error_category", "pq", "error_category", "NULL"),
        _expr(pq, "hold_reason", "pq", "hold_reason", "NULL"),
        _expr(gc, "status", "gc", "generated_content_status", "NULL") if can_join else "NULL AS generated_content_status",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM publish_queue pq {join} ORDER BY pq.rowid ASC")]


def _item(row: dict[str, Any], issue_type: str, now: datetime) -> dict[str, Any]:
    scheduled_at = _parse_dt(row.get("scheduled_at"))
    age_hours = round((now - scheduled_at).total_seconds() / 3600, 2) if scheduled_at else None
    return {
        "queue_id": _int_or_none(row.get("queue_id") or row.get("id")),
        "content_id": _int_or_none(row.get("content_id")),
        "platform": _clean(row.get("platform"), "unknown").lower(),
        "status": _clean(row.get("status"), "unknown").lower(),
        "issue_type": issue_type,
        "scheduled_at": scheduled_at.isoformat() if scheduled_at else None,
        "published_at": _iso_or_none(row.get("published_at")),
        "generated_content_status": _clean(row.get("generated_content_status")) or None,
        "age_hours": age_hours,
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["issue_type"], item["platform"]) for item in items)
    return [
        {"issue_type": issue_type, "platform": platform, "count": count}
        for (issue_type, platform), count in sorted(counts.items())
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["issue_type"], item["platform"], item["queue_id"] or 0)


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


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


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_dt(value)
    return parsed.isoformat() if parsed else (_clean(value) or None)


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
