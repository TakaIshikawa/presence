"""Report campaign planned topics at risk of missing target dates."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WARNING_DAYS = 3
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "campaign_topic_deadline_risk"
RISK_REASONS = (
    "topic_outside_campaign_dates",
    "planned_due_soon_missing_content",
    "generated_overdue_unpublished",
)


def build_campaign_topic_deadline_risk_report(
    rows: list[dict[str, Any]],
    *,
    warning_days: int = DEFAULT_WARNING_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic deadline risk report from campaign/topic rows."""
    if warning_days < 0:
        raise ValueError("warning_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    today = generated_at.date()
    warning_end = today + timedelta(days=warning_days)
    findings: list[dict[str, Any]] = []

    for row in rows:
        target = _parse_date(row.get("target_date"))
        if target is None:
            continue
        status = _clean(row.get("status") or row.get("topic_status"), "planned").lower()
        content_id = _int_or_none(row.get("content_id"))
        days_until = (target - today).days
        start = _parse_date(row.get("campaign_start_date") or row.get("start_date"))
        end = _parse_date(row.get("campaign_end_date") or row.get("end_date"))

        if (start and target < start) or (end and target > end):
            findings.append(_finding(row, target, status, "topic_outside_campaign_dates", days_until, content_id))
        if status == "planned" and content_id is None and today <= target <= warning_end:
            findings.append(_finding(row, target, status, "planned_due_soon_missing_content", days_until, content_id))
        if content_id is not None and target < today and not _is_published(row):
            findings.append(_finding(row, target, status, "generated_overdue_unpublished", days_until, content_id))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "warning_days": warning_days,
            "limit": limit,
            "warning_start": today.isoformat(),
            "warning_end": warning_end.isoformat(),
        },
        "summary": {
            "topic_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_risk_reason": _counts_by_reason(findings),
            "by_campaign": _counts_by_campaign(findings),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No campaign topic deadline risks found." if not findings else None,
        },
    }


def build_campaign_topic_deadline_risk_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load campaign topic rows from SQLite and build the deadline risk report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {
        "content_campaigns": {"id"},
        "planned_topics": {"id", "campaign_id", "target_date"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return build_campaign_topic_deadline_risk_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    return build_campaign_topic_deadline_risk_report(_load_rows(conn, schema), **kwargs)


def format_campaign_topic_deadline_risk_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_campaign_topic_deadline_risk_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Campaign Topic Deadline Risk",
        f"Generated: {report['generated_at']}",
        f"Filters: warning_days={filters['warning_days']} limit={filters['limit']}",
        f"Totals: topics={summary['topic_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "campaign_id | topic_id | target_date | status | risk_reason | days_until_target | content_id"])
    for item in report["findings"]:
        lines.append(
            f"{item['campaign_id']} | {item['topic_id']} | {item['target_date']} | {item['status']} | "
            f"{item['risk_reason']} | {item['days_until_target']} | {_display(item['content_id'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cc = schema["content_campaigns"]
    pt = schema["planned_topics"]
    gc = schema.get("generated_content", set())
    cp = schema.get("content_publications", set())

    content_expr = _content_id_expr(pt, gc)
    select = [
        "cc.id AS campaign_id",
        _expr(cc, "name", "cc", "campaign_name", "NULL"),
        _expr(cc, "start_date", "cc", "campaign_start_date", "NULL"),
        _expr(cc, "end_date", "cc", "campaign_end_date", "NULL"),
        "pt.id AS topic_id",
        _expr(pt, "target_date", "pt", "target_date", "NULL"),
        _expr(pt, "status", "pt", "status", "'planned'"),
        f"{content_expr} AS content_id",
        _expr(gc, "published", "gc", "generated_published", "0"),
        _expr(gc, "published_at", "gc", "generated_published_at", "NULL"),
        "MAX(CASE WHEN cp.status = 'published' THEN 1 ELSE 0 END) AS publication_published",
    ]
    joins = []
    if "generated_content" in schema and "id" in gc and "content_id" in pt:
        joins.append("LEFT JOIN generated_content gc ON gc.id = pt.content_id")
    elif "generated_content" in schema and "planned_topic_id" in gc:
        joins.append("LEFT JOIN generated_content gc ON gc.planned_topic_id = pt.id")
    elif "generated_content" in schema and "topic_id" in gc:
        joins.append("LEFT JOIN generated_content gc ON gc.topic_id = pt.id")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS id, 0 AS published, NULL AS published_at) gc ON 0")
    if "content_publications" in schema and "content_id" in cp:
        joins.append(f"LEFT JOIN content_publications cp ON cp.content_id = {content_expr}")
    else:
        joins.append("LEFT JOIN (SELECT NULL AS content_id, NULL AS status) cp ON 0")

    query = f"""SELECT {', '.join(select)}
                FROM content_campaigns cc
                JOIN planned_topics pt ON pt.campaign_id = cc.id
                {' '.join(joins)}
                GROUP BY cc.rowid, pt.rowid
                ORDER BY cc.id ASC, pt.target_date ASC, pt.id ASC"""
    return [dict(row) for row in conn.execute(query).fetchall()]


def _content_id_expr(pt: set[str], gc: set[str]) -> str:
    if "content_id" in pt and "id" in gc:
        return "COALESCE(pt.content_id, gc.id)"
    if "content_id" in pt:
        return "pt.content_id"
    if "id" in gc:
        return "gc.id"
    return "NULL"


def _finding(row: dict[str, Any], target: date, status: str, reason: str, days_until: int, content_id: int | None) -> dict[str, Any]:
    return {
        "campaign_id": _int_or_none(row.get("campaign_id")),
        "campaign_name": _clean(row.get("campaign_name")) or None,
        "topic_id": _int_or_none(row.get("topic_id")),
        "target_date": target.isoformat(),
        "status": status,
        "risk_reason": reason,
        "days_until_target": days_until,
        "content_id": content_id,
    }


def _is_published(row: dict[str, Any]) -> bool:
    return _bool(row.get("generated_published")) or _bool(row.get("publication_published")) or bool(_clean(row.get("generated_published_at")))


def _counts_by_reason(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["risk_reason"] for item in findings)
    return {reason: counts[reason] for reason in RISK_REASONS}


def _counts_by_campaign(findings: list[dict[str, Any]]) -> dict[str, int]:
    return dict(sorted(Counter(str(item["campaign_id"]) for item in findings).items(), key=lambda item: int(item[0])))


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (RISK_REASONS.index(item["risk_reason"]), item["campaign_id"] or 0, item["target_date"], item["topic_id"] or 0)


def _expr(columns: set[str], column: str, alias: str, output: str, default: str) -> str:
    return f"{alias}.{_quote_identifier(column)} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "published"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
