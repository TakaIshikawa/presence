"""Report planned topic source material and lifecycle integrity issues."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "planned_topic_source_material_integrity"
TABLE = "planned_topics"
REQUIRED_COLUMNS = {"id", "campaign_id", "topic", "status", "target_date", "source_material", "content_id"}
ISSUE_TYPES = (
    "malformed_source_material_json",
    "generated_missing_content_id",
    "planned_with_content_id",
    "skipped_with_content_id",
    "invalid_target_date",
)


def build_planned_topic_source_material_integrity_report(
    rows: list[dict[str, Any]],
    *,
    status: str | None = None,
    campaign_id: int | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic integrity report over planned topic rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    issues: list[dict[str, Any]] = []
    for row in rows:
        normalized = _normalize_row(row)
        issues.extend(_issues_for_row(normalized))

    issues.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), _date_sort(item.get("target_date")), _int_sort(item.get("planned_topic_id"))))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"status": status, "campaign_id": campaign_id, "days": days, "limit": limit},
        "summary": {
            "planned_topic_count": len(rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": _counts(issues),
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not issues,
            "message": "No planned topic source material integrity issues found." if not issues else None,
        },
    }


def build_planned_topic_source_material_integrity_report_from_db(
    db_or_conn: Any,
    *,
    status: str | None = None,
    campaign_id: int | None = None,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load planned topics from SQLite and build an integrity report."""
    if days <= 0:
        raise ValueError("days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if TABLE in schema else [TABLE]
    missing_columns = {
        TABLE: sorted(REQUIRED_COLUMNS - schema.get(TABLE, set()))
    } if TABLE in schema and REQUIRED_COLUMNS - schema.get(TABLE, set()) else {}
    if missing_tables or missing_columns:
        return build_planned_topic_source_material_integrity_report(
            [],
            status=status,
            campaign_id=campaign_id,
            days=days,
            limit=limit,
            now=generated_at,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )
    rows = _load_rows(conn, schema[TABLE], status=status, campaign_id=campaign_id, cutoff=generated_at - timedelta(days=days), limit=limit)
    return build_planned_topic_source_material_integrity_report(rows, status=status, campaign_id=campaign_id, days=days, limit=limit, now=generated_at)


def format_planned_topic_source_material_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_source_material_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Planned Topic Source Material Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: status={report['filters']['status'] or '-'} campaign_id={report['filters']['campaign_id'] or '-'} days={report['filters']['days']} limit={report['filters']['limit']}",
        f"Totals: planned_topics={summary['planned_topic_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not any(report["findings"].values()):
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Findings:")
    for issue_type in ISSUE_TYPES:
        for item in report["findings"].get(issue_type, []):
            lines.append(
                f"  - {issue_type} topic={item['planned_topic_id']} campaign={item['campaign_id'] or '-'} "
                f"status={item['status'] or '-'} target={item['target_date'] or '-'}"
            )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    status: str | None,
    campaign_id: int | None,
    cutoff: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    where: list[str] = []
    params: list[Any] = []
    if status is not None:
        where.append("pt.status = ?")
        params.append(status)
    if campaign_id is not None:
        where.append("pt.campaign_id = ?")
        params.append(campaign_id)
    window_filter, window_params = _window_filter(columns, cutoff)
    where.append(window_filter)
    params.extend(window_params)
    select = [
        "pt.id AS planned_topic_id",
        "pt.campaign_id",
        "pt.topic",
        "pt.status",
        "pt.target_date",
        "pt.source_material",
        "pt.content_id",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT {', '.join(select)}
               FROM planned_topics pt
               WHERE {' AND '.join(where)}
               ORDER BY {_order_by(columns)}
               LIMIT ?""",
            (*params, limit),
        ).fetchall()
    ]


def _issues_for_row(row: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    source_issue = _source_material_issue(row)
    if source_issue:
        issues.append(source_issue)
    status = _clean(row.get("status")).lower()
    has_content = _has_value(row.get("content_id"))
    if status == "generated" and not has_content:
        issues.append(_issue("generated_missing_content_id", row))
    if status == "planned" and has_content:
        issues.append(_issue("planned_with_content_id", row))
    if status == "skipped" and has_content:
        issues.append(_issue("skipped_with_content_id", row))
    target_date = _clean(row.get("target_date"))
    if target_date and _parse_date(target_date) is None:
        issues.append(_issue("invalid_target_date", row))
    return issues


def _source_material_issue(row: dict[str, Any]) -> dict[str, Any] | None:
    source_material = _clean(row.get("source_material"))
    if not source_material or source_material[0] not in "[{":
        return None
    try:
        json.loads(source_material)
    except json.JSONDecodeError as exc:
        return _issue("malformed_source_material_json", row, diagnostic=exc.msg)
    return None


def _issue(issue_type: str, row: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "planned_topic_id": row.get("planned_topic_id"),
        "campaign_id": row.get("campaign_id"),
        "topic": row.get("topic"),
        "status": row.get("status"),
        "target_date": row.get("target_date"),
        "content_id": row.get("content_id"),
        **extra,
    }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "planned_topic_id": _int_or_none(row.get("planned_topic_id", row.get("id"))),
        "campaign_id": _int_or_none(row.get("campaign_id")),
        "topic": row.get("topic"),
        "status": row.get("status"),
        "target_date": row.get("target_date"),
        "source_material": row.get("source_material"),
        "content_id": _int_or_none(row.get("content_id")),
    }


def _group_findings(issues: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {issue_type: [] for issue_type in ISSUE_TYPES}
    for issue in issues:
        grouped[issue["issue_type"]].append(issue)
    return grouped


def _counts(issues: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(issue["issue_type"] for issue in issues)
    return {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES}


def _window_filter(columns: set[str], cutoff: datetime) -> tuple[str, list[Any]]:
    filters: list[str] = []
    params: list[Any] = []
    if "created_at" in columns:
        filters.append("datetime(pt.created_at) >= datetime(?)")
        params.append(cutoff.isoformat())
    if "target_date" in columns:
        filters.append("(pt.target_date IS NULL OR date(pt.target_date) >= date(?))")
        params.append(cutoff.date().isoformat())
    if not filters:
        return "1", []
    return "(" + " OR ".join(filters) + ")", params


def _order_by(columns: set[str]) -> str:
    order = []
    if "target_date" in columns:
        order.append("pt.target_date ASC NULLS LAST")
    if "created_at" in columns:
        order.append("pt.created_at ASC")
    order.append("pt.id ASC")
    return ", ".join(order)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row["name"]): {str(column["name"]) for column in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _parse_date(value: Any) -> date | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _date_sort(value: Any) -> str:
    return "" if value is None else str(value)


def _int_sort(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else -1


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
