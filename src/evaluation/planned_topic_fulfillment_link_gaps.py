"""Audit planned topic fulfillment links to generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LIMIT = 100
DEFAULT_STATUS = "generated"
DEFAULT_CAMPAIGN_ID = "all"
ISSUE_TYPES = (
    "generated_missing_content_id",
    "missing_generated_content",
    "abandoned_generated_content",
    "content_type_mismatch",
)


def build_planned_topic_fulfillment_link_gaps_report(
    planned_topic_rows: list[dict[str, Any]],
    *,
    campaign_id: str | int = DEFAULT_CAMPAIGN_ID,
    status: str | Iterable[str] = DEFAULT_STATUS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    statuses = _normalize_status_filter(status)
    normalized_campaign_id = _clean(campaign_id) or DEFAULT_CAMPAIGN_ID
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    scanned = 0

    for row in planned_topic_rows:
        row_status = _clean(row.get("topic_status") or row.get("status")).lower()
        row_campaign_id = _clean(row.get("campaign_id"))
        if statuses and "all" not in statuses and row_status not in statuses:
            continue
        if normalized_campaign_id != "all" and row_campaign_id != normalized_campaign_id:
            continue
        scanned += 1
        issues = _issues_for_row(row, row_status)
        for issue_type in issues:
            finding = {
                "planned_topic_id": _first(row, "planned_topic_id", "id"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name"),
                "status": row_status,
                "content_id": row.get("content_id"),
                "content_type": row.get("content_type"),
                "expected_content_type": _expected_content_type(row.get("source_material")),
                "content_status": row.get("content_status"),
                "content_published": row.get("content_published"),
                "issue_type": issue_type,
            }
            findings.append(finding)
            counts[issue_type] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "planned_topic_fulfillment_link_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"campaign_id": normalized_campaign_id, "status": list(statuses), "limit": limit},
        "summary": {
            "planned_topic_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No planned topic fulfillment link gaps found." if not findings else None,
        },
    }


def build_planned_topic_fulfillment_link_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"planned_topics", "content_campaigns", "generated_content"}
    missing = sorted(required - set(schema))
    if missing:
        return build_planned_topic_fulfillment_link_gaps_report([], missing_tables=missing, **kwargs)
    return build_planned_topic_fulfillment_link_gaps_report(_load_topics(conn, schema), **kwargs)


def format_planned_topic_fulfillment_link_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_planned_topic_fulfillment_link_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Planned Topic Fulfillment Link Gaps",
        f"Generated: {report['generated_at']}",
        f"Campaign: {report['filters']['campaign_id']}",
        f"Status: {', '.join(report['filters']['status'])}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: planned_topics={summary['planned_topic_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "planned_topic_id | campaign_id | status | content_id | expected | actual | issue_type"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['planned_topic_id'] or '-'} | {finding['campaign_id'] or '-'} | "
            f"{finding['status'] or '-'} | {finding['content_id'] or '-'} | "
            f"{finding['expected_content_type'] or '-'} | {finding['content_type'] or '-'} | "
            f"{finding['issue_type']}"
        )
    return "\n".join(lines)


def _issues_for_row(row: dict[str, Any], row_status: str) -> list[str]:
    issues: list[str] = []
    if row_status != "generated":
        return issues
    content_id = row.get("content_id")
    content_exists = row.get("resolved_content_id") is not None
    if content_id in (None, ""):
        issues.append("generated_missing_content_id")
        return issues
    if not content_exists:
        issues.append("missing_generated_content")
        return issues

    content_status = _clean(row.get("content_status")).lower()
    published = _int(row.get("content_published"))
    if published == -1 or content_status in {"abandoned", "rejected"}:
        issues.append("abandoned_generated_content")
    expected = _expected_content_type(row.get("source_material"))
    actual = _clean(row.get("content_type"))
    if expected and actual and expected != actual:
        issues.append("content_type_mismatch")
    return issues


def _load_topics(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    pt_cols = schema["planned_topics"]
    cc_cols = schema["content_campaigns"]
    gc_cols = schema["generated_content"]
    if "id" not in pt_cols:
        return []
    campaign_column = "campaign_id" if "campaign_id" in pt_cols else "content_campaign_id"
    campaign_expr = f"pt.{campaign_column}" if campaign_column in pt_cols else "NULL"
    content_expr = _column_expr(pt_cols, "content_id", "generated_content_id", fallback="NULL", alias="pt")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   pt.id AS planned_topic_id,
                   {campaign_expr} AS campaign_id,
                   cc.name AS campaign_name,
                   {content_expr} AS content_id,
                   gc.id AS resolved_content_id,
                   {_column_expr(pt_cols, "status", fallback="'planned'", alias="pt")} AS topic_status,
                   {_column_expr(pt_cols, "source_material", fallback="NULL", alias="pt")} AS source_material,
                   {_column_expr(gc_cols, "content_type", "type", fallback="NULL", alias="gc")} AS content_type,
                   {_column_expr(gc_cols, "status", fallback="NULL", alias="gc")} AS content_status,
                   {_column_expr(gc_cols, "published", fallback="NULL", alias="gc")} AS content_published
               FROM planned_topics pt
               LEFT JOIN content_campaigns cc ON cc.id = {campaign_expr}
               LEFT JOIN generated_content gc ON gc.id = {content_expr}
               ORDER BY pt.id ASC"""
        ).fetchall()
    ]


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


def _expected_content_type(source_material: Any) -> str:
    if source_material in (None, ""):
        return ""
    if isinstance(source_material, dict):
        value = source_material.get("expected_content_type")
    else:
        try:
            parsed = json.loads(str(source_material))
        except (TypeError, json.JSONDecodeError):
            return ""
        value = parsed.get("expected_content_type") if isinstance(parsed, dict) else None
    return _clean(value)


def _normalize_status_filter(status: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(status, str):
        values = status.split(",")
    else:
        values = list(status)
    normalized = tuple(value for value in (_clean(item).lower() for item in values) if value)
    return normalized or (DEFAULT_STATUS,)


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _int_or_text(finding.get("campaign_id")),
        _int_or_text(finding.get("planned_topic_id")),
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
    )


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


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
