"""Report newsletter subscriber metric integrity issues."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "newsletter_subscriber_growth_integrity"
TABLE = "newsletter_subscriber_metrics"
REQUIRED_COLUMNS = {
    "id",
    "subscriber_count",
    "active_subscriber_count",
    "unsubscribes",
    "churn_rate",
    "new_subscribers",
    "net_subscriber_change",
    "fetched_at",
}
NEGATIVE_METRICS = ("subscriber_count", "active_subscriber_count", "unsubscribes", "new_subscribers")
ISSUE_TYPES = (
    "negative_metric",
    "active_exceeds_total",
    "churn_rate_out_of_range",
    "net_subscriber_change_mismatch",
)


def build_newsletter_subscriber_growth_integrity_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic integrity report over subscriber metric rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    ordered = sorted(rows, key=lambda row: (_date_sort(row.get("fetched_at")), _int_sort(row.get("id"))))
    findings: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for row in ordered:
        normalized = _normalize_row(row)
        findings.extend(_row_findings(normalized))
        if previous is not None:
            mismatch = _net_change_mismatch(previous, normalized)
            if mismatch:
                findings.append(mismatch)
        previous = normalized

    findings.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), _date_sort(item.get("fetched_at")), _int_sort(item.get("metric_id")), item.get("metric_name") or ""))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit},
        "summary": {
            "metric_count": len(ordered),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": _counts(findings),
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No newsletter subscriber growth integrity issues found." if not findings else None,
        },
    }


def build_newsletter_subscriber_growth_integrity_report_from_db(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load subscriber metric rows from SQLite and build an integrity report."""
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
        return build_newsletter_subscriber_growth_integrity_report(
            [],
            days=days,
            limit=limit,
            now=generated_at,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )
    cutoff = generated_at - timedelta(days=days)
    rows = _load_rows(conn, cutoff, generated_at)
    return build_newsletter_subscriber_growth_integrity_report(rows, days=days, limit=limit, now=generated_at)


def format_newsletter_subscriber_growth_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subscriber_growth_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Newsletter Subscriber Growth Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} limit={report['filters']['limit']}",
        f"Totals: metrics={summary['metric_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
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
                f"  - {issue_type} metric={item['metric_id']} fetched_at={item['fetched_at']} "
                f"name={item.get('metric_name') or '-'} value={item.get('actual_value')}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: datetime, now: datetime) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id, subscriber_count, active_subscriber_count, unsubscribes,
                      churn_rate, new_subscribers, net_subscriber_change, fetched_at
               FROM newsletter_subscriber_metrics
               WHERE datetime(fetched_at) >= datetime(?) AND datetime(fetched_at) <= datetime(?)
               ORDER BY datetime(fetched_at) ASC, id ASC""",
            (cutoff.isoformat(), now.isoformat()),
        ).fetchall()
    ]


def _row_findings(row: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for metric_name in NEGATIVE_METRICS:
        value = row.get(metric_name)
        if value is not None and value < 0:
            findings.append(_finding("negative_metric", row, metric_name=metric_name, actual_value=value, expected_value=">= 0"))
    subscriber_count = row.get("subscriber_count")
    active_count = row.get("active_subscriber_count")
    if subscriber_count is not None and active_count is not None and active_count > subscriber_count:
        findings.append(_finding("active_exceeds_total", row, metric_name="active_subscriber_count", actual_value=active_count, expected_value=f"<= {subscriber_count}"))
    churn_rate = row.get("churn_rate")
    if churn_rate is not None and not 0 <= churn_rate <= 1:
        findings.append(_finding("churn_rate_out_of_range", row, metric_name="churn_rate", actual_value=churn_rate, expected_value="0..1"))
    return findings


def _net_change_mismatch(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any] | None:
    previous_count = previous.get("subscriber_count")
    current_count = current.get("subscriber_count")
    net_change = current.get("net_subscriber_change")
    if previous_count is None or current_count is None or net_change is None:
        return None
    expected = current_count - previous_count
    if net_change == expected:
        return None
    return _finding(
        "net_subscriber_change_mismatch",
        current,
        metric_name="net_subscriber_change",
        actual_value=net_change,
        expected_value=expected,
        previous_metric_id=previous.get("id"),
        previous_subscriber_count=previous_count,
    )


def _finding(issue_type: str, row: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "metric_id": row.get("id"),
        "fetched_at": row.get("fetched_at"),
        "subscriber_count": row.get("subscriber_count"),
        "active_subscriber_count": row.get("active_subscriber_count"),
        "churn_rate": row.get("churn_rate"),
        "net_subscriber_change": row.get("net_subscriber_change"),
        **extra,
    }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": _int_or_none(row.get("id")),
        "subscriber_count": _int_or_none(row.get("subscriber_count")),
        "active_subscriber_count": _int_or_none(row.get("active_subscriber_count")),
        "unsubscribes": _int_or_none(row.get("unsubscribes")),
        "new_subscribers": _int_or_none(row.get("new_subscribers")),
        "net_subscriber_change": _int_or_none(row.get("net_subscriber_change")),
        "churn_rate": _float_or_none(row.get("churn_rate")),
        "fetched_at": row.get("fetched_at"),
    }


def _group_findings(findings: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {issue_type: [] for issue_type in ISSUE_TYPES}
    for finding in findings:
        grouped[finding["issue_type"]].append(finding)
    return grouped


def _counts(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(finding["issue_type"] for finding in findings)
    return {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES}


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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _date_sort(value: Any) -> str:
    return "" if value is None else str(value)


def _int_sort(value: Any) -> int:
    parsed = _int_or_none(value)
    return parsed if parsed is not None else -1


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
