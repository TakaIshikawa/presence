"""Report publication attempts with response metadata latency gaps."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MAX_LATENCY_MINUTES = 30
ARTIFACT_TYPE = "publication_attempt_response_latency_gaps"
TABLE = "publication_attempts"
REQUIRED_COLUMNS = {"id", "content_id", "platform", "response_metadata"}
ISSUE_TYPES = (
    "missing_timestamps",
    "negative_latency",
    "over_threshold_latency",
    "non_terminal_with_response_metadata",
)
TERMINAL_STATUSES = {"success", "succeeded", "published", "sent", "completed", "failed", "error"}
START_COLUMNS = ("started_at", "request_started_at", "response_started_at", "attempt_started_at", "attempted_at", "created_at")
COMPLETED_COLUMNS = ("completed_at", "finished_at", "response_completed_at", "attempt_completed_at", "published_at")
STATUS_COLUMNS = ("status", "outcome", "result")


def build_publication_attempt_response_latency_gaps_report(
    rows: list[dict[str, Any]],
    *,
    max_latency_minutes: int = DEFAULT_MAX_LATENCY_MINUTES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    optional_missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic latency-gap report from publication attempt rows."""
    if max_latency_minutes <= 0:
        raise ValueError("max_latency_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    metadata_rows = [_normalize_row(row) for row in rows if _has_response_metadata(row.get("response_metadata"))]
    findings: list[dict[str, Any]] = []
    for row in metadata_rows:
        findings.extend(_row_findings(row, max_latency_minutes=max_latency_minutes))

    findings.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), item["platform"], _date_sort(item.get("started_at")), _int_sort(item.get("attempt_id"))))
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"max_latency_minutes": max_latency_minutes, "limit": limit},
        "summary": {
            "attempt_count": len(rows),
            "attempts_with_response_metadata": len(metadata_rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": _counts(findings),
            "sample_attempt_ids": [item["attempt_id"] for item in shown[:10]],
        },
        "findings": _group_findings(shown),
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "optional_missing_columns": {table: sorted(columns) for table, columns in sorted((optional_missing_columns or {}).items())},
        "empty_state": {
            "is_empty": not findings,
            "message": "No publication attempt response latency gaps found." if not findings else None,
        },
    }


def build_publication_attempt_response_latency_gaps_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Load publication attempts from SQLite and build the latency-gap report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    columns = schema.get(TABLE)
    if columns is None:
        return build_publication_attempt_response_latency_gaps_report([], missing_tables=[TABLE], **kwargs)

    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return build_publication_attempt_response_latency_gaps_report([], missing_columns={TABLE: missing}, **kwargs)

    optional_missing = _optional_missing(columns)
    return build_publication_attempt_response_latency_gaps_report(
        _load_rows(conn, columns),
        optional_missing_columns={TABLE: optional_missing} if optional_missing else {},
        **kwargs,
    )


def format_publication_attempt_response_latency_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_response_latency_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    lines = [
        "Publication Attempt Response Latency Gaps",
        f"Generated: {report['generated_at']}",
        f"Filters: max_latency_minutes={report['filters']['max_latency_minutes']} limit={report['filters']['limit']}",
        (
            "Totals: "
            f"attempts={summary['attempt_count']} "
            f"with_metadata={summary['attempts_with_response_metadata']} "
            f"findings={summary['finding_count']} shown={summary['shown_count']}"
        ),
        "Sample attempt IDs: " + (", ".join(str(item) for item in summary["sample_attempt_ids"]) or "-"),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report.get("optional_missing_columns"):
        lines.append("Missing optional columns: " + _format_missing(report["optional_missing_columns"]))
    if not any(report["findings"].values()):
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Findings:")
    for issue_type in ISSUE_TYPES:
        for item in report["findings"].get(issue_type, []):
            lines.append(
                f"  - {issue_type} attempt={item['attempt_id']} content={item['content_id']} "
                f"platform={item['platform']} status={item['status'] or '-'} "
                f"latency={item['latency_minutes'] if item['latency_minutes'] is not None else '-'}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    start_expr = _column_expr(columns, *START_COLUMNS, fallback="NULL")
    completed_expr = _column_expr(columns, *COMPLETED_COLUMNS, fallback="NULL")
    status_expr = _column_expr(columns, *STATUS_COLUMNS, fallback="NULL")
    success_expr = _column_expr(columns, "success", fallback="NULL")
    rows = conn.execute(
        f"""SELECT id AS attempt_id,
                  content_id,
                  platform,
                  {success_expr} AS success,
                  {status_expr} AS status,
                  {start_expr} AS started_at,
                  {completed_expr} AS completed_at,
                  response_metadata
           FROM publication_attempts
           WHERE response_metadata IS NOT NULL AND TRIM(CAST(response_metadata AS TEXT)) != ''
           ORDER BY {_order_expr(columns)} ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _row_findings(row: dict[str, Any], *, max_latency_minutes: int) -> list[dict[str, Any]]:
    if not _is_terminal(row):
        return [_finding("non_terminal_with_response_metadata", row)]

    started_at = _parse_timestamp(row.get("started_at"))
    completed_at = _parse_timestamp(row.get("completed_at"))
    if started_at is None or completed_at is None:
        return [_finding("missing_timestamps", row)]

    latency = (completed_at - started_at).total_seconds() / 60
    if latency < 0:
        return [_finding("negative_latency", row, latency_minutes=round(latency, 3))]
    if latency > max_latency_minutes:
        return [_finding("over_threshold_latency", row, latency_minutes=round(latency, 3), threshold_minutes=max_latency_minutes)]
    return []


def _finding(issue_type: str, row: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {
        "issue_type": issue_type,
        "attempt_id": row.get("attempt_id"),
        "content_id": row.get("content_id"),
        "platform": row.get("platform"),
        "success": row.get("success"),
        "status": row.get("status"),
        "started_at": row.get("started_at"),
        "completed_at": row.get("completed_at"),
        "latency_minutes": None,
        **extra,
    }


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "attempt_id": row.get("attempt_id", row.get("id")),
        "content_id": row.get("content_id"),
        "platform": _clean(row.get("platform")) or "unknown",
        "success": row.get("success"),
        "status": _clean(row.get("status")).lower(),
        "started_at": row.get("started_at", row.get("attempted_at")),
        "completed_at": row.get("completed_at"),
        "response_metadata": row.get("response_metadata"),
    }


def _is_terminal(row: dict[str, Any]) -> bool:
    success = row.get("success")
    if success is not None and str(success).strip() != "":
        return True
    return _clean(row.get("status")).lower() in TERMINAL_STATUSES


def _has_response_metadata(raw: Any) -> bool:
    return raw is not None and _clean(raw) != ""


def _group_findings(findings: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped = {issue_type: [] for issue_type in ISSUE_TYPES}
    for finding in findings:
        grouped[finding["issue_type"]].append(finding)
    return grouped


def _counts(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(finding["issue_type"] for finding in findings)
    return {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES}


def _optional_missing(columns: set[str]) -> list[str]:
    missing = []
    if not set(START_COLUMNS) & columns:
        missing.append("|".join(START_COLUMNS))
    if not set(COMPLETED_COLUMNS) & columns:
        missing.append("|".join(COMPLETED_COLUMNS))
    if "success" not in columns and not set(STATUS_COLUMNS) & columns:
        missing.append("success|status|outcome|result")
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _column_expr(columns: set[str], *columns_to_try: str, fallback: str) -> str:
    for column in columns_to_try:
        if column in columns:
            return _quote_identifier(column)
    return fallback


def _order_expr(columns: set[str]) -> str:
    return _column_expr(columns, *START_COLUMNS, fallback="id")


def _parse_timestamp(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _date_sort(value: Any) -> str:
    return _clean(value)


def _int_sort(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return -1


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
