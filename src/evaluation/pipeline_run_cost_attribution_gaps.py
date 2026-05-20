"""Audit cost attribution joins between pipeline_runs and model_usage."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_HOURS = 168
DEFAULT_LIMIT = 100
SUCCESS_STATUSES = {"completed", "complete", "success", "succeeded", "published"}
ISSUE_TYPES = ("missing_pipeline_run_id", "orphaned_pipeline_run_id", "missing_usage", "content_id_mismatch")


def build_pipeline_run_cost_attribution_gaps_report(
    rows: list[dict[str, Any]],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    content_type: str = "all",
    operation_name: str = "all",
    issue_type: str = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=window_hours)
    content_filter = _clean(content_type).lower() or "all"
    op_filter = _clean(operation_name).lower() or "all"
    issue_filter = _clean(issue_type).lower() or "all"
    findings: list[dict[str, Any]] = []
    scanned = 0

    for row in rows:
        row_time = _parse_dt(row.get("event_at"))
        if row_time is not None and row_time < cutoff:
            continue
        row_content_type = _clean(row.get("content_type"), "unknown").lower()
        row_operation = _clean(row.get("operation_name"), "unknown").lower()
        if content_filter != "all" and row_content_type != content_filter:
            continue
        if op_filter != "all" and row_operation != op_filter:
            continue
        scanned += 1
        row_issue = _clean(row.get("issue_type")).lower()
        if issue_filter != "all" and row_issue != issue_filter:
            continue
        finding = {
            "issue_type": row_issue,
            "model_usage_id": _int_or_none(row.get("model_usage_id")),
            "pipeline_run_id": _int_or_none(row.get("pipeline_run_id")),
            "content_id": _int_or_none(row.get("content_id")),
            "usage_content_id": _int_or_none(row.get("usage_content_id")),
            "pipeline_content_id": _int_or_none(row.get("pipeline_content_id")),
            "content_type": row_content_type,
            "operation_name": row_operation,
            "status": _clean(row.get("status"), "unknown").lower(),
            "event_at": row_time.isoformat() if row_time else None,
        }
        findings.append(finding)

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "pipeline_run_cost_attribution_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "window_hours": window_hours,
            "cutoff": cutoff.isoformat(),
            "content_type": content_filter,
            "operation_name": op_filter,
            "issue_type": issue_filter,
            "limit": limit,
        },
        "findings": shown,
        "grouped_counts": _grouped_counts(findings),
        "summary": {
            "rows_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue: Counter(item["issue_type"] for item in findings)[issue] for issue in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_pipeline_run_cost_attribution_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, schema, missing_tables, missing_columns)
    return build_pipeline_run_cost_attribution_gaps_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_pipeline_run_cost_attribution_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_run_cost_attribution_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Pipeline Run Cost Attribution Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: {filters['window_hours']} hours",
        f"Content type: {filters['content_type']}",
        f"Operation: {filters['operation_name']}",
        f"Issue: {filters['issue_type']}",
        f"Totals: scanned={summary['rows_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append("No pipeline run cost attribution gaps found.")
        return "\n".join(lines)
    lines.extend(["", "content_type | operation_name | issue_type | count"])
    for group in report["grouped_counts"]:
        lines.append(f"{group['content_type']} | {group['operation_name']} | {group['issue_type']} | {group['count']}")
    lines.extend(["", "issue | usage_id | pipeline_run_id | content_id | usage_content_id | pipeline_content_id"])
    for item in report["findings"]:
        lines.append(
            f"{item['issue_type']} | {item['model_usage_id'] or '-'} | {item['pipeline_run_id'] or '-'} | "
            f"{item['content_id'] or '-'} | {item['usage_content_id'] or '-'} | {item['pipeline_content_id'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    if "model_usage" not in schema:
        missing_tables.append("model_usage")
    if "pipeline_runs" not in schema:
        missing_tables.append("pipeline_runs")
    if missing_tables:
        return []
    mu = schema["model_usage"]
    pr = schema["pipeline_runs"]
    rows: list[dict[str, Any]] = []

    if "id" not in mu:
        missing_columns["model_usage"] = ["id"]
    if "id" not in pr:
        missing_columns["pipeline_runs"] = ["id"]
    if missing_columns:
        return []

    mu_pipeline_col = "pipeline_run_id" if "pipeline_run_id" in mu else None
    mu_content = _qualified_col(mu, "content_id", "mu", "NULL")
    mu_op = _qualified_col(mu, "operation_name", "mu", _qualified_col(mu, "operation", "mu", "'unknown'"))
    mu_created = _qualified_col(mu, "created_at", "mu", _qualified_col(mu, "updated_at", "mu", "NULL"))
    pr_content = _qualified_col(pr, "content_id", "pr", "NULL")
    pr_type = _qualified_col(pr, "content_type", "pr", "'unknown'")
    pr_status = _qualified_col(pr, "status", "pr", "'unknown'")
    pr_created = _qualified_col(pr, "created_at", "pr", _qualified_col(pr, "updated_at", "pr", "NULL"))

    if mu_pipeline_col is None:
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT 'missing_pipeline_run_id' AS issue_type, mu.id AS model_usage_id, NULL AS pipeline_run_id,
                           {mu_content} AS content_id, {mu_content} AS usage_content_id, NULL AS pipeline_content_id,
                           'unknown' AS content_type, {mu_op} AS operation_name, 'unknown' AS status, {mu_created} AS event_at
                    FROM model_usage mu"""
            )
        )
    else:
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT 'missing_pipeline_run_id' AS issue_type, mu.id AS model_usage_id, NULL AS pipeline_run_id,
                           {mu_content} AS content_id, {mu_content} AS usage_content_id, NULL AS pipeline_content_id,
                           COALESCE({pr_type}, 'unknown') AS content_type, {mu_op} AS operation_name,
                           COALESCE({pr_status}, 'unknown') AS status, {mu_created} AS event_at
                    FROM model_usage mu
                    LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
                    WHERE mu.pipeline_run_id IS NULL"""
            )
        )
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT 'orphaned_pipeline_run_id' AS issue_type, mu.id AS model_usage_id, mu.pipeline_run_id AS pipeline_run_id,
                           {mu_content} AS content_id, {mu_content} AS usage_content_id, NULL AS pipeline_content_id,
                           'unknown' AS content_type, {mu_op} AS operation_name, 'unknown' AS status, {mu_created} AS event_at
                    FROM model_usage mu
                    LEFT JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
                    WHERE mu.pipeline_run_id IS NOT NULL AND pr.id IS NULL"""
            )
        )
        rows.extend(
            dict(row)
            for row in conn.execute(
                f"""SELECT 'content_id_mismatch' AS issue_type, mu.id AS model_usage_id, mu.pipeline_run_id AS pipeline_run_id,
                           {pr_content} AS content_id, {mu_content} AS usage_content_id, {pr_content} AS pipeline_content_id,
                           {pr_type} AS content_type, {mu_op} AS operation_name, {pr_status} AS status,
                           COALESCE({mu_created}, {pr_created}) AS event_at
                    FROM model_usage mu
                    INNER JOIN pipeline_runs pr ON pr.id = mu.pipeline_run_id
                    WHERE {mu_content} IS NOT NULL AND {pr_content} IS NOT NULL AND CAST({mu_content} AS TEXT) != CAST({pr_content} AS TEXT)"""
            )
        )

    rows.extend(
        dict(row)
        for row in conn.execute(
            f"""SELECT 'missing_usage' AS issue_type, NULL AS model_usage_id, pr.id AS pipeline_run_id,
                       {pr_content} AS content_id, NULL AS usage_content_id, {pr_content} AS pipeline_content_id,
                       {pr_type} AS content_type, 'unknown' AS operation_name, {pr_status} AS status, {pr_created} AS event_at
                FROM pipeline_runs pr
                WHERE {pr_content} IS NOT NULL
                  AND lower(COALESCE({pr_status}, '')) IN ({_quoted(SUCCESS_STATUSES)})
                  AND NOT EXISTS (SELECT 1 FROM model_usage mu WHERE {'mu.pipeline_run_id = pr.id' if mu_pipeline_col else '0'})"""
        )
    )
    return rows


def _grouped_counts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["content_type"], item["operation_name"], item["issue_type"]) for item in items)
    return [
        {"content_type": content_type, "operation_name": operation, "issue_type": issue, "count": count}
        for (content_type, operation, issue), count in sorted(counts.items())
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["issue_type"], item["content_type"], item["operation_name"], item["pipeline_run_id"] or 0, item["model_usage_id"] or 0)


def _qualified_col(columns: set[str], column: str, alias: str, default: str) -> str:
    return f"{alias}.{column}" if column in columns else default


def _quoted(values: set[str]) -> str:
    return ", ".join(f"'{value}'" for value in sorted(values))


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
    try:
        return _utc(datetime.fromisoformat(str(value).strip().replace("Z", "+00:00")))
    except ValueError:
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
