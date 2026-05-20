"""Audit model usage attribution links to content and pipeline runs."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_WINDOW_HOURS = 168
DEFAULT_COST_THRESHOLD = 1.0


def build_model_usage_pipeline_link_gaps_report(
    usage_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    pipeline_rows: list[dict[str, Any]],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    cost_threshold: float = DEFAULT_COST_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    link_checks: dict[str, str] | None = None,
) -> dict[str, Any]:
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if cost_threshold < 0:
        raise ValueError("cost_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    content_ids = {_int_or_none(row.get("content_id") or row.get("id")) for row in content_rows}
    pipeline_ids = {_int_or_none(row.get("pipeline_run_id") or row.get("id")) for row in pipeline_rows}
    checks = link_checks or {"generated_content": "available", "pipeline_runs": "available"}
    gap_items: list[dict[str, Any]] = []
    unattributed_cost: defaultdict[tuple[str, str], float] = defaultdict(float)
    total_cost: defaultdict[tuple[str, str], float] = defaultdict(float)
    total_rows: defaultdict[tuple[str, str], int] = defaultdict(int)

    for row in usage_rows:
        operation = _clean(row.get("operation_name"), "unknown")
        model = _clean(row.get("model_name"), "unknown")
        key = (operation, model)
        cost = _cost(row)
        total_cost[key] += cost
        total_rows[key] += 1
        content_id = _int_or_none(row.get("content_id"))
        pipeline_run_id = _int_or_none(row.get("pipeline_run_id"))
        if content_id is None and pipeline_run_id is None:
            unattributed_cost[key] += cost
            gap_items.append(_item(row, "missing_content_and_pipeline_link", cost))
        if content_id is not None and checks.get("generated_content") == "available" and content_id not in content_ids:
            gap_items.append(_item(row, "content_id_missing_generated_content", cost))
        if pipeline_run_id is not None and checks.get("pipeline_runs") == "available" and pipeline_run_id not in pipeline_ids:
            gap_items.append(_item(row, "pipeline_run_id_missing_pipeline_run", cost))

    bucket_summary = [
        {
            "operation_name": operation,
            "model_name": model,
            "usage_count": total_rows[(operation, model)],
            "total_cost": round(total_cost[(operation, model)], 6),
            "unattributed_cost": round(unattributed_cost[(operation, model)], 6),
            "high_unattributed_cost": unattributed_cost[(operation, model)] >= cost_threshold,
        }
        for operation, model in sorted(total_rows)
    ]
    for bucket in bucket_summary:
        if bucket["high_unattributed_cost"]:
            gap_items.append(
                {
                    "usage_id": None,
                    "issue_type": "high_unattributed_cost_bucket",
                    "operation_name": bucket["operation_name"],
                    "model_name": bucket["model_name"],
                    "content_id": None,
                    "pipeline_run_id": None,
                    "cost": bucket["unattributed_cost"],
                    "created_at": None,
                }
            )

    gap_items.sort(key=_sort_key)
    shown = gap_items[:limit]
    return {
        "artifact_type": "model_usage_pipeline_link_gaps",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"window_hours": window_hours, "cost_threshold": cost_threshold, "limit": limit},
        "summary": {"usage_count": len(usage_rows), "gap_count": len(gap_items), "shown_count": len(shown)},
        "link_checks": checks,
        "bucket_summary": bucket_summary,
        "gap_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_model_usage_pipeline_link_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "model_usage" in schema else ["model_usage"]
    missing_columns: dict[str, list[str]] = {}
    usage_rows = _load_usage(conn, schema, missing_columns, kwargs.get("window_hours", DEFAULT_WINDOW_HOURS), kwargs.get("now")) if "model_usage" in schema else []
    content_rows = _load_ids(conn, schema, "generated_content", "content_id") if "generated_content" in schema else []
    pipeline_rows = _load_ids(conn, schema, "pipeline_runs", "pipeline_run_id") if "pipeline_runs" in schema else []
    checks = {
        "generated_content": "available" if "generated_content" in schema else "unavailable_missing_table",
        "pipeline_runs": "available" if "pipeline_runs" in schema else "unavailable_missing_table",
    }
    return build_model_usage_pipeline_link_gaps_report(
        usage_rows,
        content_rows,
        pipeline_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        link_checks=checks,
        **kwargs,
    )


def format_model_usage_pipeline_link_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_model_usage_pipeline_link_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Model Usage Pipeline Link Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: {report['thresholds']['window_hours']} hours",
        f"Cost threshold: {report['thresholds']['cost_threshold']}",
        f"Totals: usage={summary['usage_count']} gaps={summary['gap_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["gap_items"]:
        lines.append("No model usage pipeline link gaps found.")
        return "\n".join(lines)
    lines.extend(["", "usage_id | issue_type | operation_name | model_name | cost"])
    for item in report["gap_items"]:
        lines.append(f"{item['usage_id'] or '-'} | {item['issue_type']} | {item['operation_name']} | {item['model_name']} | {item['cost']}")
    return "\n".join(lines)


def _load_usage(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_columns: dict[str, list[str]],
    window_hours: int,
    now: datetime | None,
) -> list[dict[str, Any]]:
    cols = schema["model_usage"]
    select = [
        _expr(cols, "id", "usage_id", "rowid"),
        _expr(cols, "content_id", "content_id", "NULL"),
        _expr(cols, "pipeline_run_id", "pipeline_run_id", "NULL"),
        _first_expr(cols, ("operation_name", "operation", "task"), "operation_name", "'unknown'"),
        _first_expr(cols, ("model_name", "model"), "model_name", "'unknown'"),
        _first_expr(cols, ("cost", "total_cost", "estimated_cost"), "cost", "0"),
        _expr(cols, "created_at", "created_at", "NULL"),
    ]
    where = ""
    params: list[Any] = []
    if "created_at" in cols:
        cutoff = _utc(now or datetime.now(timezone.utc)) - timedelta(hours=window_hours)
        where = " WHERE created_at >= ?"
        params.append(cutoff.isoformat())
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM model_usage{where} ORDER BY rowid ASC", params)]


def _load_ids(conn: sqlite3.Connection, schema: dict[str, set[str]], table: str, output: str) -> list[dict[str, Any]]:
    cols = schema[table]
    if "id" not in cols:
        return []
    return [dict(row) for row in conn.execute(f"SELECT id AS {output} FROM {table} ORDER BY id ASC")]


def _item(row: dict[str, Any], issue_type: str, cost: float) -> dict[str, Any]:
    return {
        "usage_id": _int_or_none(row.get("usage_id") or row.get("id")),
        "issue_type": issue_type,
        "operation_name": _clean(row.get("operation_name"), "unknown"),
        "model_name": _clean(row.get("model_name"), "unknown"),
        "content_id": _int_or_none(row.get("content_id")),
        "pipeline_run_id": _int_or_none(row.get("pipeline_run_id")),
        "cost": round(cost, 6),
        "created_at": _iso_or_none(row.get("created_at")),
    }


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["operation_name"], item["model_name"], item["usage_id"] is None, item["usage_id"] or 0, item["issue_type"])


def _cost(row: dict[str, Any]) -> float:
    try:
        return float(row.get("cost") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _first_expr(columns: set[str], candidates: tuple[str, ...], output: str, default: str) -> str:
    for column in candidates:
        if column in columns:
            return f"{column} AS {output}"
    return f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


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
