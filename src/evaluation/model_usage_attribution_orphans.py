"""Report model usage rows that cannot be attributed to content or pipeline runs."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50


def build_model_usage_attribution_orphans_report(
    usage_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    pipeline_rows: list[dict[str, Any]],
    *,
    include_details: bool = False,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    link_checks: dict[str, str] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    content_ids = {_int_or_none(row.get("content_id") or row.get("id")) for row in content_rows}
    pipeline_ids = {_int_or_none(row.get("pipeline_run_id") or row.get("id")) for row in pipeline_rows}
    checks = link_checks or {"generated_content": "available", "pipeline_runs": "available"}
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    details: list[dict[str, Any]] = []

    for row in usage_rows:
        reasons = _orphan_reasons(row, content_ids, pipeline_ids, checks)
        if not reasons:
            continue
        operation = _clean(row.get("operation_name"), "unknown")
        model = _clean(row.get("model_name"), "unknown")
        key = (operation, model)
        bucket = buckets.setdefault(
            key,
            {
                "operation_name": operation,
                "model_name": model,
                "orphan_count": 0,
                "total_estimated_cost": 0.0,
                "latest_created_at": None,
            },
        )
        bucket["orphan_count"] += 1
        bucket["total_estimated_cost"] += _cost(row)
        created_at = _iso_or_none(row.get("created_at"))
        if created_at and (bucket["latest_created_at"] is None or created_at > bucket["latest_created_at"]):
            bucket["latest_created_at"] = created_at
        details.append(_detail(row, reasons))

    summary_rows = []
    for bucket in buckets.values():
        bucket = dict(bucket)
        bucket["total_estimated_cost"] = round(bucket["total_estimated_cost"], 6)
        summary_rows.append(bucket)
    summary_rows.sort(key=lambda item: (item["operation_name"], item["model_name"]))
    details.sort(key=lambda item: (item["operation_name"], item["model_name"], item["usage_id"] or 0))
    shown = details[:limit] if include_details else []

    return {
        "artifact_type": "model_usage_attribution_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {"include_details": include_details, "limit": limit},
        "summary": {
            "usage_count": len(usage_rows),
            "orphan_count": len(details),
            "summary_bucket_count": len(summary_rows),
            "shown_detail_count": len(shown),
        },
        "summary_rows": summary_rows,
        "orphan_examples": shown,
        "link_checks": checks,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_model_usage_attribution_orphans_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "model_usage" in schema else ["model_usage"]
    missing_columns: dict[str, list[str]] = {}
    usage_rows = _load_usage(conn, schema, missing_columns) if "model_usage" in schema else []
    content_rows = _load_ids(conn, schema, "generated_content", "content_id") if "generated_content" in schema else []
    pipeline_rows = _load_ids(conn, schema, "pipeline_runs", "pipeline_run_id") if "pipeline_runs" in schema else []
    checks = {
        "generated_content": "available" if "generated_content" in schema else "unavailable_missing_table",
        "pipeline_runs": "available" if "pipeline_runs" in schema else "unavailable_missing_table",
    }
    return build_model_usage_attribution_orphans_report(
        usage_rows,
        content_rows,
        pipeline_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        link_checks=checks,
        **kwargs,
    )


def format_model_usage_attribution_orphans_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_model_usage_attribution_orphans_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Model Usage Attribution Orphans",
        f"Generated: {report['generated_at']}",
        f"Totals: usage={summary['usage_count']} orphans={summary['orphan_count']} buckets={summary['summary_bucket_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["summary_rows"]:
        lines.append("No model usage attribution orphans found.")
        return "\n".join(lines)
    lines.extend(["", "operation_name | model_name | orphan_count | total_estimated_cost | latest_created_at"])
    for row in report["summary_rows"]:
        lines.append(
            f"{row['operation_name']} | {row['model_name']} | {row['orphan_count']} | "
            f"{row['total_estimated_cost']} | {row['latest_created_at'] or '-'}"
        )
    if report["orphan_examples"]:
        lines.extend(["", "usage_id | orphan_reasons | content_id | pipeline_run_id | estimated_cost | created_at"])
        for item in report["orphan_examples"]:
            lines.append(
                f"{item['usage_id'] or '-'} | {', '.join(item['orphan_reasons'])} | "
                f"{item['content_id'] or '-'} | {item['pipeline_run_id'] or '-'} | "
                f"{item['estimated_cost']} | {item['created_at'] or '-'}"
            )
    return "\n".join(lines)


def _load_usage(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = schema["model_usage"]
    required = {"operation_name", "model_name"}
    missing = sorted(required - cols)
    if missing:
        missing_columns["model_usage"] = missing
    select = [
        _expr(cols, "id", "usage_id", "rowid"),
        _expr(cols, "content_id", "content_id", "NULL"),
        _expr(cols, "pipeline_run_id", "pipeline_run_id", "NULL"),
        _first_expr(cols, ("operation_name", "operation", "task"), "operation_name", "'unknown'"),
        _first_expr(cols, ("model_name", "model"), "model_name", "'unknown'"),
        _first_expr(cols, ("estimated_cost", "total_cost", "cost"), "estimated_cost", "0"),
        _expr(cols, "created_at", "created_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM model_usage ORDER BY rowid ASC")]


def _load_ids(conn: sqlite3.Connection, schema: dict[str, set[str]], table: str, output: str) -> list[dict[str, Any]]:
    if "id" not in schema[table]:
        return []
    return [dict(row) for row in conn.execute(f"SELECT id AS {output} FROM {table} ORDER BY id ASC")]


def _orphan_reasons(
    row: dict[str, Any],
    content_ids: set[int | None],
    pipeline_ids: set[int | None],
    checks: dict[str, str],
) -> list[str]:
    content_id = _int_or_none(row.get("content_id"))
    pipeline_run_id = _int_or_none(row.get("pipeline_run_id"))
    reasons: list[str] = []
    if content_id is None and pipeline_run_id is None:
        reasons.append("missing_content_and_pipeline")
    if content_id is not None and checks.get("generated_content") == "available" and content_id not in content_ids:
        reasons.append("missing_generated_content")
    if pipeline_run_id is not None and checks.get("pipeline_runs") == "available" and pipeline_run_id not in pipeline_ids:
        reasons.append("missing_pipeline_run")
    return reasons


def _detail(row: dict[str, Any], reasons: list[str]) -> dict[str, Any]:
    return {
        "usage_id": _int_or_none(row.get("usage_id") or row.get("id")),
        "operation_name": _clean(row.get("operation_name"), "unknown"),
        "model_name": _clean(row.get("model_name"), "unknown"),
        "content_id": _int_or_none(row.get("content_id")),
        "pipeline_run_id": _int_or_none(row.get("pipeline_run_id")),
        "estimated_cost": round(_cost(row), 6),
        "created_at": _iso_or_none(row.get("created_at")),
        "orphan_reasons": reasons,
    }


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


def _cost(row: dict[str, Any]) -> float:
    try:
        return float(row.get("estimated_cost") or row.get("cost") or 0.0)
    except (TypeError, ValueError):
        return 0.0
