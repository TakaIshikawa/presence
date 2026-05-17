"""Report pipeline runs with unusually long gate durations."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_DURATION_HOURS = 24.0
DEFAULT_LIMIT = 50


def build_pipeline_gate_duration_outliers_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_duration_hours: float = DEFAULT_MIN_DURATION_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if min_duration_hours <= 0:
        raise ValueError("min_duration_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {"days": days, "min_duration_hours": min_duration_hours, "limit": limit, "window_start": cutoff.isoformat(), "window_end": generated_at.isoformat()}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns, missing_optional_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty(generated_at, filters, missing_tables, missing_columns, missing_optional_columns)
    rows = _load_rows(conn, schema, cutoff, generated_at)
    outliers = []
    for row in rows:
        created = _parse(row["created_at"])
        if created is None:
            continue
        outcome = row["outcome"] or "unknown"
        end_at = _parse(row.get("published_at")) if outcome == "published" else generated_at
        if end_at is None:
            end_at = generated_at
        duration_hours = (end_at - created).total_seconds() / 3600
        if duration_hours < min_duration_hours:
            continue
        outliers.append(
            {
                "pipeline_run_id": int(row["id"]),
                "batch_id": row.get("batch_id"),
                "content_id": row.get("content_id"),
                "content_type": row.get("content_type"),
                "outcome": outcome,
                "created_at": created.isoformat(),
                "ended_at": end_at.isoformat(),
                "duration_hours": round(duration_hours, 2),
            }
        )
    outliers.sort(key=lambda item: (-item["duration_hours"], item["pipeline_run_id"]))
    limited = outliers[:limit]
    return {
        "artifact_type": "pipeline_gate_duration_outliers",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"run_count": len(rows), "outlier_count": len(outliers), "returned_count": len(limited)},
        "outlier_runs": limited,
        "outcome_breakdowns": dict(sorted(Counter(item["outcome"] for item in outliers).items())),
        "missing_tables": [],
        "missing_columns": {},
        "missing_optional_columns": missing_optional_columns,
    }


def format_pipeline_gate_duration_outliers_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_gate_duration_outliers_text(report: dict[str, Any]) -> str:
    t = report["totals"]
    lines = [
        "Pipeline Gate Duration Outliers",
        f"Generated: {report['generated_at']}",
        f"Filters: days={report['filters']['days']} min_duration_hours={report['filters']['min_duration_hours']} limit={report['filters']['limit']}",
        f"Totals: runs={t['run_count']} outliers={t['outlier_count']} returned={t['returned_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report.get("missing_optional_columns"):
        lines.append("Missing optional columns: " + _format_missing(report["missing_optional_columns"]))
    if report["outlier_runs"]:
        lines.extend(["", "Outlier runs:"])
        for item in report["outlier_runs"]:
            lines.append(f"- run={item['pipeline_run_id']} outcome={item['outcome']} duration_h={item['duration_hours']} content={item['content_id']}")
    else:
        lines.append("No pipeline gate duration outliers found.")
    return "\n".join(lines)


format_pipeline_gate_duration_outliers_table = format_pipeline_gate_duration_outliers_text


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], cutoff: datetime, generated_at: datetime) -> list[dict[str, Any]]:
    pr = schema["pipeline_runs"]
    content_expr = "pr.content_id" if "content_id" in pr else "NULL AS content_id"
    batch_expr = "pr.batch_id" if "batch_id" in pr else "NULL AS batch_id"
    type_expr = "pr.content_type" if "content_type" in pr else "NULL AS content_type"
    if "generated_content" in schema and "content_id" in pr and {"id", "published_at"}.issubset(schema["generated_content"]):
        join = "LEFT JOIN generated_content gc ON gc.id = pr.content_id"
        published_expr = "gc.published_at AS published_at"
    else:
        join = ""
        published_expr = "NULL AS published_at"
    rows = conn.execute(
        f"""SELECT pr.id, {batch_expr}, {content_expr}, {type_expr}, pr.outcome, pr.created_at, {published_expr}
            FROM pipeline_runs pr
            {join}
            WHERE pr.created_at IS NOT NULL
              AND datetime(pr.created_at) >= datetime(?)
              AND datetime(pr.created_at) <= datetime(?)
            ORDER BY datetime(pr.created_at) DESC, pr.id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]], dict[str, list[str]]]:
    required = {"pipeline_runs": {"id", "created_at", "outcome"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {table: sorted(cols - schema[table]) for table, cols in required.items() if table in schema and cols - schema[table]}
    optional: dict[str, list[str]] = {}
    if "pipeline_runs" in schema:
        missing = sorted({"content_id", "batch_id", "content_type"} - schema["pipeline_runs"])
        if missing:
            optional["pipeline_runs"] = missing
    if "generated_content" in schema:
        missing = sorted({"id", "published_at"} - schema["generated_content"])
        if missing:
            optional["generated_content"] = missing
    else:
        optional["generated_content"] = ["table"]
    return missing_tables, missing_columns, optional


def _empty(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
    missing_optional_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "pipeline_gate_duration_outliers",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"run_count": 0, "outlier_count": 0, "returned_count": 0},
        "outlier_runs": [],
        "outcome_breakdowns": {},
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "missing_optional_columns": missing_optional_columns,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
