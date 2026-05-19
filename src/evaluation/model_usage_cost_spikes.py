"""Report recent model_usage cost and token spikes."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_HOURS = 24
DEFAULT_COST_THRESHOLD = 1.0
DEFAULT_TOKEN_THRESHOLD = 100_000
DEFAULT_LIMIT = 50
EXAMPLE_LIMIT = 5


def build_model_usage_cost_spikes_report(
    usage_rows: list[dict[str, Any]],
    *,
    window_hours: int = DEFAULT_WINDOW_HOURS,
    cost_threshold: float = DEFAULT_COST_THRESHOLD,
    token_threshold: int = DEFAULT_TOKEN_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic report from already-loaded model_usage rows."""
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if cost_threshold < 0:
        raise ValueError("cost_threshold must be non-negative")
    if token_threshold < 0:
        raise ValueError("token_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(hours=window_hours)
    groups: dict[tuple[str, str], dict[str, Any]] = defaultdict(_empty_group)

    for row in usage_rows:
        operation_name = _clean(row.get("operation_name"), "unknown")
        model_name = _clean(row.get("model_name"), "unknown")
        group = groups[(operation_name, model_name)]
        group["operation_name"] = operation_name
        group["model_name"] = model_name
        group["call_count"] += 1
        group["estimated_cost"] += _float(row.get("estimated_cost"))
        group["total_tokens"] += _int(row.get("total_tokens"))
        group["input_tokens"] += _int(row.get("input_tokens"))
        group["output_tokens"] += _int(row.get("output_tokens"))
        _add_example(group["examples"], row)

    spike_items = []
    bucket_summary = []
    for group in groups.values():
        cost_total = round(float(group["estimated_cost"]), 8)
        token_total = int(group["total_tokens"])
        triggered = []
        if cost_total >= cost_threshold:
            triggered.append("estimated_cost")
        if token_total >= token_threshold:
            triggered.append("total_tokens")
        bucket = {
            "operation_name": group["operation_name"],
            "model_name": group["model_name"],
            "call_count": group["call_count"],
            "estimated_cost": cost_total,
            "total_tokens": token_total,
            "input_tokens": int(group["input_tokens"]),
            "output_tokens": int(group["output_tokens"]),
        }
        bucket_summary.append(bucket)
        if triggered:
            spike_items.append({**bucket, "triggered_thresholds": triggered, "examples": group["examples"]})

    bucket_summary.sort(key=_bucket_sort_key)
    spike_items.sort(key=_spike_sort_key)
    shown_spikes = spike_items[:limit]
    return {
        "artifact_type": "model_usage_cost_spikes",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "window_hours": window_hours,
            "window_start": window_start.isoformat(),
            "window_end": generated_at.isoformat(),
            "cost_threshold": cost_threshold,
            "token_threshold": token_threshold,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "total_spikes": len(spike_items),
        "spike_items": shown_spikes,
        "bucket_summary": bucket_summary,
        "summary": {
            "rows_scanned": len(usage_rows),
            "bucket_count": len(bucket_summary),
            "shown_spikes": len(shown_spikes),
        },
    }


def build_model_usage_cost_spikes_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "model_usage" in schema else ("model_usage",)
    if missing_tables:
        return build_model_usage_cost_spikes_report([], missing_tables=missing_tables, **kwargs)

    window_hours = kwargs.get("window_hours", DEFAULT_WINDOW_HOURS)
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(hours=window_hours)
    rows = _load_usage_rows(conn, schema["model_usage"], cutoff=cutoff, window_end=now)
    return build_model_usage_cost_spikes_report(rows, missing_tables=missing_tables, **{**kwargs, "now": now})


def format_model_usage_cost_spikes_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_model_usage_cost_spikes_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Model Usage Cost Spikes",
        f"Generated: {report['generated_at']}",
        (
            f"Window: hours={thresholds['window_hours']} start={thresholds['window_start']} "
            f"end={thresholds['window_end']}"
        ),
        (
            f"Thresholds: estimated_cost>={thresholds['cost_threshold']:g} "
            f"total_tokens>={thresholds['token_threshold']}"
        ),
        (
            f"Totals: rows_scanned={summary['rows_scanned']} buckets={summary['bucket_count']} "
            f"spikes={report['total_spikes']} shown={summary['shown_spikes']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["spike_items"]:
        lines.append("No model usage cost or token spikes found.")
        return "\n".join(lines)

    lines.extend(["", "Spikes:"])
    for item in report["spike_items"]:
        examples = ", ".join(_format_example(example) for example in item["examples"]) or "-"
        lines.append(
            f"- operation={item['operation_name']} model={item['model_name']} "
            f"calls={item['call_count']} cost={item['estimated_cost']:g} "
            f"tokens={item['total_tokens']} triggers={','.join(item['triggered_thresholds'])}"
        )
        lines.append(f"  examples={examples}")
    return "\n".join(lines)


def _load_usage_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "mu", "usage_id", default="NULL"),
        _column_expr(columns, "created_at", "mu", "created_at", default="NULL"),
        _column_expr(columns, "operation_name", "mu", "operation_name", default="'unknown'"),
        _column_expr(columns, "model_name", "mu", "model_name", default="'unknown'"),
        _column_expr(columns, "input_tokens", "mu", "input_tokens", default="0"),
        _column_expr(columns, "output_tokens", "mu", "output_tokens", default="0"),
        _column_expr(columns, "total_tokens", "mu", "total_tokens", default="0"),
        _column_expr(columns, "estimated_cost", "mu", "estimated_cost", default="0"),
        _column_expr(columns, "content_id", "mu", "content_id", default="NULL"),
        _column_expr(columns, "pipeline_run_id", "mu", "pipeline_run_id", default="NULL"),
    ]
    where = ""
    params: list[Any] = []
    if "created_at" in columns:
        where = "WHERE datetime(mu.created_at) >= datetime(?) AND datetime(mu.created_at) <= datetime(?)"
        params = [_sqlite_ts(cutoff), _sqlite_ts(window_end)]
    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM model_usage mu
            {where}
            ORDER BY {_order_expr(columns)}""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _empty_group() -> dict[str, Any]:
    return {
        "operation_name": "unknown",
        "model_name": "unknown",
        "call_count": 0,
        "estimated_cost": 0.0,
        "total_tokens": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "examples": [],
    }


def _add_example(examples: list[dict[str, Any]], row: dict[str, Any]) -> None:
    if len(examples) >= EXAMPLE_LIMIT:
        return
    content_id = _int_or_none(row.get("content_id"))
    pipeline_run_id = _int_or_none(row.get("pipeline_run_id"))
    if content_id is None and pipeline_run_id is None:
        return
    examples.append(
        {
            "usage_id": _int_or_none(row.get("usage_id") if "usage_id" in row else row.get("id")),
            "content_id": content_id,
            "pipeline_run_id": pipeline_run_id,
            "estimated_cost": round(_float(row.get("estimated_cost")), 8),
            "total_tokens": _int(row.get("total_tokens")),
            "created_at": row.get("created_at"),
        }
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _order_expr(columns: set[str]) -> str:
    if "created_at" in columns and "id" in columns:
        return "datetime(mu.created_at) DESC, mu.id DESC"
    if "created_at" in columns:
        return "datetime(mu.created_at) DESC"
    return "mu.rowid DESC"


def _spike_sort_key(item: dict[str, Any]) -> tuple[float, int, str, str]:
    return (-float(item["estimated_cost"]), -int(item["total_tokens"]), item["operation_name"], item["model_name"])


def _bucket_sort_key(item: dict[str, Any]) -> tuple[float, int, str, str]:
    return _spike_sort_key(item)


def _format_example(example: dict[str, Any]) -> str:
    parts = []
    if example.get("content_id") is not None:
        parts.append(f"content_id={example['content_id']}")
    if example.get("pipeline_run_id") is not None:
        parts.append(f"pipeline_run_id={example['pipeline_run_id']}")
    return "/".join(parts)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _clean(value: Any, fallback: str) -> str:
    text = "" if value is None else str(value).strip()
    return text or fallback


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
