"""Summarize token and cost efficiency by prompt version."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 20
DEFAULT_EXPENSIVE_COST = 1.0


def build_prompt_version_cost_efficiency_report(
    prompt_versions: list[dict[str, Any]],
    model_usage_rows: list[dict[str, Any]],
    generated_content_rows: list[dict[str, Any]],
    *,
    expensive_cost: float = DEFAULT_EXPENSIVE_COST,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if expensive_cost < 0:
        raise ValueError("expensive_cost must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    version_names = {_clean(_first(row, "id", "prompt_version_id")): _clean(_first(row, "version", "name", "label")) for row in prompt_versions}
    content_by_id = {_clean(_first(row, "id", "content_id", "generated_content_id")): row for row in generated_content_rows}
    runs = []

    for row in model_usage_rows:
        content = content_by_id.get(_clean(_first(row, "generated_content_id", "content_id"))) or {}
        runs.append(_run(row, content, version_names))

    usage_content_ids = {_clean(run["generated_content_id"]) for run in runs if run["generated_content_id"] is not None}
    for row in generated_content_rows:
        content_id = _clean(_first(row, "id", "content_id", "generated_content_id"))
        has_direct_usage = any(_first(row, key) is not None for key in ("cost_usd", "cost", "total_cost", "input_tokens", "output_tokens", "total_tokens", "tokens"))
        if has_direct_usage and content_id not in usage_content_ids:
            runs.append(_run(row, row, version_names))

    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for run in runs:
        groups[run["prompt_version"]].append(run)

    summaries = []
    for version, version_runs in groups.items():
        content_ids = {run["generated_content_id"] for run in version_runs if run["generated_content_id"] is not None}
        total_cost = round(sum(run["cost_usd"] for run in version_runs), 6)
        total_input = sum(run["input_tokens"] for run in version_runs)
        total_output = sum(run["output_tokens"] for run in version_runs)
        total_tokens = sum(run["total_tokens"] for run in version_runs)
        item_count = len(content_ids) or len(version_runs)
        summaries.append(
            {
                "prompt_version": version,
                "run_count": len(version_runs),
                "generated_content_count": item_count,
                "total_cost_usd": total_cost,
                "average_cost_per_generated_content_item": round(total_cost / item_count, 6) if item_count else 0.0,
                "token_mix": {
                    "input_tokens": total_input,
                    "output_tokens": total_output,
                    "total_tokens": total_tokens,
                    "input_share": _share(total_input, total_tokens),
                    "output_share": _share(total_output, total_tokens),
                },
            }
        )
    summaries.sort(key=lambda item: (-item["total_cost_usd"], item["prompt_version"]))

    expensive = [
        run
        for run in sorted(runs, key=lambda item: (-item["cost_usd"], item["prompt_version"], str(item["generated_content_id"] or "")))
        if run["cost_usd"] >= expensive_cost
    ][:limit]
    return {
        "artifact_type": "prompt_version_cost_efficiency",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"expensive_cost": expensive_cost, "limit": limit},
        "version_summary": summaries,
        "expensive_examples": expensive,
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_prompt_version_cost_efficiency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    return build_prompt_version_cost_efficiency_report(
        _load(conn, "prompt_versions", schema.get("prompt_versions", set())) if "prompt_versions" in schema else [],
        _load(conn, "model_usage", schema.get("model_usage", set())) if "model_usage" in schema else [],
        _load(conn, "generated_content", schema.get("generated_content", set())) if "generated_content" in schema else [],
        missing_schema=missing_schema,
        **kwargs,
    )


def format_prompt_version_cost_efficiency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_version_cost_efficiency_text(report: dict[str, Any]) -> str:
    lines = ["Prompt Version Cost Efficiency", f"Generated: {report['generated_at']}"]
    if report["missing_schema"]["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_schema"]["missing_tables"]))
    lines.append(f"Versions: {len(report['version_summary'])}")
    for item in report["version_summary"]:
        lines.append(
            f"  - version={item['prompt_version']} cost={item['total_cost_usd']} "
            f"items={item['generated_content_count']} avg={item['average_cost_per_generated_content_item']}"
        )
    return "\n".join(lines)


def _run(row: dict[str, Any], content: dict[str, Any], version_names: dict[str, str]) -> dict[str, Any]:
    version_id = _clean(_first(row, "prompt_version_id", "version_id")) or _clean(_first(content, "prompt_version_id", "version_id"))
    version = (
        _clean(_first(row, "prompt_version", "prompt_version_name", "version"))
        or _clean(_first(content, "prompt_version", "prompt_version_name", "version"))
        or version_names.get(version_id, "")
        or "unknown"
    )
    input_tokens = _int(_first(row, "input_tokens", "prompt_tokens"))
    output_tokens = _int(_first(row, "output_tokens", "completion_tokens"))
    total_tokens = _int(_first(row, "total_tokens", "tokens")) or input_tokens + output_tokens
    return {
        "usage_id": _first(row, "usage_id", "id", "model_usage_id"),
        "generated_content_id": _first(row, "generated_content_id", "content_id") or _first(content, "id", "content_id"),
        "prompt_version": version,
        "model": _clean(_first(row, "model", "model_name")) or None,
        "cost_usd": _float(_first(row, "cost_usd", "cost", "total_cost")),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "created_at": _first(row, "created_at", "generated_at") or _first(content, "created_at", "generated_at"),
    }


def _load(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    select = ", ".join(f"{column} AS {column}" for column in sorted(columns)) or "rowid"
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {select} FROM {table} ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    missing_tables = [table for table in ("generated_content",) if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    if "model_usage" not in schema and "generated_content" in schema:
        direct = {"cost_usd", "cost", "total_cost", "input_tokens", "output_tokens", "total_tokens", "tokens"}
        if not direct & schema["generated_content"]:
            missing_columns["generated_content"] = sorted(direct)
    return {"missing_tables": missing_tables, "missing_columns": missing_columns}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _share(part: int, total: int) -> float:
    return round(part / total, 6) if total else 0.0


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
