"""Report model usage rows near or beyond context windows."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from ._report_utils import clean, connection, expr, json_dumps, now_iso, positive, schema, to_int


ARTIFACT_TYPE = "model_usage_context_window_pressure"
DEFAULT_LIMIT = 50
DEFAULT_PRESSURE_THRESHOLD = 0.8
FALLBACK_LIMITS = {"gpt-4o-mini": 128000, "gpt-4o": 128000, "gpt-4": 8192, "gpt-3.5-turbo": 16385}


def build_model_usage_context_window_pressure_report(rows: list[dict[str, Any]], *, pressure_threshold: float = DEFAULT_PRESSURE_THRESHOLD, limit: int = DEFAULT_LIMIT, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None, now: Any = None) -> dict[str, Any]:
    positive("limit", limit)
    if not 0 < pressure_threshold <= 1: raise ValueError("pressure_threshold must be between 0 and 1")
    findings = []; by_op: dict[str, list[float]] = defaultdict(list)
    for row in rows:
        prompt = to_int(row.get("prompt_tokens")) or 0; completion = to_int(row.get("completion_tokens")) or 0; total = to_int(row.get("total_tokens")) or prompt + completion
        model = clean(row.get("model")); ctx = to_int(row.get("context_limit")) or FALLBACK_LIMITS.get(model)
        pressure = round(total / ctx, 4) if ctx else None; issues = []
        if not to_int(row.get("context_limit")): issues.append("missing_context_limit")
        if ctx and total > ctx: issues.append("over_context_limit")
        elif pressure is not None and pressure >= pressure_threshold: issues.append("high_context_pressure")
        if completion <= max(1, int(prompt * 0.02)) and pressure is not None and pressure >= pressure_threshold: issues.append("completion_truncation_risk")
        op = clean(row.get("operation"), "unknown"); by_op[op].append(pressure or 0)
        for issue in issues:
            findings.append({"usage_id": clean(row.get("usage_id")), "operation": op, "model": model or None, "issue_type": issue, "prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total, "context_limit": ctx, "pressure": pressure})
    for op, vals in by_op.items():
        if len(vals) >= 3 and vals[-1] > vals[0] and vals[-1] >= pressure_threshold:
            findings.append({"usage_id": None, "operation": op, "model": None, "issue_type": "rising_operation_pressure", "prompt_tokens": None, "completion_tokens": None, "total_tokens": None, "context_limit": None, "pressure": round(vals[-1], 4)})
    findings.sort(key=lambda r: (r["issue_type"], -(r["pressure"] or 0), r["operation"], r["usage_id"] or ""))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": now_iso(now), "thresholds": {"pressure_threshold": pressure_threshold, "limit": limit}, "summary": {"usage_count": len(rows), "finding_count": len(findings), "shown_count": len(shown)}, "findings": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}}


def build_model_usage_context_window_pressure_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); s = schema(conn); table = "model_usage"; missing_tables = [] if table in s else [table]; missing_columns = {}; rows = []
    if table in s:
        cols = s[table]; req = {"prompt_tokens"}
        if not req.issubset(cols): missing_columns[table] = sorted(req - cols)
        if not missing_columns:
            select = [
                expr(cols, "id", "usage_id", default="rowid", out="usage_id"),
                expr(cols, "operation", "task", default="'unknown'", out="operation"),
                expr(cols, "model", default="NULL", out="model"),
                "prompt_tokens",
                expr(cols, "completion_tokens", default="0", out="completion_tokens"),
                expr(cols, "total_tokens", default="NULL", out="total_tokens"),
                expr(cols, "context_limit", "context_window", "max_context_tokens", default="NULL", out="context_limit"),
            ]
            rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY rowid")]
    return build_model_usage_context_window_pressure_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_model_usage_context_window_pressure_json(report: dict[str, Any]) -> str: return json_dumps(report)


def format_model_usage_context_window_pressure_text(report: dict[str, Any]) -> str:
    lines = ["Model Usage Context Window Pressure", f"Generated: {report['generated_at']}", f"Totals: usage={report['summary']['usage_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}"]
    if report["missing_tables"]: lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]: lines.append("No model usage context window pressure found."); return "\n".join(lines)
    for r in report["findings"]: lines.append(f"- usage_id={r['usage_id'] or '-'} op={r['operation']} type={r['issue_type']} pressure={r['pressure']}")
    return "\n".join(lines)
