"""Report input/output token ratio drift by model usage operation."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_CURRENT_DAYS = 7
DEFAULT_BASELINE_DAYS = 28
DEFAULT_MIN_BASELINE_ROWS = 2
DEFAULT_DRIFT_THRESHOLD = 0.5
DEFAULT_LIMIT = 100
ISSUE_TYPES = ("ratio_drift", "insufficient_baseline", "zero_output_tokens")


def build_model_usage_token_ratio_drift_report(
    rows: list[dict[str, Any]],
    *,
    current_days: int = DEFAULT_CURRENT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_baseline_rows: int = DEFAULT_MIN_BASELINE_ROWS,
    drift_threshold: float = DEFAULT_DRIFT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if current_days <= 0 or baseline_days <= 0:
        raise ValueError("window days must be positive")
    if min_baseline_rows <= 0:
        raise ValueError("min_baseline_rows must be positive")
    if drift_threshold < 0:
        raise ValueError("drift_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _report_now(now, rows, ("created_at",))
    current_start = generated_at - timedelta(days=current_days)
    baseline_start = current_start - timedelta(days=baseline_days)
    grouped: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"current": [], "baseline": []})
    for row in rows:
        created_at = _parse_time(row.get("created_at")) or generated_at
        if created_at >= current_start:
            bucket = "current"
        elif created_at >= baseline_start:
            bucket = "baseline"
        else:
            continue
        key = (_clean(row.get("operation_name")) or "unknown", _clean(row.get("model_name")) or "unknown")
        grouped[key][bucket].append(row)

    issues: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for (operation_name, model_name), buckets in grouped.items():
        current = _stats(buckets["current"])
        baseline = _stats(buckets["baseline"])
        if current["row_count"] == 0:
            continue
        base = {
            "operation_name": operation_name,
            "model_name": model_name,
            "current_row_count": current["row_count"],
            "baseline_row_count": baseline["row_count"],
            "current_input_tokens": current["input_tokens"],
            "current_output_tokens": current["output_tokens"],
            "baseline_input_tokens": baseline["input_tokens"],
            "baseline_output_tokens": baseline["output_tokens"],
            "current_ratio": current["ratio"],
            "baseline_ratio": baseline["ratio"],
            "ratio_delta": None if current["ratio"] is None or baseline["ratio"] is None else round(current["ratio"] - baseline["ratio"], 6),
        }
        if current["output_tokens"] == 0:
            issues.append({**base, "issue_type": "zero_output_tokens"})
            counts["zero_output_tokens"] += 1
        if baseline["row_count"] < min_baseline_rows:
            issues.append({**base, "issue_type": "insufficient_baseline"})
            counts["insufficient_baseline"] += 1
        elif current["ratio"] is not None and baseline["ratio"] is not None and abs(current["ratio"] - baseline["ratio"]) >= drift_threshold:
            issues.append({**base, "issue_type": "ratio_drift"})
            counts["ratio_drift"] += 1

    issues.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), item["operation_name"], item["model_name"]))
    shown = issues[:limit]
    return {
        "artifact_type": "model_usage_token_ratio_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "current_days": current_days,
            "baseline_days": baseline_days,
            "min_baseline_rows": min_baseline_rows,
            "drift_threshold": drift_threshold,
            "limit": limit,
        },
        "summary": {
            "operation_model_count": len(grouped),
            "row_count": len(rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "issue_items": shown,
        "empty_state": {"is_empty": not issues, "message": "No model usage token ratio drift issues found." if not issues else None},
    }


def build_model_usage_token_ratio_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "model_usage" not in schema:
        return build_model_usage_token_ratio_drift_report([], missing_tables=["model_usage"], **kwargs)
    return build_model_usage_token_ratio_drift_report(_load_rows(conn, schema["model_usage"]), **kwargs)


def format_model_usage_token_ratio_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_model_usage_token_ratio_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Model Usage Token Ratio Drift",
        f"Generated: {report['generated_at']}",
        f"Thresholds: current_days={report['thresholds']['current_days']} baseline_days={report['thresholds']['baseline_days']} drift_threshold={report['thresholds']['drift_threshold']} limit={report['thresholds']['limit']}",
        f"Totals: operation_models={summary['operation_model_count']} rows={summary['row_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(f"  - {item['issue_type']} operation={item['operation_name']} model={item['model_name']} current_ratio={item['current_ratio']} baseline_ratio={item['baseline_ratio']}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "operation_name", fallback="'unknown'") + " AS operation_name",
        _expr(columns, "model_name", fallback="'unknown'") + " AS model_name",
        _expr(columns, "input_tokens", "prompt_tokens", fallback="0") + " AS input_tokens",
        _expr(columns, "output_tokens", "completion_tokens", fallback="0") + " AS output_tokens",
        _expr(columns, "created_at", fallback="NULL") + " AS created_at",
    ]
    order = _expr(columns, "created_at", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM model_usage ORDER BY {order} ASC").fetchall()]


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    input_tokens = sum(_to_int(row.get("input_tokens")) for row in rows)
    output_tokens = sum(_to_int(row.get("output_tokens")) for row in rows)
    return {
        "row_count": len(rows),
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "ratio": None if output_tokens == 0 else round(input_tokens / output_tokens, 6),
    }


def _expr(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _report_now(now: datetime | None, rows: list[dict[str, Any]], timestamp_keys: tuple[str, ...]) -> datetime:
    if now is not None:
        return _utc(now)
    observed = [
        parsed
        for row in rows
        for key in timestamp_keys
        if (parsed := _parse_time(row.get(key))) is not None
    ]
    return max(observed) if observed else datetime.now(timezone.utc)


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
