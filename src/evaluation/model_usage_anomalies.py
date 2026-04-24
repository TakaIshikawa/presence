"""Model usage anomaly diagnostics grouped by operation and model."""

from __future__ import annotations

import math
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


@dataclass(frozen=True)
class ModelUsageAnomalyConfig:
    days: int = 7
    min_samples: int = 3
    threshold: float = 2.0
    operations: tuple[str, ...] = ()


def build_model_usage_anomaly_report(
    db_or_conn: Any,
    days: int = 7,
    min_samples: int = 3,
    threshold: float = 2.0,
    operations: Iterable[str] | None = None,
    now: datetime | None = None,
) -> dict:
    """Compare recent model usage with the prior window of the same length."""
    config = ModelUsageAnomalyConfig(
        days=max(1, int(days)),
        min_samples=max(1, int(min_samples)),
        threshold=max(0.0, float(threshold)),
        operations=tuple(op for op in (operations or ()) if op),
    )
    now = _as_utc(now or datetime.now(timezone.utc))
    current_start = now - timedelta(days=config.days)
    baseline_start = current_start - timedelta(days=config.days)

    conn = _connection(db_or_conn)
    current, baseline = _load_usage_windows(
        conn,
        baseline_start=baseline_start,
        current_start=current_start,
        now=now,
        operations=config.operations,
    )

    keys = sorted(set(current) | set(baseline))
    rows = [
        _build_row(key, current.get(key, []), baseline.get(key, []), config)
        for key in keys
        if key in current
    ]
    anomalies = [row for row in rows if row["status"] == "anomalous"]
    warnings = [
        _warning_for_row(row)
        for row in anomalies
    ]

    return {
        "status": "warning" if anomalies else "ok",
        "generated_at": now.isoformat(),
        "window": {
            "days": config.days,
            "current_start": current_start.isoformat(),
            "current_end": now.isoformat(),
            "baseline_start": baseline_start.isoformat(),
            "baseline_end": current_start.isoformat(),
        },
        "config": asdict(config),
        "rows": rows,
        "warnings": warnings,
    }


def format_model_usage_anomaly_report(report: dict) -> str:
    """Format anomaly diagnostics for terminal output."""
    rows = report.get("rows") or []
    days = (report.get("window") or {}).get("days")
    if not rows:
        suffix = f" in last {days} days" if days is not None else ""
        return f"No model usage found{suffix}."

    lines = [
        "=" * 100,
        f"MODEL USAGE ANOMALIES (last {days} days)",
        "=" * 100,
        f"Status: {str(report.get('status') or 'ok').upper()}",
        "",
        (
            f"{'Severity':9s} {'Status':18s} {'Operation':30s} {'Model':18s} "
            f"{'Samples':>7s} {'Tok Avg':>9s} {'Base Tok':>9s} {'Tok x':>7s} "
            f"{'Cost Avg':>10s} {'Base Cost':>10s} {'Cost x':>7s}"
        ),
        (
            f"{'-' * 9:9s} {'-' * 18:18s} {'-' * 30:30s} {'-' * 18:18s} "
            f"{'-' * 7:>7s} {'-' * 9:>9s} {'-' * 9:>9s} {'-' * 7:>7s} "
            f"{'-' * 10:>10s} {'-' * 10:>10s} {'-' * 7:>7s}"
        ),
    ]
    for row in rows:
        lines.append(
            f"{str(row['severity']):9s} "
            f"{str(row['status']):18s} "
            f"{str(row['operation_name'])[:30]:30s} "
            f"{str(row['model_name'])[:18]:18s} "
            f"{int(row['sample_count']):7d} "
            f"{float(row['current_avg_tokens']):9.1f} "
            f"{_fmt_optional_float(row['baseline_avg_tokens'], 1):>9s} "
            f"{_fmt_optional_float(row['token_ratio'], 2):>7s} "
            f"{_fmt_cost(row['current_avg_cost']):>10s} "
            f"{_fmt_optional_cost(row['baseline_avg_cost']):>10s} "
            f"{_fmt_optional_float(row['cost_ratio'], 2):>7s}"
        )

    warnings = report.get("warnings") or []
    if warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"  - {warning}" for warning in warnings)
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _load_usage_windows(
    conn: sqlite3.Connection,
    baseline_start: datetime,
    current_start: datetime,
    now: datetime,
    operations: tuple[str, ...],
) -> tuple[dict[tuple[str, str], list[dict]], dict[tuple[str, str], list[dict]]]:
    where = ["created_at >= ?", "created_at < ?"]
    params: list[Any] = [_sqlite_ts(baseline_start), _sqlite_ts(now)]
    if operations:
        placeholders = ", ".join("?" for _ in operations)
        where.append(f"operation_name IN ({placeholders})")
        params.extend(operations)

    cursor = conn.execute(
        f"""SELECT operation_name, model_name, total_tokens, estimated_cost, created_at
            FROM model_usage
            WHERE {' AND '.join(where)}
            ORDER BY operation_name, model_name, created_at""",
        params,
    )
    current: dict[tuple[str, str], list[dict]] = defaultdict(list)
    baseline: dict[tuple[str, str], list[dict]] = defaultdict(list)
    current_start_text = _sqlite_ts(current_start)
    for db_row in cursor.fetchall():
        row = dict(db_row)
        key = (row["operation_name"], row["model_name"])
        if str(row["created_at"]).replace("T", " ")[:19] >= current_start_text:
            current[key].append(row)
        else:
            baseline[key].append(row)
    return current, baseline


def _build_row(
    key: tuple[str, str],
    current_rows: list[dict],
    baseline_rows: list[dict],
    config: ModelUsageAnomalyConfig,
) -> dict:
    current_tokens = [float(row["total_tokens"] or 0) for row in current_rows]
    current_costs = [float(row["estimated_cost"] or 0) for row in current_rows]
    baseline_tokens = [float(row["total_tokens"] or 0) for row in baseline_rows]
    baseline_costs = [float(row["estimated_cost"] or 0) for row in baseline_rows]

    token_stats = _metric_stats(current_tokens, baseline_tokens)
    cost_stats = _metric_stats(current_costs, baseline_costs)
    sample_count = len(current_rows)
    baseline_count = len(baseline_rows)

    status = "normal"
    severity = "ok"
    if sample_count < config.min_samples:
        status = "insufficient_data"
        severity = "info"
    elif baseline_count < config.min_samples:
        status = "no_baseline"
        severity = "info"
    elif _metric_is_anomalous(token_stats, config.threshold) or _metric_is_anomalous(
        cost_stats, config.threshold
    ):
        status = "anomalous"
        severity = _severity(token_stats, cost_stats, config.threshold)

    return {
        "operation_name": key[0],
        "model_name": key[1],
        "status": status,
        "severity": severity,
        "sample_count": sample_count,
        "baseline_sample_count": baseline_count,
        "current_avg_tokens": round(token_stats["current_avg"], 6),
        "baseline_avg_tokens": _round_optional(token_stats["baseline_avg"]),
        "token_ratio": _round_optional(token_stats["ratio"]),
        "token_z_score": _round_optional(token_stats["z_score"]),
        "current_avg_cost": round(cost_stats["current_avg"], 8),
        "baseline_avg_cost": _round_optional(cost_stats["baseline_avg"], 8),
        "cost_ratio": _round_optional(cost_stats["ratio"]),
        "cost_z_score": _round_optional(cost_stats["z_score"]),
    }


def _metric_stats(current: list[float], baseline: list[float]) -> dict:
    current_avg = _avg(current)
    baseline_avg = _avg(baseline) if baseline else None
    ratio = None
    z_score = None
    if baseline_avg is not None:
        if baseline_avg > 0:
            ratio = current_avg / baseline_avg
        stdev = _sample_stdev(baseline)
        if stdev > 0:
            z_score = (current_avg - baseline_avg) / stdev
    return {
        "current_avg": current_avg,
        "baseline_avg": baseline_avg,
        "ratio": ratio,
        "z_score": z_score,
    }


def _metric_is_anomalous(stats: dict, threshold: float) -> bool:
    ratio = stats.get("ratio")
    z_score = stats.get("z_score")
    return (
        ratio is not None
        and ratio >= threshold
        or z_score is not None
        and z_score >= threshold
    )


def _severity(token_stats: dict, cost_stats: dict, threshold: float) -> str:
    highest = max(
        value
        for value in [
            token_stats.get("ratio"),
            token_stats.get("z_score"),
            cost_stats.get("ratio"),
            cost_stats.get("z_score"),
            0,
        ]
        if value is not None
    )
    return "critical" if highest >= threshold * 2 else "warning"


def _warning_for_row(row: dict) -> str:
    token_ratio = row.get("token_ratio")
    cost_ratio = row.get("cost_ratio")
    parts = []
    if token_ratio is not None:
        parts.append(f"tokens {token_ratio:.2f}x baseline")
    if cost_ratio is not None:
        parts.append(f"cost {cost_ratio:.2f}x baseline")
    metrics = ", ".join(parts) or "usage above baseline"
    return f"{row['operation_name']} on {row['model_name']}: {metrics}"


def _avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _sample_stdev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = _avg(values)
    variance = sum((value - avg) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _round_optional(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(value, digits)


def _fmt_cost(value: float | None) -> str:
    return f"${float(value or 0):.4f}"


def _fmt_optional_cost(value: float | None) -> str:
    return "n/a" if value is None else _fmt_cost(value)


def _fmt_optional_float(value: float | None, digits: int) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"
