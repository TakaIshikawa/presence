"""Report prompt version score regressions against prior stable versions."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_MIN_SAMPLES = 3
DEFAULT_REGRESSION_THRESHOLD = 0.5
METRICS = ("quality_score", "authenticity_score", "engagement_score")


def build_prompt_version_regression_drift_report(
    eval_rows: list[dict[str, Any]],
    prompt_rows: list[dict[str, Any]] | None = None,
    *,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    regression_threshold: float = DEFAULT_REGRESSION_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic prompt-version regression drift report."""
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")
    if regression_threshold <= 0:
        raise ValueError("regression_threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    prompts = [_normalize_prompt(row) for row in (prompt_rows or [])]
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    missing_metadata_count = 0

    for raw in eval_rows:
        row = _normalize_eval(raw, prompts)
        if not row["prompt_family"] or not row["prompt_version"]:
            missing_metadata_count += 1
            continue
        key = (row["prompt_family"], row["prompt_version"])
        group = groups.setdefault(
            key,
            {
                "prompt_family": row["prompt_family"],
                "prompt_version": row["prompt_version"],
                "version_sort": row["version_sort"],
                "sample_count": 0,
                "metric_values": {metric: [] for metric in METRICS},
                "first_evaluated_at": None,
                "latest_evaluated_at": None,
            },
        )
        group["sample_count"] += 1
        group["version_sort"] = min(group["version_sort"], row["version_sort"])
        group["first_evaluated_at"] = _min_iso(group["first_evaluated_at"], row["evaluated_at_dt"])
        group["latest_evaluated_at"] = _max_iso(group["latest_evaluated_at"], row["evaluated_at_dt"])
        for metric in METRICS:
            value = _float_or_none(row.get(metric))
            if value is not None:
                group["metric_values"][metric].append(value)

    summaries = [_summarize_group(group) for group in groups.values()]
    by_family: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for summary in summaries:
        by_family[summary["prompt_family"]].append(summary)

    findings: list[dict[str, Any]] = []
    skipped_counts: Counter[str] = Counter()
    for family, rows in sorted(by_family.items()):
        rows.sort(key=lambda row: (row["version_sort"], row["prompt_version"]))
        stable: list[dict[str, Any]] = []
        for current in rows:
            baseline = stable[-1] if stable else None
            if baseline is None:
                skipped_counts["no_baseline"] += 1
            elif current["sample_count"] < min_samples:
                skipped_counts["insufficient_current_samples"] += 1
            else:
                deltas = _metric_deltas(current, baseline)
                regressed = {
                    metric: delta
                    for metric, delta in deltas.items()
                    if delta is not None and delta <= -regression_threshold
                }
                if regressed:
                    findings.append(
                        {
                            "prompt_family": family,
                            "current_version": current["prompt_version"],
                            "baseline_version": baseline["prompt_version"],
                            "metric_deltas": deltas,
                            "current_scores": current["average_scores"],
                            "baseline_scores": baseline["average_scores"],
                            "current_sample_count": current["sample_count"],
                            "baseline_sample_count": baseline["sample_count"],
                            "severity": _severity(regressed),
                            "regressed_metrics": sorted(regressed),
                        }
                    )
            if current["sample_count"] >= min_samples:
                stable.append(current)
            else:
                skipped_counts["unstable_version"] += 1

    findings.sort(key=lambda item: (_severity_rank(item["severity"]), item["prompt_family"], item["current_version"]))
    shown = findings[:limit]
    return {
        "artifact_type": "prompt_version_regression_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "min_samples": min_samples,
            "regression_threshold": regression_threshold,
            "limit": limit,
        },
        "summary": {
            "eval_row_count": len(eval_rows),
            "version_group_count": len(summaries),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "missing_prompt_version_metadata_count": missing_metadata_count,
            "skipped_counts": dict(sorted(skipped_counts.items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "version_groups": sorted(
            summaries,
            key=lambda row: (row["prompt_family"], row["version_sort"], row["prompt_version"]),
        ),
        "findings": shown,
    }


def build_prompt_version_regression_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "eval_results" in schema else ["eval_results"]
    missing_columns: dict[str, list[str]] = {}
    if "eval_results" in schema and not _has_any(schema["eval_results"], ("created_at", "evaluated_at")):
        missing_columns["eval_results"] = ["created_at|evaluated_at"]
    return build_prompt_version_regression_drift_report(
        _load_eval_rows(conn, schema) if "eval_results" in schema else [],
        _load_prompt_rows(conn, schema) if "prompt_versions" in schema else [],
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_prompt_version_regression_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_prompt_version_regression_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Prompt Version Regression Drift",
        f"Generated: {report['generated_at']}",
        (
            "Thresholds: "
            f"min_samples={report['thresholds']['min_samples']} "
            f"regression_threshold={report['thresholds']['regression_threshold']} "
            f"limit={report['thresholds']['limit']}"
        ),
        (
            f"Totals: eval_rows={summary['eval_row_count']} groups={summary['version_group_count']} "
            f"findings={summary['finding_count']} missing_metadata={summary['missing_prompt_version_metadata_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{t}.{c}" for t, cols in report["missing_columns"].items() for c in cols))
    if not report["findings"]:
        lines.append("No prompt version regression drift found.")
        return "\n".join(lines)
    lines.append("Findings:")
    for finding in report["findings"]:
        deltas = ", ".join(f"{metric}={value:+.2f}" for metric, value in finding["metric_deltas"].items() if value is not None)
        lines.append(
            f"  - {finding['severity']} family={finding['prompt_family']} "
            f"current={finding['current_version']} baseline={finding['baseline_version']} "
            f"samples={finding['current_sample_count']}/{finding['baseline_sample_count']} deltas={deltas}"
        )
    return "\n".join(lines)


def _load_eval_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["eval_results"]
    select = [
        _expr(cols, "id", fallback="rowid") + " AS eval_result_id",
        _expr(cols, "prompt_family", "prompt_type", "content_type", fallback="NULL") + " AS prompt_family",
        _expr(cols, "prompt_version", "version", fallback="NULL") + " AS prompt_version",
        _expr(cols, "prompt_version_id", fallback="NULL") + " AS prompt_version_id",
        _expr(cols, "prompt_hash", fallback="NULL") + " AS prompt_hash",
        _expr(cols, "quality_score", "content_quality_score", "final_score", "eval_score", "score", fallback="NULL") + " AS quality_score",
        _expr(cols, "authenticity_score", "auth_score", fallback="NULL") + " AS authenticity_score",
        _expr(cols, "engagement_score", "actual_engagement_score", fallback="NULL") + " AS engagement_score",
        _expr(cols, "created_at", "evaluated_at", fallback="NULL") + " AS evaluated_at",
    ]
    order = _expr(cols, "created_at", "evaluated_at", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM eval_results ORDER BY {order}, rowid").fetchall()]


def _load_prompt_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["prompt_versions"]
    select = [
        _expr(cols, "id", "prompt_version_id", fallback="NULL") + " AS prompt_version_id",
        _expr(cols, "prompt_type", "prompt_family", fallback="NULL") + " AS prompt_family",
        _expr(cols, "prompt_version", "version", fallback="NULL") + " AS prompt_version",
        _expr(cols, "prompt_hash", fallback="NULL") + " AS prompt_hash",
        _expr(cols, "created_at", fallback="NULL") + " AS created_at",
    ]
    order = _expr(cols, "prompt_type", "prompt_family", fallback="''") + ", " + _expr(cols, "version", "prompt_version", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM prompt_versions ORDER BY {order}, rowid").fetchall()]


def _normalize_prompt(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "prompt_version_id": _clean(row.get("prompt_version_id")),
        "prompt_family": _clean(row.get("prompt_family")),
        "prompt_version": _clean(row.get("prompt_version")),
        "prompt_hash": _clean(row.get("prompt_hash")),
        "created_at_dt": _parse_time(row.get("created_at")),
    }


def _normalize_eval(row: dict[str, Any], prompts: list[dict[str, Any]]) -> dict[str, Any]:
    prompt = _resolve_prompt(row, prompts)
    family = _clean(row.get("prompt_family")) or prompt.get("prompt_family", "")
    version = _clean(row.get("prompt_version")) or prompt.get("prompt_version", "")
    return {
        **row,
        "prompt_family": family,
        "prompt_version": version,
        "version_sort": _version_sort(version),
        "evaluated_at_dt": _parse_time(row.get("evaluated_at")),
    }


def _resolve_prompt(row: dict[str, Any], prompts: list[dict[str, Any]]) -> dict[str, Any]:
    prompt_id = _clean(row.get("prompt_version_id"))
    prompt_hash = _clean(row.get("prompt_hash"))
    family = _clean(row.get("prompt_family"))
    evaluated_at = _parse_time(row.get("evaluated_at"))
    if prompt_id:
        match = next((prompt for prompt in prompts if prompt["prompt_version_id"] == prompt_id), None)
        if match:
            return match
    if prompt_hash:
        match = next((prompt for prompt in prompts if prompt["prompt_hash"] == prompt_hash and (not family or prompt["prompt_family"] == family)), None)
        if match:
            return match
    if family and evaluated_at:
        candidates = [
            prompt
            for prompt in prompts
            if prompt["prompt_family"] == family
            and prompt["created_at_dt"] is not None
            and prompt["created_at_dt"] <= evaluated_at
        ]
        if candidates:
            return sorted(candidates, key=lambda prompt: (prompt["created_at_dt"], _version_sort(prompt["prompt_version"])))[-1]
    return {}


def _summarize_group(group: dict[str, Any]) -> dict[str, Any]:
    averages = {
        metric: _round(sum(values) / len(values)) if values else None
        for metric, values in group["metric_values"].items()
    }
    return {
        "prompt_family": group["prompt_family"],
        "prompt_version": group["prompt_version"],
        "version_sort": group["version_sort"],
        "sample_count": group["sample_count"],
        "metric_sample_counts": {metric: len(values) for metric, values in group["metric_values"].items()},
        "average_scores": averages,
        "first_evaluated_at": group["first_evaluated_at"],
        "latest_evaluated_at": group["latest_evaluated_at"],
    }


def _metric_deltas(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, float | None]:
    deltas = {}
    for metric in METRICS:
        current_value = current["average_scores"].get(metric)
        baseline_value = baseline["average_scores"].get(metric)
        deltas[metric] = _round(current_value - baseline_value) if current_value is not None and baseline_value is not None else None
    return deltas


def _severity(regressed: dict[str, float]) -> str:
    worst = min(regressed.values())
    if worst <= -2.0 or len([delta for delta in regressed.values() if delta <= -1.0]) >= 2:
        return "critical"
    if worst <= -1.0:
        return "high"
    return "medium"


def _severity_rank(severity: str) -> int:
    return {"critical": 0, "high": 1, "medium": 2}.get(severity, 3)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _expr(columns: set[str], *names: str, fallback: str) -> str:
    found = [name for name in names if name in columns]
    if not found:
        return fallback
    return found[0] if len(found) == 1 else "COALESCE(" + ", ".join(found) + ")"


def _has_any(columns: set[str], names: tuple[str, ...]) -> bool:
    return any(name in columns for name in names)


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _min_iso(current: str | None, candidate: datetime | None) -> str | None:
    if candidate is None:
        return current
    candidate_iso = candidate.isoformat()
    return candidate_iso if current is None or candidate_iso < current else current


def _max_iso(current: str | None, candidate: datetime | None) -> str | None:
    if candidate is None:
        return current
    candidate_iso = candidate.isoformat()
    return candidate_iso if current is None or candidate_iso > current else current


def _version_sort(version: str) -> tuple[int, str]:
    match = re.search(r"\d+", version or "")
    return (int(match.group()) if match else 10**9, version or "")


def _float_or_none(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _round(value: float | None, digits: int = 4) -> float | None:
    return round(value, digits) if value is not None else None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
