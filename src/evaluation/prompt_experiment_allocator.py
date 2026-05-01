"""Allocate future generation runs across prompt versions."""

from __future__ import annotations

import json
import math
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_TOTAL_RUNS = 20
DEFAULT_EXPLORE_PERCENT = 25.0
DEFAULT_MIN_RUNS = 3


@dataclass(frozen=True)
class PromptExperimentAllocation:
    """Recommended future run allocation for one prompt version."""

    prompt_type: str
    version: int
    prompt_hash: str
    historical_runs: int
    prediction_count: int
    pipeline_run_count: int
    actual_prediction_count: int
    avg_actual_engagement_score: float | None
    mean_absolute_prediction_error: float | None
    avg_pipeline_final_score: float | None
    publish_rate: float | None
    performance_score: float
    exploration_runs: int
    exploitation_runs: int
    total_runs: int
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["reasons"] = list(self.reasons)
        data["warnings"] = list(self.warnings)
        return data


@dataclass(frozen=True)
class PromptExperimentAllocationReport:
    """Prompt experiment allocation plan."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    counts: dict[str, int]
    warnings: tuple[str, ...]
    allocations: tuple[PromptExperimentAllocation, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "allocations": [row.to_dict() for row in self.allocations],
            "artifact_type": self.artifact_type,
            "counts": self.counts,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "warnings": list(self.warnings),
        }


def allocate_prompt_experiments(
    db_or_conn: Any,
    *,
    total_runs: int = DEFAULT_TOTAL_RUNS,
    explore_percent: float = DEFAULT_EXPLORE_PERCENT,
    min_runs: int = DEFAULT_MIN_RUNS,
    prompt_type: str | None = None,
    now: datetime | None = None,
) -> PromptExperimentAllocationReport:
    """Recommend how many future runs should use each prompt version."""
    if total_runs < 0:
        raise ValueError("total_runs must be non-negative")
    if not 0 <= explore_percent <= 100:
        raise ValueError("explore_percent must be between 0 and 100")
    if min_runs < 0:
        raise ValueError("min_runs must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    warnings: list[str] = []

    if "prompt_versions" not in schema:
        warnings.append("Missing required table: prompt_versions.")
        return _empty_report(
            generated_at=generated_at,
            total_runs=total_runs,
            explore_percent=explore_percent,
            min_runs=min_runs,
            prompt_type=prompt_type,
            warnings=warnings,
        )

    optional_missing = [
        table
        for table in ("engagement_predictions", "pipeline_runs")
        if table not in schema
    ]
    for table in optional_missing:
        warnings.append(f"Missing optional table: {table}.")

    prompts = _prompt_windows(conn, prompt_type)
    if not prompts:
        if prompt_type:
            warnings.append(f"No prompt_versions rows matched prompt_type={prompt_type}.")
        else:
            warnings.append("No prompt_versions rows were found.")

    candidates = [
        _candidate_from_prompt(conn, schema, prompt, min_runs=min_runs)
        for prompt in prompts
    ]
    plan = _allocate(candidates, total_runs=total_runs, explore_percent=explore_percent)
    allocations = tuple(_finalize_candidate(candidate) for candidate in plan)
    allocated_total = sum(row.total_runs for row in allocations)
    counts = {
        "prompt_versions": len(allocations),
        "total_runs": allocated_total,
        "exploration_runs": sum(row.exploration_runs for row in allocations),
        "exploitation_runs": sum(row.exploitation_runs for row in allocations),
        "under_sampled": sum(1 for row in allocations if row.historical_runs < min_runs),
        "warnings": len(warnings) + sum(len(row.warnings) for row in allocations),
    }
    if allocated_total != total_runs and allocations:
        warnings.append(
            f"Allocation total {allocated_total} did not match requested total_runs {total_runs}."
        )
        counts["warnings"] += 1

    return PromptExperimentAllocationReport(
        artifact_type="prompt_experiment_allocation",
        generated_at=generated_at,
        filters={
            "explore_percent": explore_percent,
            "min_runs": min_runs,
            "prompt_type": prompt_type,
            "total_runs": total_runs,
        },
        counts=counts,
        warnings=tuple(warnings),
        allocations=allocations,
    )


def format_prompt_experiment_allocation_json(
    report: PromptExperimentAllocationReport,
) -> str:
    """Serialize a prompt experiment allocation report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_prompt_experiment_allocation_text(
    report: PromptExperimentAllocationReport,
) -> str:
    """Format a prompt experiment allocation report for operator review."""
    lines = [
        "Prompt Experiment Allocation",
        f"Generated: {report.generated_at}",
        (
            f"Budget: total={report.filters['total_runs']} "
            f"explore={report.filters['explore_percent']}% "
            f"min_runs={report.filters['min_runs']}"
        ),
    ]
    if report.filters["prompt_type"]:
        lines.append(f"Prompt type: {report.filters['prompt_type']}")
    lines.append(
        f"Allocated: total={report.counts['total_runs']} "
        f"exploration={report.counts['exploration_runs']} "
        f"exploitation={report.counts['exploitation_runs']}"
    )
    if report.warnings:
        lines.append("Warnings: " + "; ".join(report.warnings))
    lines.append("")

    if not report.allocations:
        lines.append("No prompt versions matched the requested filters.")
        return "\n".join(lines)

    for row in report.allocations:
        lines.append(
            f"- {row.prompt_type} v{row.version} ({row.prompt_hash[:10]}): "
            f"runs={row.total_runs} explore={row.exploration_runs} "
            f"exploit={row.exploitation_runs} historical={row.historical_runs} "
            f"score={row.performance_score:.3f}"
        )
        lines.append(
            "  metrics: "
            f"actual={_fmt(row.avg_actual_engagement_score)} "
            f"mae={_fmt(row.mean_absolute_prediction_error)} "
            f"pipeline={_fmt(row.avg_pipeline_final_score)} "
            f"publish={_pct(row.publish_rate)}"
        )
        if row.reasons:
            lines.append("  reasons: " + "; ".join(row.reasons))
        if row.warnings:
            lines.append("  warnings: " + "; ".join(row.warnings))
    return "\n".join(lines)


def _empty_report(
    *,
    generated_at: str,
    total_runs: int,
    explore_percent: float,
    min_runs: int,
    prompt_type: str | None,
    warnings: list[str],
) -> PromptExperimentAllocationReport:
    return PromptExperimentAllocationReport(
        artifact_type="prompt_experiment_allocation",
        generated_at=generated_at,
        filters={
            "explore_percent": explore_percent,
            "min_runs": min_runs,
            "prompt_type": prompt_type,
            "total_runs": total_runs,
        },
        counts={
            "prompt_versions": 0,
            "total_runs": 0,
            "exploration_runs": 0,
            "exploitation_runs": 0,
            "under_sampled": 0,
            "warnings": len(warnings),
        },
        warnings=tuple(warnings),
        allocations=(),
    )


def _candidate_from_prompt(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
    *,
    min_runs: int,
) -> dict[str, Any]:
    prediction = _prediction_stats(conn, schema, prompt)
    pipeline = _pipeline_stats(conn, schema, prompt)
    historical_runs = prediction["count"] + pipeline["count"]
    score = _performance_score(prediction, pipeline, prompt)
    warnings = []
    if historical_runs < min_runs:
        warnings.append(
            f"Insufficient sample: historical_runs={historical_runs} below min_runs={min_runs}."
        )
    if prediction["actual_count"] == 0 and pipeline["count"] == 0:
        warnings.append("No performance outcomes found for this prompt version.")
    return {
        "prompt": prompt,
        "prediction": prediction,
        "pipeline": pipeline,
        "historical_runs": historical_runs,
        "performance_score": score,
        "exploration_runs": 0,
        "exploitation_runs": 0,
        "min_runs": min_runs,
        "reasons": [],
        "warnings": warnings,
    }


def _allocate(
    candidates: list[dict[str, Any]],
    *,
    total_runs: int,
    explore_percent: float,
) -> list[dict[str, Any]]:
    if not candidates or total_runs == 0:
        return candidates

    exploration_budget = min(
        total_runs,
        int(math.floor(total_runs * (explore_percent / 100.0))),
    )
    _allocate_exploration(candidates, exploration_budget)
    exploitation_budget = total_runs - sum(row["exploration_runs"] for row in candidates)
    _allocate_exploitation(candidates, exploitation_budget)
    return sorted(candidates, key=_candidate_key)


def _allocate_exploration(candidates: list[dict[str, Any]], budget: int) -> None:
    remaining = budget
    under_sampled = sorted(
        [row for row in candidates if row["historical_runs"] < row["min_runs"]],
        key=lambda row: (row["historical_runs"], *_candidate_key(row)),
    )
    while remaining > 0:
        eligible = [
            row
            for row in under_sampled
            if row["historical_runs"] + row["exploration_runs"]
            < row["min_runs"]
        ]
        if not eligible:
            break
        for row in eligible:
            if remaining <= 0:
                break
            row["exploration_runs"] += 1
            remaining -= 1

    if remaining > 0:
        for row in _round_robin(candidates, remaining):
            row["exploration_runs"] += 1

    for row in candidates:
        if row["exploration_runs"]:
            if row["historical_runs"] < _warning_min_runs(row):
                row["reasons"].append(
                    "Exploration allocation closes an under-sampled prompt-version gap."
                )
            else:
                row["reasons"].append("Exploration allocation keeps active variants in rotation.")


def _allocate_exploitation(candidates: list[dict[str, Any]], budget: int) -> None:
    if budget <= 0:
        return
    scores = [max(0.0, row["performance_score"]) for row in candidates]
    if sum(scores) == 0:
        weights = [1.0 for _row in candidates]
    else:
        weights = scores
    total_weight = sum(weights)
    raw = [budget * weight / total_weight for weight in weights]
    base = [int(math.floor(value)) for value in raw]
    remaining = budget - sum(base)
    for row, runs in zip(candidates, base, strict=True):
        row["exploitation_runs"] += runs
    remainders = sorted(
        range(len(candidates)),
        key=lambda index: (
            -(raw[index] - base[index]),
            -candidates[index]["performance_score"],
            candidates[index]["historical_runs"],
            _candidate_key(candidates[index]),
        ),
    )
    for index in remainders[:remaining]:
        candidates[index]["exploitation_runs"] += 1

    for row in candidates:
        if row["exploitation_runs"]:
            row["reasons"].append(
                "Exploitation allocation is weighted by deterministic performance score."
            )


def _round_robin(candidates: list[dict[str, Any]], count: int) -> list[dict[str, Any]]:
    ordered = sorted(candidates, key=_candidate_key)
    return [ordered[index % len(ordered)] for index in range(count)]


def _finalize_candidate(candidate: dict[str, Any]) -> PromptExperimentAllocation:
    prompt = candidate["prompt"]
    prediction = candidate["prediction"]
    pipeline = candidate["pipeline"]
    total = candidate["exploration_runs"] + candidate["exploitation_runs"]
    if total == 0:
        candidate["reasons"].append("No runs allocated within the requested budget.")
    return PromptExperimentAllocation(
        prompt_type=prompt["prompt_type"],
        version=int(prompt["version"]),
        prompt_hash=prompt["prompt_hash"],
        historical_runs=candidate["historical_runs"],
        prediction_count=prediction["count"],
        pipeline_run_count=pipeline["count"],
        actual_prediction_count=prediction["actual_count"],
        avg_actual_engagement_score=_round_or_none(prediction["avg_actual"]),
        mean_absolute_prediction_error=_round_or_none(prediction["mae"]),
        avg_pipeline_final_score=_round_or_none(pipeline["avg_final_score"]),
        publish_rate=_round_or_none(pipeline["publish_rate"], digits=3),
        performance_score=_round_or_none(candidate["performance_score"], digits=3) or 0.0,
        exploration_runs=candidate["exploration_runs"],
        exploitation_runs=candidate["exploitation_runs"],
        total_runs=total,
        reasons=tuple(candidate["reasons"]),
        warnings=tuple(candidate["warnings"]),
    )


def _prompt_windows(
    conn: sqlite3.Connection,
    prompt_type: str | None,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if prompt_type:
        clauses.append("prompt_type = ?")
        params.append(prompt_type)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = _fetchall(
        conn,
        f"""SELECT prompt_type,
                   version,
                   prompt_hash,
                   avg_score,
                   usage_count,
                   created_at,
                   LEAD(created_at) OVER (
                       PARTITION BY prompt_type
                       ORDER BY created_at, version, id
                   ) AS next_created_at
            FROM prompt_versions
            {where_sql}
            ORDER BY prompt_type, version, created_at, id""",
        params,
    )
    return rows


def _prediction_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
) -> dict[str, Any]:
    if "engagement_predictions" not in schema:
        return {"count": 0, "actual_count": 0, "avg_actual": None, "mae": None}
    params: list[Any] = [
        prompt["prompt_type"],
        prompt["prompt_hash"],
        prompt["prompt_type"],
        str(prompt["version"]),
        f"v{prompt['version']}",
        prompt["prompt_type"],
    ]
    fallback = "prompt_type = ? AND (prompt_hash IS NULL OR prompt_hash = '') AND prompt_version IS NULL"
    if prompt.get("created_at"):
        fallback += " AND created_at >= ?"
        params.append(prompt["created_at"])
    if prompt.get("next_created_at"):
        fallback += " AND created_at < ?"
        params.append(prompt["next_created_at"])
    row = _fetchone(
        conn,
        f"""SELECT COUNT(*) AS count,
                   SUM(CASE WHEN actual_engagement_score IS NOT NULL THEN 1 ELSE 0 END)
                       AS actual_count,
                   AVG(actual_engagement_score) AS avg_actual,
                   AVG(ABS(prediction_error)) AS mae
            FROM engagement_predictions
            WHERE (prompt_type = ? AND prompt_hash = ?)
               OR (
                   prompt_type = ?
                   AND (prompt_hash IS NULL OR prompt_hash = '')
                   AND CAST(prompt_version AS TEXT) IN (?, ?)
               )
               OR ({fallback})""",
        params,
    )
    return {
        "count": row["count"] or 0,
        "actual_count": row["actual_count"] or 0,
        "avg_actual": row["avg_actual"],
        "mae": row["mae"],
    }


def _pipeline_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt: dict[str, Any],
) -> dict[str, Any]:
    if "pipeline_runs" not in schema:
        return {"count": 0, "avg_final_score": None, "publish_rate": None}
    clauses = ["content_type = ?"]
    params: list[Any] = [prompt["prompt_type"]]
    if prompt.get("created_at"):
        clauses.append("created_at >= ?")
        params.append(prompt["created_at"])
    if prompt.get("next_created_at"):
        clauses.append("created_at < ?")
        params.append(prompt["next_created_at"])
    row = _fetchone(
        conn,
        f"""SELECT COUNT(*) AS count,
                   AVG(final_score) AS avg_final_score,
                   AVG(CASE
                         WHEN published = 1 OR outcome = 'published' THEN 1.0
                         ELSE 0.0
                       END) AS publish_rate
            FROM pipeline_runs
            WHERE {' AND '.join(clauses)}""",
        params,
    )
    return {
        "count": row["count"] or 0,
        "avg_final_score": row["avg_final_score"],
        "publish_rate": row["publish_rate"],
    }


def _performance_score(
    prediction: dict[str, Any],
    pipeline: dict[str, Any],
    prompt: dict[str, Any],
) -> float:
    components = []
    if prediction["avg_actual"] is not None:
        components.append(float(prediction["avg_actual"]))
    if prediction["mae"] is not None:
        components.append(max(0.0, 10.0 - float(prediction["mae"])))
    if pipeline["avg_final_score"] is not None:
        components.append(float(pipeline["avg_final_score"]))
    if pipeline["publish_rate"] is not None:
        components.append(float(pipeline["publish_rate"]) * 10.0)
    if prompt.get("avg_score") is not None:
        components.append(float(prompt["avg_score"]))
    if not components:
        return 0.0
    return sum(components) / len(components)


def _candidate_key(candidate: dict[str, Any]) -> tuple[str, int, str]:
    prompt = candidate["prompt"]
    return (prompt["prompt_type"], int(prompt["version"]), prompt["prompt_hash"])


def _warning_min_runs(candidate: dict[str, Any]) -> int:
    return candidate["min_runs"]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [
        row["name"]
        for row in _fetchall(
            conn,
            "SELECT name FROM sqlite_master WHERE type = 'table'",
            [],
        )
    ]
    return {
        table: {row["name"] for row in _fetchall(conn, f"PRAGMA table_info({table})", [])}
        for table in tables
    }


def _fetchone(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> dict[str, Any]:
    rows = _fetchall(conn, sql, params)
    return rows[0] if rows else {}


def _fetchall(
    conn: sqlite3.Connection,
    sql: str,
    params: list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    columns = [description[0] for description in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None
