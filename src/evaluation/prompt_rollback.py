"""Prompt rollback recommendation reporting."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MIN_SAMPLES = 5
ROLLBACK_RULES = {
    "avg_eval_score": -1.0,
    "pass_rate": -0.2,
    "publish_rate": -0.2,
    "avg_actual_engagement_score": -2.0,
    "mean_absolute_prediction_error": 2.0,
}


@dataclass(frozen=True)
class PromptVersionMetrics:
    """Outcome metrics for one prompt version window."""

    prompt_type: str
    version: int
    prompt_hash: str
    prompt_created_at: str | None
    next_created_at: str | None
    sample_count: int
    eval_result_count: int
    pipeline_run_count: int
    prediction_count: int
    generated_content_count: int
    avg_eval_score: float | None
    pass_rate: float | None
    publish_rate: float | None
    avg_actual_engagement_score: float | None
    mean_absolute_prediction_error: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PromptRollbackRecommendation:
    """Rollback recommendation for one prompt type."""

    prompt_type: str
    decision: str
    current: PromptVersionMetrics
    candidate_previous: PromptVersionMetrics | None
    metric_deltas: dict[str, float | None]
    reasons: tuple[str, ...]
    confidence_caveats: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "prompt_type": self.prompt_type,
            "decision": self.decision,
            "current": self.current.to_dict(),
            "candidate_previous": (
                self.candidate_previous.to_dict() if self.candidate_previous else None
            ),
            "metric_deltas": self.metric_deltas,
            "reasons": list(self.reasons),
            "confidence_caveats": list(self.confidence_caveats),
        }


@dataclass(frozen=True)
class PromptRollbackReport:
    """Prompt rollback report plus schema and filter metadata."""

    generated_at: str
    days: int
    prompt_type: str | None
    min_samples: int
    missing_required_tables: tuple[str, ...]
    missing_optional_tables: tuple[str, ...]
    recommendations: tuple[PromptRollbackRecommendation, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "prompt_rollback_report",
            "status": "ok" if self.recommendations else "empty",
            "generated_at": self.generated_at,
            "days": self.days,
            "prompt_type": self.prompt_type,
            "min_samples": self.min_samples,
            "missing_required_tables": list(self.missing_required_tables),
            "missing_optional_tables": list(self.missing_optional_tables),
            "recommendation_count": len(self.recommendations),
            "recommendations": [item.to_dict() for item in self.recommendations],
        }


def build_prompt_rollback_report(
    db_or_conn: Any,
    *,
    prompt_type: str | None = None,
    days: int = DEFAULT_DAYS,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    now: datetime | None = None,
) -> PromptRollbackReport:
    """Build a read-only report recommending prompt rollback when warranted."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    required = ("prompt_versions",)
    optional = (
        "pipeline_runs",
        "engagement_predictions",
        "eval_results",
        "generated_content",
    )
    missing_required = tuple(table for table in required if table not in schema)
    missing_optional = tuple(table for table in optional if table not in schema)
    if missing_required:
        return PromptRollbackReport(
            generated_at=generated_at.isoformat(),
            days=days,
            prompt_type=prompt_type,
            min_samples=min_samples,
            missing_required_tables=missing_required,
            missing_optional_tables=missing_optional,
            recommendations=(),
        )

    windows = _prompt_windows(conn, schema, prompt_type)
    if not windows:
        return PromptRollbackReport(
            generated_at=generated_at.isoformat(),
            days=days,
            prompt_type=prompt_type,
            min_samples=min_samples,
            missing_required_tables=(),
            missing_optional_tables=missing_optional,
            recommendations=(),
        )

    metrics_by_type: dict[str, list[PromptVersionMetrics]] = {}
    cutoff = generated_at - timedelta(days=days)
    for window in windows:
        metrics = _version_metrics(conn, schema, window, cutoff, generated_at)
        metrics_by_type.setdefault(metrics.prompt_type, []).append(metrics)

    recommendations = []
    for item_type, rows in sorted(metrics_by_type.items()):
        rows.sort(key=lambda row: (row.version, row.prompt_created_at or "", row.prompt_hash))
        current = rows[-1]
        previous = next(
            (row for row in reversed(rows[:-1]) if row.sample_count >= min_samples),
            None,
        )
        if previous is None and len(rows) > 1:
            previous = rows[-2]
        recommendations.append(_recommend(item_type, current, previous, min_samples))

    return PromptRollbackReport(
        generated_at=generated_at.isoformat(),
        days=days,
        prompt_type=prompt_type,
        min_samples=min_samples,
        missing_required_tables=(),
        missing_optional_tables=missing_optional,
        recommendations=tuple(recommendations),
    )


def format_prompt_rollback_json(report: PromptRollbackReport) -> str:
    """Render the rollback report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_prompt_rollback_text(report: PromptRollbackReport) -> str:
    """Render the rollback report as operator-facing text."""
    lines = [
        "Prompt Rollback Recommendations",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        f"Minimum samples: {report.min_samples}",
    ]
    if report.prompt_type:
        lines.append(f"Prompt type: {report.prompt_type}")
    if report.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(report.missing_required_tables))
    if report.missing_optional_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_optional_tables))
    lines.append("")

    if not report.recommendations:
        lines.append("No prompt versions found for rollback analysis.")
        return "\n".join(lines)

    for recommendation in report.recommendations:
        current = recommendation.current
        previous = recommendation.candidate_previous
        lines.append(
            f"{recommendation.prompt_type}: {recommendation.decision.upper()} "
            f"current=v{current.version} n={current.sample_count}"
        )
        if previous:
            lines.append(
                f"  Candidate previous: v{previous.version} "
                f"({previous.prompt_hash[:10]}) n={previous.sample_count}"
            )
            lines.append("  Deltas: " + _delta_summary(recommendation.metric_deltas))
        else:
            lines.append("  Candidate previous: none")
        for reason in recommendation.reasons:
            lines.append(f"  - {reason}")
        for caveat in recommendation.confidence_caveats:
            lines.append(f"  ! {caveat}")
        lines.append("")
    return "\n".join(lines).rstrip()


def _recommend(
    prompt_type: str,
    current: PromptVersionMetrics,
    previous: PromptVersionMetrics | None,
    min_samples: int,
) -> PromptRollbackRecommendation:
    reasons: list[str] = []
    caveats: list[str] = []
    deltas = _metric_deltas(current, previous)

    if current.sample_count < min_samples:
        caveats.append(
            f"Current version has {current.sample_count} samples; "
            f"{min_samples} required before rollback."
        )
    if previous is None:
        caveats.append("No previous prompt version is available for comparison.")
    elif previous.sample_count < min_samples:
        caveats.append(
            f"Candidate previous version has {previous.sample_count} samples; "
            f"{min_samples} required for a stable baseline."
        )

    if previous is None:
        decision = "watch"
        reasons.append("No baseline exists yet; keep collecting outcomes.")
    elif current.sample_count < min_samples or previous.sample_count < min_samples:
        decision = "watch"
        reasons.append("Sample threshold prevents a rollback recommendation.")
    else:
        regressions = _regressions(deltas)
        severe = _severe_regression(deltas)
        if severe or len(regressions) >= 2:
            decision = "rollback"
            reasons.append(
                f"Current v{current.version} underperforms v{previous.version} "
                f"on {', '.join(regressions) or 'a primary metric'}."
            )
        elif regressions:
            decision = "watch"
            reasons.append(
                f"Current v{current.version} trails v{previous.version} "
                f"on {', '.join(regressions)}."
            )
        else:
            decision = "keep"
            reasons.append(
                f"Current v{current.version} is not materially worse than v{previous.version}."
            )

    if current.eval_result_count == 0:
        caveats.append("No eval_results rows contributed to the current version.")
    if current.pipeline_run_count == 0:
        caveats.append("No pipeline_runs rows contributed to the current version.")
    if current.prediction_count == 0:
        caveats.append("No engagement_predictions actuals contributed to the current version.")

    return PromptRollbackRecommendation(
        prompt_type=prompt_type,
        decision=decision,
        current=current,
        candidate_previous=previous,
        metric_deltas=deltas,
        reasons=tuple(reasons),
        confidence_caveats=tuple(caveats),
    )


def _regressions(deltas: dict[str, float | None]) -> list[str]:
    names = []
    for metric, threshold in ROLLBACK_RULES.items():
        delta = deltas.get(metric)
        if delta is None:
            continue
        if metric == "mean_absolute_prediction_error":
            if delta >= threshold:
                names.append(metric)
        elif delta <= threshold:
            names.append(metric)
    return names


def _severe_regression(deltas: dict[str, float | None]) -> bool:
    return any(
        (
            (deltas.get("avg_eval_score") is not None and deltas["avg_eval_score"] <= -2.0),
            (deltas.get("pass_rate") is not None and deltas["pass_rate"] <= -0.35),
            (deltas.get("publish_rate") is not None and deltas["publish_rate"] <= -0.35),
            (
                deltas.get("avg_actual_engagement_score") is not None
                and deltas["avg_actual_engagement_score"] <= -4.0
            ),
            (
                deltas.get("mean_absolute_prediction_error") is not None
                and deltas["mean_absolute_prediction_error"] >= 4.0
            ),
        )
    )


def _metric_deltas(
    current: PromptVersionMetrics,
    previous: PromptVersionMetrics | None,
) -> dict[str, float | None]:
    metrics = (
        "avg_eval_score",
        "pass_rate",
        "publish_rate",
        "avg_actual_engagement_score",
        "mean_absolute_prediction_error",
    )
    if previous is None:
        return {metric: None for metric in metrics}
    payload: dict[str, float | None] = {}
    for metric in metrics:
        current_value = getattr(current, metric)
        previous_value = getattr(previous, metric)
        payload[metric] = (
            _round(current_value - previous_value, 4)
            if current_value is not None and previous_value is not None
            else None
        )
    return payload


def _version_metrics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    window: dict[str, Any],
    cutoff: datetime,
    now: datetime,
) -> PromptVersionMetrics:
    prompt_type = str(window["prompt_type"])
    version = int(window["version"])
    prompt_hash = str(window["prompt_hash"])
    created_at = window.get("created_at")
    next_created_at = window.get("next_created_at")

    eval_stats = _eval_stats(conn, schema, prompt_type, created_at, next_created_at, cutoff, now)
    pipeline_stats = _pipeline_stats(
        conn, schema, prompt_type, created_at, next_created_at, cutoff, now
    )
    prediction_stats = _prediction_stats(
        conn,
        schema,
        prompt_type,
        version,
        prompt_hash,
        created_at,
        next_created_at,
        cutoff,
        now,
    )
    generated_stats = _generated_stats(
        conn, schema, prompt_type, created_at, next_created_at, cutoff, now
    )
    sample_count = (
        eval_stats["count"]
        + pipeline_stats["count"]
        + prediction_stats["count"]
        + generated_stats["count"]
    )
    return PromptVersionMetrics(
        prompt_type=prompt_type,
        version=version,
        prompt_hash=prompt_hash,
        prompt_created_at=created_at,
        next_created_at=next_created_at,
        sample_count=sample_count,
        eval_result_count=eval_stats["count"],
        pipeline_run_count=pipeline_stats["count"],
        prediction_count=prediction_stats["count"],
        generated_content_count=generated_stats["count"],
        avg_eval_score=_round(_coalesce(eval_stats["avg_score"], generated_stats["avg_score"])),
        pass_rate=_round(_coalesce(eval_stats["pass_rate"], generated_stats["pass_rate"]), 4),
        publish_rate=_round(
            _coalesce(pipeline_stats["publish_rate"], generated_stats["publish_rate"]), 4
        ),
        avg_actual_engagement_score=_round(prediction_stats["avg_actual"]),
        mean_absolute_prediction_error=_round(prediction_stats["mae"]),
    )


def _eval_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt_type: str,
    created_at: str | None,
    next_created_at: str | None,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "eval_results" not in schema:
        return {"count": 0, "avg_score": None, "pass_rate": None}
    columns = schema["eval_results"]
    if not {"content_type", "created_at"}.issubset(columns):
        return {"count": 0, "avg_score": None, "pass_rate": None}
    score = "final_score" if "final_score" in columns else "NULL"
    threshold = "threshold" if "threshold" in columns else "7.0"
    where_sql, params = _window_where("content_type", prompt_type, created_at, next_created_at, cutoff, now)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count,
                   AVG({score}) AS avg_score,
                   AVG(CASE
                         WHEN {score} IS NOT NULL AND {score} >= {threshold} THEN 1.0
                         WHEN {score} IS NOT NULL THEN 0.0
                       END) AS pass_rate
            FROM eval_results
            WHERE {where_sql}""",
        params,
    ).fetchone()
    return {
        "count": row["count"] or 0,
        "avg_score": row["avg_score"],
        "pass_rate": row["pass_rate"],
    }


def _pipeline_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt_type: str,
    created_at: str | None,
    next_created_at: str | None,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "pipeline_runs" not in schema:
        return {"count": 0, "publish_rate": None}
    columns = schema["pipeline_runs"]
    if not {"content_type", "created_at"}.issubset(columns):
        return {"count": 0, "publish_rate": None}
    published_expr = (
        "CASE WHEN published = 1 OR outcome = 'published' THEN 1.0 ELSE 0.0 END"
        if {"published", "outcome"}.issubset(columns)
        else "NULL"
    )
    where_sql, params = _window_where("content_type", prompt_type, created_at, next_created_at, cutoff, now)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count,
                   AVG({published_expr}) AS publish_rate
            FROM pipeline_runs
            WHERE {where_sql}""",
        params,
    ).fetchone()
    return {"count": row["count"] or 0, "publish_rate": row["publish_rate"]}


def _prediction_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt_type: str,
    version: int,
    prompt_hash: str,
    created_at: str | None,
    next_created_at: str | None,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "engagement_predictions" not in schema:
        return {"count": 0, "avg_actual": None, "mae": None}
    columns = schema["engagement_predictions"]
    if not {"created_at", "prompt_type"}.issubset(columns):
        return {"count": 0, "avg_actual": None, "mae": None}
    params: list[Any] = [cutoff.isoformat(), now.isoformat(), prompt_type, prompt_hash]
    clauses = [
        "created_at >= ?",
        "created_at <= ?",
        "actual_engagement_score IS NOT NULL",
        "((prompt_type = ? AND prompt_hash = ?)",
    ]
    if "prompt_version" in columns:
        clauses[-1] += " OR (prompt_type = ? AND CAST(prompt_version AS TEXT) IN (?, ?))"
        params.extend([prompt_type, str(version), f"v{version}"])
    if created_at:
        clauses[-1] += " OR (prompt_type = ? AND (prompt_hash IS NULL OR prompt_hash = '')"
        params.append(prompt_type)
        clauses[-1] += " AND prompt_version IS NULL AND created_at >= ?"
        params.append(created_at)
        if next_created_at:
            clauses[-1] += " AND created_at < ?"
            params.append(next_created_at)
        clauses[-1] += ")"
    clauses[-1] += ")"
    row = conn.execute(
        f"""SELECT COUNT(*) AS count,
                   AVG(actual_engagement_score) AS avg_actual,
                   AVG(ABS(prediction_error)) AS mae
            FROM engagement_predictions
            WHERE {' AND '.join(clauses)}""",
        params,
    ).fetchone()
    return {
        "count": row["count"] or 0,
        "avg_actual": row["avg_actual"],
        "mae": row["mae"],
    }


def _generated_stats(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt_type: str,
    created_at: str | None,
    next_created_at: str | None,
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any]:
    if "generated_content" not in schema:
        return {"count": 0, "avg_score": None, "pass_rate": None, "publish_rate": None}
    columns = schema["generated_content"]
    if not {"content_type", "created_at"}.issubset(columns):
        return {"count": 0, "avg_score": None, "pass_rate": None, "publish_rate": None}
    score = "eval_score" if "eval_score" in columns else "NULL"
    published = "published" if "published" in columns else "NULL"
    where_sql, params = _window_where("content_type", prompt_type, created_at, next_created_at, cutoff, now)
    row = conn.execute(
        f"""SELECT COUNT(*) AS count,
                   AVG({score}) AS avg_score,
                   AVG(CASE
                         WHEN {score} IS NOT NULL AND {score} >= 7.0 THEN 1.0
                         WHEN {score} IS NOT NULL THEN 0.0
                       END) AS pass_rate,
                   AVG(CASE
                         WHEN {published} = 1 THEN 1.0
                         WHEN {published} IS NOT NULL THEN 0.0
                       END) AS publish_rate
            FROM generated_content
            WHERE {where_sql}""",
        params,
    ).fetchone()
    return {
        "count": row["count"] or 0,
        "avg_score": row["avg_score"],
        "pass_rate": row["pass_rate"],
        "publish_rate": row["publish_rate"],
    }


def _prompt_windows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    prompt_type: str | None,
) -> list[dict[str, Any]]:
    columns = schema["prompt_versions"]
    required = {"prompt_type", "version", "prompt_hash"}
    if not required.issubset(columns):
        return []
    created = "created_at" if "created_at" in columns else "NULL"
    id_order = ", id" if "id" in columns else ""
    clauses = []
    params: list[Any] = []
    if prompt_type:
        clauses.append("prompt_type = ?")
        params.append(prompt_type)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT prompt_type,
                       version,
                       prompt_hash,
                       {created} AS created_at,
                       LEAD({created}) OVER (
                           PARTITION BY prompt_type
                           ORDER BY version{id_order}
                       ) AS next_created_at
                FROM prompt_versions
                {where_sql}
                ORDER BY prompt_type, version{id_order}""",
            params,
        ).fetchall()
    ]


def _window_where(
    type_column: str,
    prompt_type: str,
    created_at: str | None,
    next_created_at: str | None,
    cutoff: datetime,
    now: datetime,
) -> tuple[str, list[Any]]:
    clauses = [
        f"{type_column} = ?",
        "created_at >= ?",
        "created_at <= ?",
    ]
    params: list[Any] = [prompt_type, cutoff.isoformat(), now.isoformat()]
    if created_at:
        clauses.append("created_at >= ?")
        params.append(created_at)
    if next_created_at:
        clauses.append("created_at < ?")
        params.append(next_created_at)
    return " AND ".join(clauses), params


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _coalesce(primary: float | None, fallback: float | None) -> float | None:
    return primary if primary is not None else fallback


def _round(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def _delta_summary(deltas: dict[str, float | None]) -> str:
    parts = []
    for metric, value in deltas.items():
        if value is None:
            continue
        parts.append(f"{metric}={value:+.2f}")
    return ", ".join(parts) if parts else "n/a"
