"""Prompt version performance analytics."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class PromptPerformanceRow:
    """Metrics for one persisted prompt version."""

    prompt_type: str
    version: int
    prompt_hash: str
    prompt_created_at: str | None
    total_runs: int
    eval_result_count: int
    pipeline_run_count: int
    prediction_count: int
    avg_eval_score: float | None
    pass_rate: float | None
    avg_candidate_count: float | None
    avg_prediction_error: float | None
    mean_absolute_prediction_error: float | None
    published_count: int
    publish_rate: float | None
    outcomes: dict[str, int]
    insufficient_sample: bool


@dataclass(frozen=True)
class PromptPerformanceTotals:
    """Report-level totals."""

    prompt_versions: int
    total_runs: int
    eval_results: int
    pipeline_runs: int
    predictions: int
    published: int
    insufficient_samples: int


@dataclass(frozen=True)
class PromptPerformanceReport:
    """Prompt performance query result."""

    days: int
    prompt_type: str | None
    min_runs: int
    totals: PromptPerformanceTotals
    rows: list[PromptPerformanceRow]


class PromptPerformanceAnalyzer:
    """Correlate prompt versions with downstream quality and publish signals."""

    def __init__(self, db) -> None:
        self.db = db

    def build_report(
        self,
        days: int = 90,
        prompt_type: str | None = None,
        min_runs: int = 3,
    ) -> PromptPerformanceReport:
        """Build a prompt performance report.

        Exact prompt hashes are used where available. Type-only tables are
        attributed to the prompt version active at the event timestamp.
        """
        min_runs = max(1, min_runs)
        rows = [
            self._row_from_prompt(prompt, days=days, min_runs=min_runs)
            for prompt in self._prompt_windows(prompt_type)
        ]
        totals = PromptPerformanceTotals(
            prompt_versions=len(rows),
            total_runs=sum(row.total_runs for row in rows),
            eval_results=sum(row.eval_result_count for row in rows),
            pipeline_runs=sum(row.pipeline_run_count for row in rows),
            predictions=sum(row.prediction_count for row in rows),
            published=sum(row.published_count for row in rows),
            insufficient_samples=sum(1 for row in rows if row.insufficient_sample),
        )
        rows.sort(
            key=lambda row: (
                row.prompt_type,
                row.version,
                row.prompt_hash,
            )
        )
        return PromptPerformanceReport(
            days=days,
            prompt_type=prompt_type,
            min_runs=min_runs,
            totals=totals,
            rows=rows,
        )

    def _prompt_windows(self, prompt_type: str | None) -> list[dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if prompt_type:
            clauses.append("prompt_type = ?")
            params.append(prompt_type)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cursor = self.db.conn.execute(
            f"""SELECT prompt_type,
                       version,
                       prompt_hash,
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
        return [dict(row) for row in cursor.fetchall()]

    def _row_from_prompt(
        self,
        prompt: dict[str, Any],
        days: int,
        min_runs: int,
    ) -> PromptPerformanceRow:
        prompt_type = prompt["prompt_type"]
        version = int(prompt["version"])
        prompt_hash = prompt["prompt_hash"]
        created_at = prompt.get("created_at")
        next_created_at = prompt.get("next_created_at")

        eval_stats = self.average_eval_score(
            prompt_type=prompt_type,
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )
        prediction_stats = self.prediction_error(
            prompt_type=prompt_type,
            version=version,
            prompt_hash=prompt_hash,
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )
        publish_stats = self.publish_outcome(
            prompt_type=prompt_type,
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )

        total_runs = (
            eval_stats["count"]
            + prediction_stats["count"]
            + publish_stats["count"]
        )
        return PromptPerformanceRow(
            prompt_type=prompt_type,
            version=version,
            prompt_hash=prompt_hash,
            prompt_created_at=created_at,
            total_runs=total_runs,
            eval_result_count=eval_stats["count"],
            pipeline_run_count=publish_stats["count"],
            prediction_count=prediction_stats["count"],
            avg_eval_score=_round_or_none(eval_stats["avg_eval_score"]),
            pass_rate=_round_or_none(eval_stats["pass_rate"], digits=3),
            avg_candidate_count=_round_or_none(eval_stats["avg_candidate_count"]),
            avg_prediction_error=_round_or_none(prediction_stats["avg_prediction_error"]),
            mean_absolute_prediction_error=_round_or_none(
                prediction_stats["mean_absolute_prediction_error"]
            ),
            published_count=publish_stats["published_count"],
            publish_rate=_round_or_none(publish_stats["publish_rate"], digits=3),
            outcomes=publish_stats["outcomes"],
            insufficient_sample=total_runs < min_runs,
        )

    def average_eval_score(
        self,
        prompt_type: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> dict[str, Any]:
        """Return average eval score, pass rate, and candidate count."""
        where_sql, params = self._type_time_where(
            column_type="content_type",
            prompt_type=prompt_type,
            created_column="created_at",
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )
        row = self.db.conn.execute(
            f"""SELECT COUNT(*) AS count,
                       AVG(final_score) AS avg_eval_score,
                       AVG(candidate_count) AS avg_candidate_count,
                       AVG(CASE
                             WHEN final_score IS NOT NULL
                              AND final_score >= threshold THEN 1.0
                             WHEN final_score IS NOT NULL THEN 0.0
                           END) AS pass_rate
                FROM eval_results
                WHERE {where_sql}""",
            params,
        ).fetchone()
        return {
            "count": row["count"] or 0,
            "avg_eval_score": row["avg_eval_score"],
            "avg_candidate_count": row["avg_candidate_count"],
            "pass_rate": row["pass_rate"],
        }

    def prediction_error(
        self,
        prompt_type: str,
        version: int,
        prompt_hash: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> dict[str, Any]:
        """Return prediction error metrics for a prompt version."""
        params: list[Any] = [
            prompt_type,
            prompt_hash,
            prompt_type,
            str(version),
            f"v{version}",
            prompt_type,
        ]
        clauses = [
            "actual_engagement_score IS NOT NULL",
            """(
                 (prompt_type = ? AND prompt_hash = ?)
                 OR (
                     prompt_type = ?
                     AND (prompt_hash IS NULL OR prompt_hash = '')
                     AND CAST(prompt_version AS TEXT) IN (?, ?)
                 )
                 OR (
                     prompt_type = ?
                     AND (prompt_hash IS NULL OR prompt_hash = '')
                     AND prompt_version IS NULL
            """,
        ]
        if created_at:
            clauses[-1] += " AND created_at >= ?"
            params.append(created_at)
        if next_created_at:
            clauses[-1] += " AND created_at < ?"
            params.append(next_created_at)
        clauses[-1] += "))"
        clauses.append("created_at >= datetime('now', ?)")
        params.append(f"-{days} days")

        row = self.db.conn.execute(
            f"""SELECT COUNT(*) AS count,
                       AVG(prediction_error) AS avg_prediction_error,
                       AVG(ABS(prediction_error)) AS mae
                FROM engagement_predictions
                WHERE {' AND '.join(clauses)}""",
            params,
        ).fetchone()
        return {
            "count": row["count"] or 0,
            "avg_prediction_error": row["avg_prediction_error"],
            "mean_absolute_prediction_error": row["mae"],
        }

    def candidate_count(
        self,
        prompt_type: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> float | None:
        """Return average eval candidate count for a prompt type/version window."""
        return self.average_eval_score(
            prompt_type=prompt_type,
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )["avg_candidate_count"]

    def pass_rate(
        self,
        prompt_type: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> float | None:
        """Return eval pass rate for a prompt type/version window."""
        return self.average_eval_score(
            prompt_type=prompt_type,
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )["pass_rate"]

    def publish_outcome(
        self,
        prompt_type: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> dict[str, Any]:
        """Return pipeline publish outcome metrics for a prompt version."""
        where_sql, params = self._type_time_where(
            column_type="content_type",
            prompt_type=prompt_type,
            created_column="created_at",
            created_at=created_at,
            next_created_at=next_created_at,
            days=days,
        )
        rows = self.db.conn.execute(
            f"""SELECT COALESCE(outcome, CASE WHEN published = 1 THEN 'published' ELSE 'unknown' END)
                          AS outcome,
                       COUNT(*) AS count,
                       SUM(CASE
                             WHEN published = 1 OR outcome = 'published' THEN 1
                             ELSE 0
                           END) AS published_count
                FROM pipeline_runs
                WHERE {where_sql}
                GROUP BY outcome""",
            params,
        ).fetchall()

        outcomes: dict[str, int] = {}
        total = 0
        published_count = 0
        for row in rows:
            outcome = row["outcome"] or "unknown"
            count = row["count"] or 0
            outcomes[outcome] = count
            total += count
            published_count += row["published_count"] or 0

        return {
            "count": total,
            "published_count": published_count,
            "publish_rate": (published_count / total) if total else None,
            "outcomes": outcomes,
        }

    @staticmethod
    def _type_time_where(
        column_type: str,
        prompt_type: str,
        created_column: str,
        created_at: str | None,
        next_created_at: str | None,
        days: int,
    ) -> tuple[str, list[Any]]:
        clauses = [
            f"{column_type} = ?",
            f"{created_column} >= datetime('now', ?)",
        ]
        params: list[Any] = [prompt_type, f"-{days} days"]
        if created_at:
            clauses.append(f"{created_column} >= ?")
            params.append(created_at)
        if next_created_at:
            clauses.append(f"{created_column} < ?")
            params.append(next_created_at)
        return " AND ".join(clauses), params


def prompt_performance_report_to_dict(report: PromptPerformanceReport) -> dict[str, Any]:
    """Serialize a prompt performance report for JSON output."""
    return {
        "status": "ok" if report.rows else "empty",
        "days": report.days,
        "prompt_type": report.prompt_type,
        "min_runs": report.min_runs,
        "totals": asdict(report.totals),
        "rows": [asdict(row) for row in report.rows],
    }


def format_prompt_performance_json(report: PromptPerformanceReport) -> str:
    """Format a prompt performance report as JSON."""
    return json.dumps(prompt_performance_report_to_dict(report), indent=2, sort_keys=True)


def format_prompt_performance_text(report: PromptPerformanceReport) -> str:
    """Format a prompt performance report as operator-facing text."""
    lines = [
        "Prompt Performance Report",
        "=" * 80,
        f"Lookback:    last {report.days} days",
        f"Min runs:    {report.min_runs}",
    ]
    if report.prompt_type:
        lines.append(f"Prompt type: {report.prompt_type}")
    lines.extend(
        [
            f"Versions:    {report.totals.prompt_versions}",
            f"Runs:        {report.totals.total_runs}",
            "",
        ]
    )

    if not report.rows:
        lines.append("No prompt versions matched the requested filters.")
        return "\n".join(lines)

    enough = [row for row in report.rows if not row.insufficient_sample]
    if enough:
        ranked = sorted(enough, key=_ranking_score, reverse=True)
        lines.append(f"Best:  {_summary_line(ranked[0])}")
        lines.append(f"Worst: {_summary_line(ranked[-1])}")
        lines.append("")
    else:
        lines.append("No prompt versions have enough samples for best/worst ranking.")
        lines.append("")

    headers = [
        "Prompt",
        "Ver",
        "Hash",
        "Runs",
        "Eval",
        "Pass",
        "Cand",
        "MAE",
        "Pub",
        "Low N",
    ]
    rendered_rows = [
        [
            row.prompt_type,
            str(row.version),
            row.prompt_hash[:10],
            str(row.total_runs),
            _fmt(row.avg_eval_score),
            _pct(row.pass_rate),
            _fmt(row.avg_candidate_count),
            _fmt(row.mean_absolute_prediction_error),
            _pct(row.publish_rate),
            "yes" if row.insufficient_sample else "no",
        ]
        for row in report.rows
    ]
    widths = [
        max(len(headers[index]), *(len(row[index]) for row in rendered_rows))
        for index in range(len(headers))
    ]
    lines.append(
        "  ".join(header.ljust(widths[index]) for index, header in enumerate(headers))
    )
    lines.append("  ".join("-" * width for width in widths))
    for row in rendered_rows:
        lines.append(
            "  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
        )
    return "\n".join(lines)


def _ranking_score(row: PromptPerformanceRow) -> tuple[float, float, float, float, int]:
    return (
        row.pass_rate if row.pass_rate is not None else -1.0,
        row.publish_rate if row.publish_rate is not None else -1.0,
        row.avg_eval_score if row.avg_eval_score is not None else -1.0,
        -(row.mean_absolute_prediction_error or 0.0),
        row.total_runs,
    )


def _summary_line(row: PromptPerformanceRow) -> str:
    return (
        f"{row.prompt_type} v{row.version} ({row.prompt_hash[:10]}), "
        f"runs={row.total_runs}, eval={_fmt(row.avg_eval_score)}, "
        f"pass={_pct(row.pass_rate)}, publish={_pct(row.publish_rate)}, "
        f"mae={_fmt(row.mean_absolute_prediction_error)}"
    )


def _fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _pct(value: float | None) -> str:
    return "n/a" if value is None else f"{value * 100:.1f}%"


def _round_or_none(value: float | None, digits: int = 2) -> float | None:
    return round(value, digits) if value is not None else None


def default_period_start(days: int) -> datetime:
    """Return the UTC start timestamp for a lookback period."""
    return datetime.now(timezone.utc) - timedelta(days=days)
