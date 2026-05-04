"""Pipeline refinement delta report for measuring refinement effectiveness.

Analyzes pipeline_runs to measure whether refinement is improving candidate scores.
Reports on delta distributions, outcome patterns, and refinement pick rates.
"""

import csv
import io
import json
import logging
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from storage.db import Database

logger = logging.getLogger(__name__)

DEFAULT_DAYS = 30
DEFAULT_MIN_DELTA = None


@dataclass
class RefinementDeltaAggregation:
    """Aggregated refinement delta statistics by content_type and refinement_picked."""

    content_type: str
    refinement_picked: Optional[str]
    run_count: int
    improved_count: int
    regressed_count: int
    unchanged_count: int
    skipped_count: int
    average_delta: float
    median_delta: float


@dataclass
class RefinementDeltaDetail:
    """Detailed refinement delta row."""

    batch_id: str
    content_type: str
    refinement_picked: Optional[str]
    best_score_before_refine: Optional[float]
    best_score_after_refine: Optional[float]
    final_score: Optional[float]
    delta: Optional[float]
    outcome: Optional[str]
    rejection_reason: Optional[str]
    created_at: str


@dataclass
class PipelineRefinementDeltaReport:
    """Complete pipeline refinement delta report."""

    period_start: datetime
    period_end: datetime
    total_runs: int
    aggregations: list[RefinementDeltaAggregation]
    details: list[RefinementDeltaDetail]


def build_pipeline_refinement_delta_report(
    db: Database,
    *,
    content_type: Optional[str] = None,
    outcome: Optional[str] = None,
    refinement_picked: Optional[str] = None,
    days: int = DEFAULT_DAYS,
    min_delta: Optional[float] = DEFAULT_MIN_DELTA,
    limit: Optional[int] = None,
) -> PipelineRefinementDeltaReport:
    """Build a pipeline refinement delta report with aggregations and details.

    Args:
        db: Database instance
        content_type: Filter by content_type (e.g., 'x_thread', 'x_post')
        outcome: Filter by outcome (e.g., 'published', 'below_threshold', 'all_filtered')
        refinement_picked: Filter by refinement_picked (e.g., 'REFINED', 'ORIGINAL')
        days: Lookback window in days
        min_delta: Minimum absolute delta to include in details (None = no filter)
        limit: Maximum number of detail rows to return (None = no limit)

    Returns:
        PipelineRefinementDeltaReport with aggregations and details
    """
    period_end = datetime.now(timezone.utc)
    period_start = period_end - timedelta(days=days)

    # Build WHERE clause
    where_clauses = ["created_at >= datetime('now', ?)"]
    params: list = [f"-{days} days"]

    if content_type:
        where_clauses.append("content_type = ?")
        params.append(content_type)

    if outcome:
        where_clauses.append("outcome = ?")
        params.append(outcome)

    if refinement_picked:
        where_clauses.append("refinement_picked = ?")
        params.append(refinement_picked)

    where_sql = " AND ".join(where_clauses)

    # Get all pipeline runs
    cursor = db.conn.execute(
        f"""SELECT batch_id, content_type, refinement_picked,
                   best_score_before_refine, best_score_after_refine,
                   final_score, outcome, rejection_reason, created_at
            FROM pipeline_runs
            WHERE {where_sql}
            ORDER BY created_at DESC""",
        params,
    )

    rows = cursor.fetchall()
    total_runs = len(rows)

    # Build details
    details: list[RefinementDeltaDetail] = []
    deltas_by_key: dict[tuple, list[float]] = {}

    for row in rows:
        batch_id = row[0]
        ct = row[1]
        picked = row[2]
        before = row[3]
        after = row[4]
        final = row[5]
        out = row[6]
        rejection = row[7]
        created = row[8]

        # Calculate delta
        delta = None
        if before is not None and after is not None:
            delta = after - before

        detail = RefinementDeltaDetail(
            batch_id=batch_id,
            content_type=ct,
            refinement_picked=picked,
            best_score_before_refine=before,
            best_score_after_refine=after,
            final_score=final,
            delta=delta,
            outcome=out,
            rejection_reason=rejection,
            created_at=created,
        )

        # Apply min_delta filter
        if min_delta is not None:
            if delta is None or abs(delta) < min_delta:
                continue

        details.append(detail)

        # Track deltas for aggregation
        if delta is not None:
            key = (ct, picked)
            deltas_by_key.setdefault(key, []).append(delta)

    # Apply limit to details
    if limit is not None:
        details = details[:limit]

    # Build aggregations
    aggregations: list[RefinementDeltaAggregation] = []

    # Group all rows by content_type and refinement_picked
    for row in rows:
        ct = row[1]
        picked = row[2]
        before = row[3]
        after = row[4]

        key = (ct, picked)

        # Find or create aggregation
        agg = next((a for a in aggregations if a.content_type == ct and a.refinement_picked == picked), None)
        if agg is None:
            deltas = deltas_by_key.get(key, [])
            agg = RefinementDeltaAggregation(
                content_type=ct,
                refinement_picked=picked,
                run_count=0,
                improved_count=0,
                regressed_count=0,
                unchanged_count=0,
                skipped_count=0,
                average_delta=round(sum(deltas) / len(deltas), 3) if deltas else 0.0,
                median_delta=round(statistics.median(deltas), 3) if deltas else 0.0,
            )
            aggregations.append(agg)

        # Count this run
        agg.run_count += 1

        # Categorize delta
        if before is None or after is None:
            agg.skipped_count += 1
        else:
            delta = after - before
            if delta > 0:
                agg.improved_count += 1
            elif delta < 0:
                agg.regressed_count += 1
            else:
                agg.unchanged_count += 1

    # Sort aggregations
    aggregations.sort(key=lambda a: (a.content_type, a.refinement_picked or ""))

    return PipelineRefinementDeltaReport(
        period_start=period_start,
        period_end=period_end,
        total_runs=total_runs,
        aggregations=aggregations,
        details=details,
    )


def format_pipeline_refinement_delta_json(report: PipelineRefinementDeltaReport) -> str:
    """Format pipeline refinement delta report as JSON."""
    return json.dumps(
        {
            "period_start": report.period_start.isoformat(),
            "period_end": report.period_end.isoformat(),
            "total_runs": report.total_runs,
            "aggregations": [
                {
                    "content_type": agg.content_type,
                    "refinement_picked": agg.refinement_picked,
                    "run_count": agg.run_count,
                    "improved_count": agg.improved_count,
                    "regressed_count": agg.regressed_count,
                    "unchanged_count": agg.unchanged_count,
                    "skipped_count": agg.skipped_count,
                    "average_delta": agg.average_delta,
                    "median_delta": agg.median_delta,
                }
                for agg in report.aggregations
            ],
            "details": [
                {
                    "batch_id": d.batch_id,
                    "content_type": d.content_type,
                    "refinement_picked": d.refinement_picked,
                    "best_score_before_refine": d.best_score_before_refine,
                    "best_score_after_refine": d.best_score_after_refine,
                    "final_score": d.final_score,
                    "delta": d.delta,
                    "outcome": d.outcome,
                    "rejection_reason": d.rejection_reason,
                    "created_at": d.created_at,
                }
                for d in report.details
            ],
        },
        indent=2,
        sort_keys=True,
    )


def format_pipeline_refinement_delta_csv(report: PipelineRefinementDeltaReport) -> str:
    """Format pipeline refinement delta report as CSV."""
    output = io.StringIO()

    # Write aggregations section
    output.write("# Aggregations\n")
    agg_writer = csv.DictWriter(
        output,
        fieldnames=[
            "content_type",
            "refinement_picked",
            "run_count",
            "improved_count",
            "regressed_count",
            "unchanged_count",
            "skipped_count",
            "average_delta",
            "median_delta",
        ],
    )
    agg_writer.writeheader()
    for agg in report.aggregations:
        agg_writer.writerow({
            "content_type": agg.content_type,
            "refinement_picked": agg.refinement_picked or "",
            "run_count": agg.run_count,
            "improved_count": agg.improved_count,
            "regressed_count": agg.regressed_count,
            "unchanged_count": agg.unchanged_count,
            "skipped_count": agg.skipped_count,
            "average_delta": agg.average_delta,
            "median_delta": agg.median_delta,
        })

    # Write details section
    output.write("\n# Details\n")
    detail_writer = csv.DictWriter(
        output,
        fieldnames=[
            "batch_id",
            "content_type",
            "refinement_picked",
            "best_score_before_refine",
            "best_score_after_refine",
            "final_score",
            "delta",
            "outcome",
            "rejection_reason",
            "created_at",
        ],
    )
    detail_writer.writeheader()
    for detail in report.details:
        detail_writer.writerow({
            "batch_id": detail.batch_id,
            "content_type": detail.content_type,
            "refinement_picked": detail.refinement_picked or "",
            "best_score_before_refine": detail.best_score_before_refine or "",
            "best_score_after_refine": detail.best_score_after_refine or "",
            "final_score": detail.final_score or "",
            "delta": detail.delta or "",
            "outcome": detail.outcome or "",
            "rejection_reason": detail.rejection_reason or "",
            "created_at": detail.created_at,
        })

    return output.getvalue()
