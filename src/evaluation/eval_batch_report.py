"""Comparison reports for recorded dry-run evaluation batches."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


@dataclass(frozen=True)
class BatchSummary:
    """Aggregate summary for one recorded evaluation batch."""

    batch_id: int
    label: str | None
    content_type: str
    generator_model: str
    evaluator_model: str
    threshold: float
    created_at: str | None
    result_count: int
    average_final_score: float | None
    rejection_count: int
    rejection_rate: float
    prompt_count: int
    commit_count: int
    candidate_count: int
    filter_stats: dict[str, int]


@dataclass(frozen=True)
class BatchComparison:
    """Pairwise comparison of one batch against the baseline."""

    batch_id: int
    label: str | None
    average_final_score_delta: float | None
    rejection_rate_delta: float
    filter_stats_delta: dict[str, int]
    batch: BatchSummary


@dataclass(frozen=True)
class EvalBatchReport:
    """Report payload for baseline and comparison batches."""

    label: str | None
    days: int | None
    baseline_batch_id: int | None
    compare_batch_ids: list[int]
    baseline: BatchSummary | None
    comparisons: list[BatchComparison]
    missing_batch_ids: list[int]


def _created_at_in_window(created_at: str | None, days: int | None, now: datetime) -> bool:
    if days is None or not created_at:
        return True
    try:
        created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError:
        return True
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    return created >= now - timedelta(days=days)


def _numeric_filter_stats(raw: Any) -> dict[str, int]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw, dict):
        return {}

    stats: dict[str, int] = {}
    for key, value in raw.items():
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            stats[str(key)] = stats.get(str(key), 0) + int(value)
    return stats


def _sum_filter_stats(results: list[dict]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for row in results:
        for key, value in _numeric_filter_stats(row.get("filter_stats")).items():
            totals[key] = totals.get(key, 0) + value
    return dict(sorted(totals.items()))


def summarize_batch(payload: dict) -> BatchSummary:
    """Build a deterministic aggregate summary from Database.get_eval_batch()."""

    batch = payload["batch"]
    results = payload.get("results") or []
    result_count = len(results)
    scores = [
        float(row["final_score"])
        for row in results
        if row.get("final_score") is not None
    ]
    rejection_count = sum(1 for row in results if row.get("rejection_reason"))
    average_final_score = sum(scores) / len(scores) if scores else None

    return BatchSummary(
        batch_id=int(batch["id"]),
        label=batch.get("label"),
        content_type=str(batch.get("content_type") or ""),
        generator_model=str(batch.get("generator_model") or ""),
        evaluator_model=str(batch.get("evaluator_model") or ""),
        threshold=float(batch.get("threshold") or 0),
        created_at=batch.get("created_at"),
        result_count=result_count,
        average_final_score=average_final_score,
        rejection_count=rejection_count,
        rejection_rate=(rejection_count / result_count) if result_count else 0.0,
        prompt_count=sum(int(row.get("prompt_count") or 0) for row in results),
        commit_count=sum(int(row.get("commit_count") or 0) for row in results),
        candidate_count=sum(int(row.get("candidate_count") or 0) for row in results),
        filter_stats=_sum_filter_stats(results),
    )


def compare_batches(baseline: BatchSummary, batch: BatchSummary) -> BatchComparison:
    """Compare one batch summary against a baseline summary."""

    if baseline.average_final_score is None or batch.average_final_score is None:
        score_delta = None
    else:
        score_delta = batch.average_final_score - baseline.average_final_score

    filter_keys = sorted(set(baseline.filter_stats) | set(batch.filter_stats))
    filter_delta = {
        key: batch.filter_stats.get(key, 0) - baseline.filter_stats.get(key, 0)
        for key in filter_keys
    }

    return BatchComparison(
        batch_id=batch.batch_id,
        label=batch.label,
        average_final_score_delta=score_delta,
        rejection_rate_delta=batch.rejection_rate - baseline.rejection_rate,
        filter_stats_delta=filter_delta,
        batch=batch,
    )


def build_eval_batch_report(
    db,
    baseline_batch_id: int | None,
    compare_batch_ids: list[int] | None = None,
    *,
    label: str | None = None,
    days: int | None = None,
    now: datetime | None = None,
) -> EvalBatchReport:
    """Load requested batches and build a comparison report.

    The function only reads persisted eval tables. It performs no model calls.
    """

    compare_batch_ids = compare_batch_ids or []
    now = now or datetime.now(timezone.utc)
    missing: list[int] = []

    def load(batch_id: int | None) -> BatchSummary | None:
        if batch_id is None:
            return None
        payload = db.get_eval_batch(batch_id)
        if not payload:
            missing.append(batch_id)
            return None
        batch = payload.get("batch") or {}
        if not _created_at_in_window(batch.get("created_at"), days, now):
            missing.append(batch_id)
            return None
        return summarize_batch(payload)

    baseline = load(baseline_batch_id)
    comparison_summaries = [summary for summary in (load(batch_id) for batch_id in compare_batch_ids) if summary]
    comparisons = [
        compare_batches(baseline, summary)
        for summary in comparison_summaries
        if baseline is not None
    ]

    return EvalBatchReport(
        label=label,
        days=days,
        baseline_batch_id=baseline_batch_id,
        compare_batch_ids=list(compare_batch_ids),
        baseline=baseline,
        comparisons=comparisons,
        missing_batch_ids=sorted(set(missing)),
    )


def _fmt_score(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"


def _fmt_delta(value: float | None, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:+.2f}{suffix}"


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.1f}%"


def format_text_report(report: EvalBatchReport) -> str:
    """Format an evaluation batch comparison as stable human-readable text."""

    if report.baseline is None:
        missing = ", ".join(str(batch_id) for batch_id in report.missing_batch_ids) or "none"
        return (
            "Eval Batch Comparison Report\n"
            "No baseline batch found.\n"
            f"Missing/skipped batch IDs: {missing}"
        )

    baseline = report.baseline
    lines = [
        "Eval Batch Comparison Report",
        f"Label: {report.label or '(none)'}",
        f"Days: {report.days if report.days is not None else 'all'}",
        "",
        "Baseline",
        (
            f"  Batch {baseline.batch_id}: {baseline.label or '(unlabeled)'} | "
            f"{baseline.content_type} | gen={baseline.generator_model} | "
            f"eval={baseline.evaluator_model} | threshold={baseline.threshold:g}"
        ),
        (
            f"  Results: {baseline.result_count} | Avg score: {_fmt_score(baseline.average_final_score)} | "
            f"Rejections: {baseline.rejection_count} ({_fmt_pct(baseline.rejection_rate)})"
        ),
        (
            f"  Totals: prompts={baseline.prompt_count}, commits={baseline.commit_count}, "
            f"candidates={baseline.candidate_count}"
        ),
        "  Filters: " + _format_filter_stats(baseline.filter_stats),
        "",
    ]

    if not report.compare_batch_ids:
        lines.append("Comparisons: none requested")
    elif not report.comparisons:
        lines.append("Comparisons: no comparison batches found")
    else:
        lines.extend([
            "Comparisons",
            (
                f"{'Batch':>6} {'Label':<20} {'Results':>7} {'Avg':>7} "
                f"{'Delta':>8} {'Reject':>9} {'RejDelta':>9}"
            ),
            (
                f"{'-' * 6:>6} {'-' * 20:<20} {'-' * 7:>7} {'-' * 7:>7} "
                f"{'-' * 8:>8} {'-' * 9:>9} {'-' * 9:>9}"
            ),
        ])
        for comparison in report.comparisons:
            batch = comparison.batch
            label = (batch.label or "(unlabeled)")[:20]
            lines.append(
                f"{batch.batch_id:6d} {label:<20} {batch.result_count:7d} "
                f"{_fmt_score(batch.average_final_score):>7} "
                f"{_fmt_delta(comparison.average_final_score_delta):>8} "
                f"{_fmt_pct(batch.rejection_rate):>9} "
                f"{_fmt_delta(comparison.rejection_rate_delta * 100, '%'):>9}"
            )
            lines.append("       Filters: " + _format_filter_stats(batch.filter_stats))
            lines.append("       Filter delta: " + _format_filter_stats(comparison.filter_stats_delta))

    if report.missing_batch_ids:
        lines.extend([
            "",
            "Missing/skipped batch IDs: " + ", ".join(str(batch_id) for batch_id in report.missing_batch_ids),
        ])

    return "\n".join(lines)


def _format_filter_stats(stats: dict[str, int]) -> str:
    if not stats:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(stats.items()))


def report_to_dict(report: EvalBatchReport) -> dict:
    """Convert a report dataclass to a JSON-serializable dictionary."""

    return asdict(report)


def format_json_report(report: EvalBatchReport) -> str:
    """Format an evaluation batch comparison as deterministic JSON."""

    return json.dumps(report_to_dict(report), indent=2, sort_keys=True)
