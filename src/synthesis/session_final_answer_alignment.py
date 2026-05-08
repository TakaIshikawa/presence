"""Session final answer alignment analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_DEFERRED = "deferred"
STATUS_SKIPPED = "skipped"

QUALITY_NO_ITEMS = "no_items"
QUALITY_ALIGNED = "aligned"
QUALITY_INCOMPLETE = "incomplete"
QUALITY_MISLEADING = "misleading"


@dataclass(frozen=True)
class FinalAnswerOutcome:
    item_id: str
    item_type: str
    status: str
    mentioned_in_final: bool


@dataclass(frozen=True)
class FinalAnswerAlignmentMetrics:
    completed_items: int
    failed_items: int
    deferred_items: int
    mentioned_completed_count: int
    omitted_completed_count: int
    overstated_failed_count: int
    unmentioned_deferred_count: int
    mentioned_skipped_count: int


@dataclass(frozen=True)
class FinalAnswerAlignmentAnalysis:
    metrics: FinalAnswerAlignmentMetrics
    alignment_quality: str
    insights: tuple[str, ...]


def analyze_session_final_answer_alignment(
    outcomes: Sequence[FinalAnswerOutcome],
) -> FinalAnswerAlignmentAnalysis:
    """Compare tracked work outcomes with final answer mentions."""

    _validate_outcomes(outcomes)
    if not outcomes:
        metrics = FinalAnswerAlignmentMetrics(0, 0, 0, 0, 0, 0, 0, 0)
        return FinalAnswerAlignmentAnalysis(
            metrics, QUALITY_NO_ITEMS, ("No tracked work outcomes supplied.",)
        )

    completed = [item for item in outcomes if item.status == STATUS_COMPLETED]
    failed = [item for item in outcomes if item.status == STATUS_FAILED]
    deferred = [item for item in outcomes if item.status == STATUS_DEFERRED]
    skipped = [item for item in outcomes if item.status == STATUS_SKIPPED]
    metrics = FinalAnswerAlignmentMetrics(
        completed_items=len(completed),
        failed_items=len(failed),
        deferred_items=len(deferred),
        mentioned_completed_count=sum(1 for item in completed if item.mentioned_in_final),
        omitted_completed_count=sum(1 for item in completed if not item.mentioned_in_final),
        overstated_failed_count=sum(1 for item in failed if item.mentioned_in_final),
        unmentioned_deferred_count=sum(1 for item in deferred if not item.mentioned_in_final),
        mentioned_skipped_count=sum(1 for item in skipped if item.mentioned_in_final),
    )
    return FinalAnswerAlignmentAnalysis(
        metrics,
        _classify_quality(metrics),
        _generate_insights(metrics),
    )


def _validate_outcomes(outcomes: Sequence[FinalAnswerOutcome]) -> None:
    if not isinstance(outcomes, (list, tuple)):
        raise ValueError("outcomes must be a list or tuple")
    valid_statuses = {STATUS_COMPLETED, STATUS_FAILED, STATUS_DEFERRED, STATUS_SKIPPED}
    seen: set[str] = set()
    for item in outcomes:
        if not isinstance(item, FinalAnswerOutcome):
            raise ValueError("outcomes must contain FinalAnswerOutcome instances")
        if not isinstance(item.item_id, str) or not item.item_id.strip():
            raise ValueError("item_id must be a non-empty string")
        if item.item_id in seen:
            raise ValueError("item_id values must be unique")
        if not isinstance(item.item_type, str) or not item.item_type.strip():
            raise ValueError("item_type must be a non-empty string")
        if item.status not in valid_statuses:
            raise ValueError(f"unsupported status: {item.status}")
        if not isinstance(item.mentioned_in_final, bool):
            raise ValueError("mentioned_in_final must be a boolean")
        seen.add(item.item_id)


def _classify_quality(metrics: FinalAnswerAlignmentMetrics) -> str:
    if (
        metrics.completed_items == 0
        and metrics.failed_items == 0
        and metrics.deferred_items == 0
        and metrics.mentioned_skipped_count == 0
    ):
        return QUALITY_NO_ITEMS
    if metrics.overstated_failed_count:
        return QUALITY_MISLEADING
    if (
        metrics.omitted_completed_count
        or metrics.unmentioned_deferred_count
        or metrics.mentioned_skipped_count
    ):
        return QUALITY_INCOMPLETE
    return QUALITY_ALIGNED


def _generate_insights(metrics: FinalAnswerAlignmentMetrics) -> tuple[str, ...]:
    if (
        metrics.completed_items == 0
        and metrics.failed_items == 0
        and metrics.deferred_items == 0
        and metrics.mentioned_skipped_count == 0
    ):
        return ("No tracked work outcomes supplied.",)
    if metrics.overstated_failed_count:
        return (f"{metrics.overstated_failed_count} failed items were mentioned as final work.",)
    if metrics.mentioned_skipped_count:
        return (f"{metrics.mentioned_skipped_count} skipped items were mentioned in the final answer.",)
    if metrics.omitted_completed_count:
        return (f"{metrics.omitted_completed_count} completed items were omitted from the final answer.",)
    if metrics.unmentioned_deferred_count:
        return (f"{metrics.unmentioned_deferred_count} deferred items were not called out.",)
    return ("Final answer mentions align with tracked work outcomes.",)
