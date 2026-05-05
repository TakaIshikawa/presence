"""Prompt effectiveness score metric for response quality assessment.

Calculates prompt quality scores based on response coherence, task completion,
and iteration count to measure prompt engineering quality and identify
patterns that lead to successful first-response outcomes.

Scoring factors:
- First-response accuracy: Whether prompt achieved goal without revisions
- Clarification request frequency: Lower is better (clear, specific prompts)
- Revision cycles: Fewer iterations indicate better prompt quality
- Task completion rate: Percentage of tasks fully completed

Quality tiers:
- excellent: 85-100 (clear, specific, achieves goals quickly)
- good: 70-84 (mostly effective with minor iterations)
- average: 50-69 (requires moderate clarification/revision)
- poor: 0-49 (vague, incomplete, requires extensive back-and-forth)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# Weight factors for composite score
WEIGHT_FIRST_RESPONSE_SUCCESS = 0.40  # Primary indicator
WEIGHT_CLARIFICATION_RATE = 0.25  # Lower is better
WEIGHT_REVISION_CYCLES = 0.20  # Fewer is better
WEIGHT_COMPLETION_RATE = 0.15  # Higher is better

# Normalization constants
MAX_CLARIFICATIONS = 3.0  # More than 3 clarifications is poor
MAX_REVISIONS = 5.0  # More than 5 revisions is poor

# Quality tiers
TIER_POOR = "poor"
TIER_AVERAGE = "average"
TIER_GOOD = "good"
TIER_EXCELLENT = "excellent"

THRESHOLD_AVERAGE = 50
THRESHOLD_GOOD = 70
THRESHOLD_EXCELLENT = 85


@dataclass(frozen=True)
class PromptMetrics:
    """Prompt interaction metrics."""

    total_prompts: int
    first_response_successes: int  # Prompts that achieved goal immediately
    clarification_requests: int  # Total clarifications needed
    revision_cycles: int  # Total revisions required
    tasks_completed: int  # Tasks fully completed
    tasks_attempted: int  # Tasks attempted


@dataclass(frozen=True)
class PromptEffectivenessScore:
    """Prompt effectiveness score and breakdown."""

    score: float  # 0-100 composite score
    tier: str  # Quality tier classification
    metrics: PromptMetrics  # Raw input metrics
    component_scores: dict[str, float]  # Breakdown by factor
    insights: list[str]  # Actionable insights


def calculate_prompt_effectiveness_score(
    total_prompts: int,
    first_response_successes: int,
    clarification_requests: int,
    revision_cycles: int,
    tasks_completed: int,
    tasks_attempted: int,
) -> PromptEffectivenessScore:
    """Calculate composite prompt effectiveness score.

    Args:
        total_prompts: Total number of prompts submitted
        first_response_successes: Prompts achieving goal without revision
        clarification_requests: Number of clarification requests made
        revision_cycles: Number of revision/correction cycles
        tasks_completed: Number of tasks fully completed
        tasks_attempted: Number of tasks attempted

    Returns:
        PromptEffectivenessScore with composite score, tier, and insights

    Raises:
        ValueError: If metrics are invalid (negative values, inconsistencies)
    """
    # Validate inputs
    if total_prompts < 0:
        raise ValueError("total_prompts must be non-negative")
    if first_response_successes < 0:
        raise ValueError("first_response_successes must be non-negative")
    if clarification_requests < 0:
        raise ValueError("clarification_requests must be non-negative")
    if revision_cycles < 0:
        raise ValueError("revision_cycles must be non-negative")
    if tasks_completed < 0:
        raise ValueError("tasks_completed must be non-negative")
    if tasks_attempted < 0:
        raise ValueError("tasks_attempted must be non-negative")

    # Logical validations
    if first_response_successes > total_prompts:
        raise ValueError("first_response_successes cannot exceed total_prompts")
    if tasks_completed > tasks_attempted:
        raise ValueError("tasks_completed cannot exceed tasks_attempted")

    # Handle empty case
    if total_prompts == 0:
        metrics = PromptMetrics(
            total_prompts=0,
            first_response_successes=0,
            clarification_requests=0,
            revision_cycles=0,
            tasks_completed=0,
            tasks_attempted=0,
        )
        return PromptEffectivenessScore(
            score=0.0,
            tier=TIER_POOR,
            metrics=metrics,
            component_scores={
                "first_response_success": 0.0,
                "clarification_rate": 0.0,
                "revision_cycles": 0.0,
                "completion_rate": 0.0,
            },
            insights=["No prompts analyzed - insufficient data"],
        )

    # Create metrics
    metrics = PromptMetrics(
        total_prompts=total_prompts,
        first_response_successes=first_response_successes,
        clarification_requests=clarification_requests,
        revision_cycles=revision_cycles,
        tasks_completed=tasks_completed,
        tasks_attempted=tasks_attempted,
    )

    # Calculate component scores
    first_response_score = _calculate_first_response_score(
        first_response_successes,
        total_prompts,
    )

    clarification_score = _calculate_clarification_score(
        clarification_requests,
        total_prompts,
    )

    revision_score = _calculate_revision_score(
        revision_cycles,
        total_prompts,
    )

    completion_score = _calculate_completion_score(
        tasks_completed,
        tasks_attempted,
    )

    # Weighted composite score
    composite = (
        first_response_score * WEIGHT_FIRST_RESPONSE_SUCCESS
        + clarification_score * WEIGHT_CLARIFICATION_RATE
        + revision_score * WEIGHT_REVISION_CYCLES
        + completion_score * WEIGHT_COMPLETION_RATE
    )

    # Scale to 0-100
    score = min(100.0, max(0.0, composite * 100.0))

    # Component breakdown
    component_scores = {
        "first_response_success": round(first_response_score * 100.0, 2),
        "clarification_rate": round(clarification_score * 100.0, 2),
        "revision_cycles": round(revision_score * 100.0, 2),
        "completion_rate": round(completion_score * 100.0, 2),
    }

    # Categorize tier
    tier = _categorize_quality_tier(score)

    # Generate insights
    insights = _generate_insights(
        score=score,
        tier=tier,
        metrics=metrics,
        component_scores=component_scores,
    )

    return PromptEffectivenessScore(
        score=round(score, 2),
        tier=tier,
        metrics=metrics,
        component_scores=component_scores,
        insights=insights,
    )


def _calculate_first_response_score(
    first_response_successes: int,
    total_prompts: int,
) -> float:
    """Calculate first-response success score (0-1 normalized).

    Args:
        first_response_successes: Prompts achieving goal immediately
        total_prompts: Total prompts

    Returns:
        Normalized first-response success rate (0-1)
    """
    if total_prompts == 0:
        return 0.0

    return first_response_successes / total_prompts


def _calculate_clarification_score(
    clarification_requests: int,
    total_prompts: int,
) -> float:
    """Calculate clarification rate score (0-1 normalized, inverted).

    Lower clarification rate is better.

    Args:
        clarification_requests: Number of clarifications needed
        total_prompts: Total prompts

    Returns:
        Normalized clarification score (0-1, inverted so lower is better)
    """
    if total_prompts == 0:
        return 0.0

    clarifications_per_prompt = clarification_requests / total_prompts

    # Normalize against max (higher = worse, so invert)
    normalized = min(1.0, clarifications_per_prompt / MAX_CLARIFICATIONS)
    return 1.0 - normalized  # Invert: 0 clarifications = score 1.0


def _calculate_revision_score(
    revision_cycles: int,
    total_prompts: int,
) -> float:
    """Calculate revision cycles score (0-1 normalized, inverted).

    Fewer revision cycles is better.

    Args:
        revision_cycles: Number of revision cycles
        total_prompts: Total prompts

    Returns:
        Normalized revision score (0-1, inverted so fewer is better)
    """
    if total_prompts == 0:
        return 0.0

    revisions_per_prompt = revision_cycles / total_prompts

    # Normalize against max (higher = worse, so invert)
    normalized = min(1.0, revisions_per_prompt / MAX_REVISIONS)
    return 1.0 - normalized  # Invert: 0 revisions = score 1.0


def _calculate_completion_score(
    tasks_completed: int,
    tasks_attempted: int,
) -> float:
    """Calculate task completion rate score (0-1 normalized).

    Args:
        tasks_completed: Tasks fully completed
        tasks_attempted: Tasks attempted

    Returns:
        Normalized completion rate (0-1)
    """
    if tasks_attempted == 0:
        return 0.0

    return tasks_completed / tasks_attempted


def _categorize_quality_tier(score: float) -> str:
    """Categorize quality score into tier.

    Args:
        score: Quality score (0-100)

    Returns:
        Tier name
    """
    if score >= THRESHOLD_EXCELLENT:
        return TIER_EXCELLENT
    elif score >= THRESHOLD_GOOD:
        return TIER_GOOD
    elif score >= THRESHOLD_AVERAGE:
        return TIER_AVERAGE
    else:
        return TIER_POOR


def _generate_insights(
    score: float,
    tier: str,
    metrics: PromptMetrics,
    component_scores: dict[str, float],
) -> list[str]:
    """Generate actionable insights for prompt effectiveness.

    Args:
        score: Overall quality score
        tier: Quality tier
        metrics: Raw prompt metrics
        component_scores: Component score breakdown

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if tier == TIER_EXCELLENT:
        insights.append(
            f"Excellent prompt quality ({score:.1f}/100) - "
            "clear, specific, achieves goals quickly"
        )
    elif tier == TIER_GOOD:
        insights.append(
            f"Good prompt quality ({score:.1f}/100) - "
            "mostly effective with minor iterations"
        )
    elif tier == TIER_AVERAGE:
        insights.append(
            f"Average prompt quality ({score:.1f}/100) - "
            "room for improvement in clarity"
        )
    else:  # TIER_POOR
        insights.append(
            f"Poor prompt quality ({score:.1f}/100) - "
            "prompts need significant improvement"
        )

    # First-response success insights
    if component_scores["first_response_success"] >= 80.0:
        success_rate = (metrics.first_response_successes / metrics.total_prompts) * 100
        insights.append(
            f"High first-response success rate ({success_rate:.0f}%) - "
            "prompts are clear and specific"
        )
    elif component_scores["first_response_success"] < 30.0:
        success_rate = (metrics.first_response_successes / metrics.total_prompts) * 100
        insights.append(
            f"Low first-response success rate ({success_rate:.0f}%) - "
            "prompts may be too vague or incomplete"
        )

    # Clarification rate insights
    clarifications_per_prompt = metrics.clarification_requests / metrics.total_prompts
    if clarifications_per_prompt > 2.0:
        insights.append(
            f"High clarification rate ({clarifications_per_prompt:.1f} per prompt) - "
            "prompts missing key details or context"
        )
    elif clarifications_per_prompt < 0.5 and metrics.total_prompts >= 5:
        insights.append(
            f"Low clarification rate ({clarifications_per_prompt:.1f} per prompt) - "
            "prompts are well-structured"
        )

    # Revision cycles insights
    revisions_per_prompt = metrics.revision_cycles / metrics.total_prompts
    if revisions_per_prompt > 3.0:
        insights.append(
            f"High revision rate ({revisions_per_prompt:.1f} per prompt) - "
            "prompts may not align well with desired outcomes"
        )
    elif revisions_per_prompt < 1.0 and metrics.total_prompts >= 5:
        insights.append(
            f"Low revision rate ({revisions_per_prompt:.1f} per prompt) - "
            "good alignment between prompts and goals"
        )

    # Completion rate insights
    if metrics.tasks_attempted > 0:
        completion_rate = (metrics.tasks_completed / metrics.tasks_attempted) * 100
        if completion_rate >= 90.0:
            insights.append(
                f"High task completion rate ({completion_rate:.0f}%) - "
                "excellent goal achievement"
            )
        elif completion_rate < 50.0:
            insights.append(
                f"Low task completion rate ({completion_rate:.0f}%) - "
                "many tasks incomplete or abandoned"
            )

    # Pattern detection
    if (metrics.first_response_successes == 0 and
        metrics.total_prompts > 0 and
        metrics.clarification_requests > metrics.total_prompts):
        insights.append(
            "No immediate successes with high clarification needs - "
            "consider more detailed initial prompts"
        )

    # Best practice recommendations
    if tier in [TIER_POOR, TIER_AVERAGE]:
        recommendations = []
        if component_scores["first_response_success"] < 50.0:
            recommendations.append("be more specific about desired outcomes")
        if component_scores["clarification_rate"] < 50.0:
            recommendations.append("include more context and examples")
        if component_scores["completion_rate"] < 50.0:
            recommendations.append("break complex tasks into smaller steps")

        if recommendations:
            insights.append(
                f"Suggestions to improve: {', '.join(recommendations)}"
            )

    return insights
