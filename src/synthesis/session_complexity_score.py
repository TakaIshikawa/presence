"""Session complexity score calculation for workflow optimization.

Analyzes session characteristics to calculate normalized complexity scores
and categorize sessions into complexity tiers for resource allocation and
workflow planning.

Complexity factors:
- Duration: Longer sessions indicate more complex work
- Tool calls: More tool calls suggest higher complexity
- Context switches: Frequent switching between tasks/files increases complexity
- Error rate: Higher error rates indicate complexity or friction

Complexity tiers:
- simple: 0-25 (straightforward tasks, minimal context)
- moderate: 26-50 (standard development work)
- complex: 51-75 (multi-component features, refactoring)
- high-risk: 76-100 (critical systems, extensive changes)
"""

from __future__ import annotations

from dataclasses import dataclass


# Complexity tier thresholds (0-100 scale)
TIER_SIMPLE = "simple"
TIER_MODERATE = "moderate"
TIER_COMPLEX = "complex"
TIER_HIGH_RISK = "high-risk"

THRESHOLD_MODERATE = 26
THRESHOLD_COMPLEX = 51
THRESHOLD_HIGH_RISK = 76

# Weight factors for complexity components
WEIGHT_DURATION = 0.25
WEIGHT_TOOL_CALLS = 0.30
WEIGHT_CONTEXT_SWITCHES = 0.25
WEIGHT_ERROR_RATE = 0.20

# Normalization constants
MAX_DURATION_MINUTES = 120.0  # 2 hours
MAX_TOOL_CALLS = 100.0
MAX_CONTEXT_SWITCHES = 50.0
MAX_ERROR_RATE = 0.50  # 50%


@dataclass(frozen=True)
class SessionCharacteristics:
    """Session characteristics for complexity analysis."""

    duration_minutes: float
    tool_call_count: int
    context_switch_count: int
    error_count: int
    total_operations: int


@dataclass(frozen=True)
class SessionComplexityScore:
    """Session complexity score and categorization."""

    score: float  # 0-100 normalized score
    tier: str  # simple, moderate, complex, high-risk
    characteristics: SessionCharacteristics
    component_scores: dict[str, float]  # breakdown by factor
    insights: list[str]  # actionable insights


def calculate_session_complexity_score(
    duration_minutes: float,
    tool_call_count: int,
    context_switch_count: int,
    error_count: int,
    total_operations: int,
) -> SessionComplexityScore:
    """Calculate normalized session complexity score (0-100 scale).

    Args:
        duration_minutes: Session duration in minutes
        tool_call_count: Number of tool calls made
        context_switch_count: Number of context switches (file/task changes)
        error_count: Number of errors encountered
        total_operations: Total operations attempted (for error rate calculation)

    Returns:
        SessionComplexityScore with normalized score, tier, and insights
    """
    characteristics = SessionCharacteristics(
        duration_minutes=duration_minutes,
        tool_call_count=tool_call_count,
        context_switch_count=context_switch_count,
        error_count=error_count,
        total_operations=total_operations,
    )

    # Calculate component scores (normalized 0-1, then scaled to 0-100)
    duration_score = _normalize_duration(duration_minutes)
    tool_call_score = _normalize_tool_calls(tool_call_count)
    context_switch_score = _normalize_context_switches(context_switch_count)
    error_rate_score = _normalize_error_rate(error_count, total_operations)

    # Weighted composite score
    composite = (
        duration_score * WEIGHT_DURATION
        + tool_call_score * WEIGHT_TOOL_CALLS
        + context_switch_score * WEIGHT_CONTEXT_SWITCHES
        + error_rate_score * WEIGHT_ERROR_RATE
    )

    # Scale to 0-100
    score = min(100.0, max(0.0, composite * 100.0))

    # Component breakdown for transparency
    component_scores = {
        "duration": round(duration_score * 100.0, 2),
        "tool_calls": round(tool_call_score * 100.0, 2),
        "context_switches": round(context_switch_score * 100.0, 2),
        "error_rate": round(error_rate_score * 100.0, 2),
    }

    # Categorize into tier
    tier = categorize_complexity_tier(score)

    # Generate insights
    insights = _generate_insights(score, tier, characteristics, component_scores)

    return SessionComplexityScore(
        score=round(score, 2),
        tier=tier,
        characteristics=characteristics,
        component_scores=component_scores,
        insights=insights,
    )


def categorize_complexity_tier(score: float) -> str:
    """Categorize complexity score into tier.

    Args:
        score: Normalized complexity score (0-100)

    Returns:
        Tier name: simple, moderate, complex, or high-risk
    """
    if score < THRESHOLD_MODERATE:
        return TIER_SIMPLE
    elif score < THRESHOLD_COMPLEX:
        return TIER_MODERATE
    elif score < THRESHOLD_HIGH_RISK:
        return TIER_COMPLEX
    else:
        return TIER_HIGH_RISK


def _normalize_duration(duration_minutes: float) -> float:
    """Normalize session duration to 0-1 scale."""
    if duration_minutes <= 0:
        return 0.0
    return min(1.0, duration_minutes / MAX_DURATION_MINUTES)


def _normalize_tool_calls(tool_call_count: int) -> float:
    """Normalize tool call count to 0-1 scale."""
    if tool_call_count <= 0:
        return 0.0
    return min(1.0, tool_call_count / MAX_TOOL_CALLS)


def _normalize_context_switches(context_switch_count: int) -> float:
    """Normalize context switch count to 0-1 scale."""
    if context_switch_count <= 0:
        return 0.0
    return min(1.0, context_switch_count / MAX_CONTEXT_SWITCHES)


def _normalize_error_rate(error_count: int, total_operations: int) -> float:
    """Normalize error rate to 0-1 scale."""
    if total_operations <= 0 or error_count <= 0:
        return 0.0
    error_rate = error_count / total_operations
    return min(1.0, error_rate / MAX_ERROR_RATE)


def _generate_insights(
    score: float,
    tier: str,
    characteristics: SessionCharacteristics,
    component_scores: dict[str, float],
) -> list[str]:
    """Generate actionable insights for workflow optimization.

    Args:
        score: Overall complexity score
        tier: Complexity tier
        characteristics: Session characteristics
        component_scores: Component score breakdown

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if tier == TIER_SIMPLE:
        insights.append("Low complexity session - suitable for quick iterations")
    elif tier == TIER_MODERATE:
        insights.append("Standard complexity - typical development workflow")
    elif tier == TIER_COMPLEX:
        insights.append(
            "High complexity - consider breaking into smaller sessions"
        )
    else:  # high-risk
        insights.append(
            "Very high complexity - requires careful planning and monitoring"
        )

    # Component-specific insights
    if component_scores["duration"] > 75:
        insights.append(
            f"Extended duration ({characteristics.duration_minutes:.1f} min) - "
            "consider periodic breaks"
        )

    if component_scores["tool_calls"] > 75:
        insights.append(
            f"High tool usage ({characteristics.tool_call_count} calls) - "
            "may indicate exploratory work"
        )

    if component_scores["context_switches"] > 75:
        insights.append(
            f"Frequent context switches ({characteristics.context_switch_count}) - "
            "consider focusing on fewer components"
        )

    if component_scores["error_rate"] > 50:
        error_rate = (
            characteristics.error_count / characteristics.total_operations
            if characteristics.total_operations > 0
            else 0
        )
        insights.append(
            f"Elevated error rate ({error_rate:.1%}) - "
            "review prerequisites or dependencies"
        )

    # Resource allocation insights
    if score < THRESHOLD_MODERATE:
        insights.append("Resource allocation: minimal oversight required")
    elif score < THRESHOLD_COMPLEX:
        insights.append("Resource allocation: standard review process")
    elif score < THRESHOLD_HIGH_RISK:
        insights.append("Resource allocation: pair programming recommended")
    else:
        insights.append("Resource allocation: senior review and validation required")

    return insights
