"""Context window utilization tracking for Claude session memory efficiency.

Tracks Claude session context window usage over conversation turns to
monitor memory efficiency, identify context pruning events, and detect
summarization triggers.

Key metrics:
- Utilization percentage: Context tokens used vs. max capacity
- Pruning events: When context is forcibly reduced due to capacity limits
- Summarization triggers: Proactive context compression before pruning
- Rolling window stats: Average and peak utilization over recent turns

Monitoring approach:
- Track token usage per conversation turn
- Calculate utilization as percentage of max context window
- Detect pruning when utilization drops sharply after approaching max
- Identify summarization as gradual reduction in high-utilization periods
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# Context window constants
DEFAULT_MAX_CONTEXT_TOKENS = 200000  # Claude's typical context window
PRUNING_THRESHOLD = 0.95  # 95% utilization likely triggers pruning
SUMMARIZATION_THRESHOLD = 0.80  # 80% utilization may trigger summarization
ROLLING_WINDOW_SIZE = 10  # Number of recent turns for rolling stats

# Utilization tier classifications
TIER_LOW = "low"  # < 40% utilization
TIER_MODERATE = "moderate"  # 40-70% utilization
TIER_HIGH = "high"  # 70-90% utilization
TIER_CRITICAL = "critical"  # >= 90% utilization

THRESHOLD_MODERATE = 40
THRESHOLD_HIGH = 70
THRESHOLD_CRITICAL = 90


@dataclass(frozen=True)
class ConversationTurn:
    """Single conversation turn with context usage."""

    turn_number: int
    context_tokens: int  # Total tokens in context window at this turn
    added_tokens: Optional[int] = None  # New tokens added this turn


@dataclass(frozen=True)
class ContextWindowMetrics:
    """Context window utilization metrics."""

    avg_utilization: float  # Average utilization percentage (0-100)
    peak_utilization: float  # Maximum utilization percentage
    current_utilization: float  # Latest turn utilization
    pruning_events: int  # Count of detected pruning events
    summarization_events: int  # Count of detected summarization events
    total_turns: int  # Total conversation turns analyzed


@dataclass(frozen=True)
class ContextWindowUtilization:
    """Context window utilization analysis result."""

    metrics: ContextWindowMetrics
    utilization_tier: str  # Current utilization tier classification
    max_context_tokens: int  # Maximum context window size
    insights: list[str]  # Actionable insights


def analyze_context_window_utilization(
    turns: object,
    max_context_tokens: int = DEFAULT_MAX_CONTEXT_TOKENS,
) -> ContextWindowUtilization:
    """Analyze context window utilization across conversation turns.

    Args:
        turns: List of conversation turns with context token counts
        max_context_tokens: Maximum context window capacity

    Returns:
        ContextWindowUtilization with metrics, tier, and insights

    Raises:
        ValueError: If turns is not a list, contains invalid instances,
                   or has invalid values
    """
    # Validate inputs
    if not isinstance(turns, list):
        raise ValueError("turns must be a list")

    if not isinstance(max_context_tokens, int) or isinstance(max_context_tokens, bool):
        raise ValueError("max_context_tokens must be positive")
    if max_context_tokens <= 0:
        raise ValueError("max_context_tokens must be positive")

    # Validate turn instances
    for turn in turns:
        if not isinstance(turn, ConversationTurn):
            raise ValueError("All turns must be ConversationTurn instances")
        if not isinstance(turn.turn_number, int) or isinstance(turn.turn_number, bool):
            raise ValueError("turn_number must be non-negative")
        if turn.turn_number < 0:
            raise ValueError("turn_number must be non-negative")
        if not isinstance(turn.context_tokens, int) or isinstance(turn.context_tokens, bool):
            raise ValueError("context_tokens must be non-negative")
        if turn.context_tokens < 0:
            raise ValueError("context_tokens must be non-negative")
        if turn.added_tokens is not None and (
            not isinstance(turn.added_tokens, int)
            or isinstance(turn.added_tokens, bool)
            or turn.added_tokens < 0
        ):
            raise ValueError("added_tokens must be non-negative or None")

    # Handle empty session
    if not turns:
        metrics = ContextWindowMetrics(
            avg_utilization=0.0,
            peak_utilization=0.0,
            current_utilization=0.0,
            pruning_events=0,
            summarization_events=0,
            total_turns=0,
        )
        return ContextWindowUtilization(
            metrics=metrics,
            utilization_tier=TIER_LOW,
            max_context_tokens=max_context_tokens,
            insights=["Empty session - no context utilization data"],
        )

    # Calculate utilization metrics
    avg_utilization = _calculate_avg_utilization(turns, max_context_tokens)
    peak_utilization = _calculate_peak_utilization(turns, max_context_tokens)
    current_utilization = _calculate_current_utilization(turns, max_context_tokens)

    # Detect context management events
    pruning_events = _detect_pruning_events(turns, max_context_tokens)
    summarization_events = _detect_summarization_events(turns, max_context_tokens)

    # Build metrics
    metrics = ContextWindowMetrics(
        avg_utilization=round(avg_utilization, 2),
        peak_utilization=round(peak_utilization, 2),
        current_utilization=round(current_utilization, 2),
        pruning_events=pruning_events,
        summarization_events=summarization_events,
        total_turns=len(turns),
    )

    # Classify tier based on current utilization
    tier = _classify_utilization_tier(current_utilization)

    # Generate insights
    insights = _generate_insights(metrics, tier, max_context_tokens)

    return ContextWindowUtilization(
        metrics=metrics,
        utilization_tier=tier,
        max_context_tokens=max_context_tokens,
        insights=insights,
    )


def _calculate_avg_utilization(
    turns: list[ConversationTurn],
    max_context_tokens: int,
) -> float:
    """Calculate average context window utilization.

    Args:
        turns: List of conversation turns
        max_context_tokens: Maximum context window capacity

    Returns:
        Average utilization percentage (0-100)
    """
    if not turns:
        return 0.0

    total_utilization = sum(
        (turn.context_tokens / max_context_tokens) * 100.0
        for turn in turns
    )
    return total_utilization / len(turns)


def _calculate_peak_utilization(
    turns: list[ConversationTurn],
    max_context_tokens: int,
) -> float:
    """Calculate peak context window utilization.

    Args:
        turns: List of conversation turns
        max_context_tokens: Maximum context window capacity

    Returns:
        Maximum utilization percentage (0-100)
    """
    if not turns:
        return 0.0

    max_tokens = max(turn.context_tokens for turn in turns)
    return (max_tokens / max_context_tokens) * 100.0


def _calculate_current_utilization(
    turns: list[ConversationTurn],
    max_context_tokens: int,
) -> float:
    """Calculate current context window utilization.

    Args:
        turns: List of conversation turns
        max_context_tokens: Maximum context window capacity

    Returns:
        Current (latest turn) utilization percentage (0-100)
    """
    if not turns:
        return 0.0

    return (turns[-1].context_tokens / max_context_tokens) * 100.0


def _detect_pruning_events(
    turns: list[ConversationTurn],
    max_context_tokens: int,
) -> int:
    """Detect context pruning events.

    Pruning is detected when:
    - Utilization was at or near max capacity (>= 95%)
    - Followed by a sharp drop in context tokens (> 20% reduction)

    Args:
        turns: List of conversation turns
        max_context_tokens: Maximum context window capacity

    Returns:
        Count of detected pruning events
    """
    if len(turns) < 2:
        return 0

    pruning_count = 0
    threshold_tokens = max_context_tokens * PRUNING_THRESHOLD

    for i in range(1, len(turns)):
        prev_turn = turns[i - 1]
        curr_turn = turns[i]

        # Check if previous turn was near capacity
        if prev_turn.context_tokens >= threshold_tokens:
            # Check for sharp drop (> 20% reduction)
            if curr_turn.context_tokens < prev_turn.context_tokens * 0.80:
                pruning_count += 1

    return pruning_count


def _detect_summarization_events(
    turns: list[ConversationTurn],
    max_context_tokens: int,
) -> int:
    """Detect context summarization events.

    Summarization is detected when:
    - Utilization is high (>= 80%) but below pruning threshold
    - Followed by a moderate reduction in context (10-20% decrease)
    - Reduction is gradual (not sharp like pruning)

    Args:
        turns: List of conversation turns
        max_context_tokens: Maximum context window capacity

    Returns:
        Count of detected summarization events
    """
    if len(turns) < 2:
        return 0

    summarization_count = 0
    threshold_tokens = max_context_tokens * SUMMARIZATION_THRESHOLD
    pruning_threshold_tokens = max_context_tokens * PRUNING_THRESHOLD

    for i in range(1, len(turns)):
        prev_turn = turns[i - 1]
        curr_turn = turns[i]

        # Check if previous turn was in summarization zone
        if threshold_tokens <= prev_turn.context_tokens < pruning_threshold_tokens:
            # Check for moderate reduction (10-20%)
            reduction_ratio = curr_turn.context_tokens / prev_turn.context_tokens
            if 0.80 <= reduction_ratio < 0.90:
                summarization_count += 1

    return summarization_count


def _classify_utilization_tier(utilization: float) -> str:
    """Classify utilization percentage into tier.

    Args:
        utilization: Utilization percentage (0-100)

    Returns:
        Tier classification string
    """
    if utilization >= THRESHOLD_CRITICAL:
        return TIER_CRITICAL
    elif utilization >= THRESHOLD_HIGH:
        return TIER_HIGH
    elif utilization >= THRESHOLD_MODERATE:
        return TIER_MODERATE
    else:
        return TIER_LOW


def _generate_insights(
    metrics: ContextWindowMetrics,
    tier: str,
    max_context_tokens: int,
) -> list[str]:
    """Generate actionable insights for context window utilization.

    Args:
        metrics: Context window metrics
        tier: Utilization tier classification
        max_context_tokens: Maximum context window capacity

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if tier == TIER_CRITICAL:
        insights.append(
            f"Critical context utilization ({metrics.current_utilization:.1f}%) - "
            "pruning likely imminent"
        )
    elif tier == TIER_HIGH:
        insights.append(
            f"High context utilization ({metrics.current_utilization:.1f}%) - "
            "consider proactive summarization"
        )
    elif tier == TIER_MODERATE:
        insights.append(
            f"Moderate context utilization ({metrics.current_utilization:.1f}%) - "
            "healthy memory usage"
        )
    else:  # TIER_LOW
        insights.append(
            f"Low context utilization ({metrics.current_utilization:.1f}%) - "
            "efficient memory usage"
        )

    # Peak utilization insights
    if metrics.peak_utilization >= 95.0:
        insights.append(
            f"Peak utilization reached {metrics.peak_utilization:.1f}% - "
            "session approached capacity limits"
        )
    elif metrics.peak_utilization >= 80.0:
        insights.append(
            f"Peak utilization {metrics.peak_utilization:.1f}% - "
            "substantial context depth achieved"
        )

    # Pruning event insights
    if metrics.pruning_events > 0:
        insights.append(
            f"Detected {metrics.pruning_events} pruning event(s) - "
            "forced context reduction occurred"
        )
        if metrics.pruning_events >= 3:
            insights.append(
                "Frequent pruning detected - consider shorter conversation segments"
            )

    # Summarization event insights
    if metrics.summarization_events > 0:
        insights.append(
            f"Detected {metrics.summarization_events} summarization event(s) - "
            "proactive context compression observed"
        )
        if metrics.summarization_events >= 5:
            insights.append(
                "Frequent summarization - effective memory management in long session"
            )

    # Utilization efficiency insights
    if metrics.avg_utilization < 20.0 and metrics.total_turns > 10:
        insights.append(
            f"Very low average utilization ({metrics.avg_utilization:.1f}%) - "
            "context capacity underutilized"
        )
    elif metrics.avg_utilization > 70.0:
        insights.append(
            f"High average utilization ({metrics.avg_utilization:.1f}%) - "
            "memory-intensive conversation"
        )

    # Session length insights
    if metrics.total_turns > 50 and metrics.pruning_events == 0:
        insights.append(
            f"Long session ({metrics.total_turns} turns) without pruning - "
            "excellent memory efficiency"
        )

    # No events detected
    if metrics.pruning_events == 0 and metrics.summarization_events == 0:
        if metrics.total_turns > 5:
            insights.append(
                "No context management events detected - "
                "stable memory usage throughout session"
            )

    return insights
