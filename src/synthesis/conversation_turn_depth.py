"""Conversation turn depth analysis for nested interaction complexity tracking.

Analyzes conversation nesting and turn depth patterns to measure interaction complexity.
Turn depth refers to the vertical complexity of conversations - how deeply nested
interactions become through follow-ups, clarifications, and context building.

Metrics:
- Turn depth stats: Average depth, max nesting level, depth variance
- Nesting histogram: Distribution of conversation depths
- Context switches: Frequency of depth level changes
- Deep engagement duration: Sustained periods of deep conversation

Depth levels:
- Level 0: Initial query or topic introduction
- Level 1: Direct response or first follow-up
- Level 2+: Nested clarifications, refinements, deep dives
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


# Depth level classifications
DEPTH_SHALLOW = 0  # Initial queries
DEPTH_MODERATE = 1  # Direct responses
DEPTH_DEEP = 2  # First level of nesting
DEPTH_VERY_DEEP = 3  # Deep nested interactions

# Engagement thresholds
MIN_DEEP_ENGAGEMENT_TURNS = 3  # Minimum turns to qualify as sustained deep engagement
DEEP_ENGAGEMENT_THRESHOLD = 2  # Depth level that qualifies as "deep"


@dataclass(frozen=True)
class ConversationTurn:
    """Single turn in a conversation with depth information."""

    turn_number: int  # Sequential turn number (0-indexed)
    depth_level: int  # Nesting depth (0 = root, 1+ = nested)
    parent_turn: int | None  # Turn this responds to (None for depth 0)


@dataclass(frozen=True)
class TurnDepthStats:
    """Statistical metrics for turn depths."""

    avg_depth: float
    max_depth: int
    min_depth: int
    depth_variance: float


@dataclass(frozen=True)
class ContextSwitch:
    """A context switch between depth levels."""

    from_turn: int
    to_turn: int
    from_depth: int
    to_depth: int
    depth_delta: int  # Positive = deeper, negative = shallower


@dataclass(frozen=True)
class DeepEngagementPeriod:
    """Period of sustained deep conversation."""

    start_turn: int
    end_turn: int
    duration_turns: int
    avg_depth: float
    max_depth: int


@dataclass(frozen=True)
class ConversationTurnDepthAnalysis:
    """Complete conversation turn depth analysis."""

    turn_depth_stats: TurnDepthStats
    nesting_histogram: dict[int, int]  # depth -> count
    context_switches: list[ContextSwitch]
    deep_engagement_duration: list[DeepEngagementPeriod]
    total_turns: int
    insights: list[str]


def analyze_conversation_turn_depth(
    turns: Sequence[ConversationTurn],
) -> dict:
    """Analyze conversation nesting and turn depth patterns.

    Calculates metrics for turn depth distribution, nesting levels,
    context switching patterns, and sustained deep engagement periods.

    Args:
        turns: Sequence of conversation turns with depth information

    Returns:
        Dict with:
            - turn_depth_stats: TurnDepthStats with avg/max/min/variance
            - nesting_histogram: Dict mapping depth levels to turn counts
            - context_switches: List of ContextSwitch objects
            - deep_engagement_duration: List of DeepEngagementPeriod objects

    Raises:
        ValueError: If turns contains invalid data
    """
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a sequence (list or tuple)")

    # Validate turns
    for turn in turns:
        if not isinstance(turn, ConversationTurn):
            raise ValueError("turns must contain ConversationTurn instances")
        if turn.turn_number < 0:
            raise ValueError("turn_number must be non-negative")
        if turn.depth_level < 0:
            raise ValueError("depth_level must be non-negative")
        if turn.parent_turn is not None and turn.parent_turn < 0:
            raise ValueError("parent_turn must be non-negative or None")
        if turn.parent_turn is not None and turn.parent_turn >= turn.turn_number:
            raise ValueError("parent_turn must be less than turn_number")

    # Handle empty conversation
    if not turns:
        return {
            "turn_depth_stats": {
                "avg_depth": 0.0,
                "max_depth": 0,
                "min_depth": 0,
                "depth_variance": 0.0,
            },
            "nesting_histogram": {},
            "context_switches": [],
            "deep_engagement_duration": [],
        }

    # Calculate turn depth statistics
    turn_depth_stats = _calculate_turn_depth_stats(turns)

    # Build nesting histogram
    nesting_histogram = _build_nesting_histogram(turns)

    # Detect context switches
    context_switches = _detect_context_switches(turns)

    # Identify deep engagement periods
    deep_engagement_periods = _identify_deep_engagement_periods(turns)

    # Generate insights
    insights = _generate_turn_depth_insights(
        turn_depth_stats=turn_depth_stats,
        nesting_histogram=nesting_histogram,
        context_switches=context_switches,
        deep_engagement_periods=deep_engagement_periods,
        total_turns=len(turns),
    )

    # Return as ConversationTurnDepthAnalysis for structured output
    analysis = ConversationTurnDepthAnalysis(
        turn_depth_stats=turn_depth_stats,
        nesting_histogram=nesting_histogram,
        context_switches=context_switches,
        deep_engagement_duration=deep_engagement_periods,
        total_turns=len(turns),
        insights=insights,
    )

    # Convert to dict for return
    return {
        "turn_depth_stats": {
            "avg_depth": analysis.turn_depth_stats.avg_depth,
            "max_depth": analysis.turn_depth_stats.max_depth,
            "min_depth": analysis.turn_depth_stats.min_depth,
            "depth_variance": analysis.turn_depth_stats.depth_variance,
        },
        "nesting_histogram": analysis.nesting_histogram,
        "context_switches": [
            {
                "from_turn": cs.from_turn,
                "to_turn": cs.to_turn,
                "from_depth": cs.from_depth,
                "to_depth": cs.to_depth,
                "depth_delta": cs.depth_delta,
            }
            for cs in analysis.context_switches
        ],
        "deep_engagement_duration": [
            {
                "start_turn": period.start_turn,
                "end_turn": period.end_turn,
                "duration_turns": period.duration_turns,
                "avg_depth": period.avg_depth,
                "max_depth": period.max_depth,
            }
            for period in analysis.deep_engagement_duration
        ],
    }


def _calculate_turn_depth_stats(turns: Sequence[ConversationTurn]) -> TurnDepthStats:
    """Calculate statistical metrics for turn depths.

    Args:
        turns: Conversation turns

    Returns:
        TurnDepthStats with avg/max/min/variance
    """
    if not turns:
        return TurnDepthStats(avg_depth=0.0, max_depth=0, min_depth=0, depth_variance=0.0)

    depths = [turn.depth_level for turn in turns]

    avg_depth = sum(depths) / len(depths)
    max_depth = max(depths)
    min_depth = min(depths)

    # Calculate variance
    if len(depths) > 1:
        variance = sum((d - avg_depth) ** 2 for d in depths) / len(depths)
    else:
        variance = 0.0

    return TurnDepthStats(
        avg_depth=round(avg_depth, 2),
        max_depth=max_depth,
        min_depth=min_depth,
        depth_variance=round(variance, 3),
    )


def _build_nesting_histogram(turns: Sequence[ConversationTurn]) -> dict[int, int]:
    """Build histogram of turn depths.

    Args:
        turns: Conversation turns

    Returns:
        Dict mapping depth levels to counts
    """
    histogram: dict[int, int] = {}

    for turn in turns:
        depth = turn.depth_level
        histogram[depth] = histogram.get(depth, 0) + 1

    return histogram


def _detect_context_switches(turns: Sequence[ConversationTurn]) -> list[ContextSwitch]:
    """Detect context switches between depth levels.

    A context switch occurs when the depth level changes between consecutive turns.

    Args:
        turns: Conversation turns

    Returns:
        List of ContextSwitch objects
    """
    if len(turns) < 2:
        return []

    switches = []

    for i in range(1, len(turns)):
        prev_turn = turns[i - 1]
        curr_turn = turns[i]

        if prev_turn.depth_level != curr_turn.depth_level:
            switch = ContextSwitch(
                from_turn=prev_turn.turn_number,
                to_turn=curr_turn.turn_number,
                from_depth=prev_turn.depth_level,
                to_depth=curr_turn.depth_level,
                depth_delta=curr_turn.depth_level - prev_turn.depth_level,
            )
            switches.append(switch)

    return switches


def _identify_deep_engagement_periods(
    turns: Sequence[ConversationTurn],
) -> list[DeepEngagementPeriod]:
    """Identify sustained periods of deep conversation.

    A deep engagement period is a sequence of turns at depth >= DEEP_ENGAGEMENT_THRESHOLD
    lasting at least MIN_DEEP_ENGAGEMENT_TURNS.

    Args:
        turns: Conversation turns

    Returns:
        List of DeepEngagementPeriod objects
    """
    if not turns:
        return []

    periods = []
    current_period_start = None
    current_period_turns = []

    for turn in turns:
        if turn.depth_level >= DEEP_ENGAGEMENT_THRESHOLD:
            # In deep conversation
            if current_period_start is None:
                # Start new period
                current_period_start = turn.turn_number
                current_period_turns = [turn]
            else:
                # Continue period
                current_period_turns.append(turn)
        else:
            # Shallow conversation - end current period if exists
            if current_period_start is not None:
                # Check if period is long enough
                if len(current_period_turns) >= MIN_DEEP_ENGAGEMENT_TURNS:
                    depths = [t.depth_level for t in current_period_turns]
                    period = DeepEngagementPeriod(
                        start_turn=current_period_start,
                        end_turn=current_period_turns[-1].turn_number,
                        duration_turns=len(current_period_turns),
                        avg_depth=round(sum(depths) / len(depths), 2),
                        max_depth=max(depths),
                    )
                    periods.append(period)

                # Reset for next period
                current_period_start = None
                current_period_turns = []

    # Handle period that extends to end of conversation
    if current_period_start is not None and len(current_period_turns) >= MIN_DEEP_ENGAGEMENT_TURNS:
        depths = [t.depth_level for t in current_period_turns]
        period = DeepEngagementPeriod(
            start_turn=current_period_start,
            end_turn=current_period_turns[-1].turn_number,
            duration_turns=len(current_period_turns),
            avg_depth=round(sum(depths) / len(depths), 2),
            max_depth=max(depths),
        )
        periods.append(period)

    return periods


def _generate_turn_depth_insights(
    turn_depth_stats: TurnDepthStats,
    nesting_histogram: dict[int, int],
    context_switches: list[ContextSwitch],
    deep_engagement_periods: list[DeepEngagementPeriod],
    total_turns: int,
) -> list[str]:
    """Generate actionable insights about conversation turn depth.

    Args:
        turn_depth_stats: Turn depth statistics
        nesting_histogram: Depth distribution
        context_switches: Detected context switches
        deep_engagement_periods: Periods of deep engagement
        total_turns: Total number of turns

    Returns:
        List of insight strings
    """
    insights = []

    # Overall depth characteristics
    if turn_depth_stats.avg_depth < 1.0:
        insights.append(
            f"Shallow conversation (avg depth {turn_depth_stats.avg_depth:.1f}) - "
            "mostly surface-level interactions"
        )
    elif turn_depth_stats.avg_depth < 2.0:
        insights.append(
            f"Moderate depth (avg depth {turn_depth_stats.avg_depth:.1f}) - "
            "balanced between surface and deep exploration"
        )
    else:
        insights.append(
            f"Deep conversation (avg depth {turn_depth_stats.avg_depth:.1f}) - "
            "significant nested interaction complexity"
        )

    # Max depth insights
    if turn_depth_stats.max_depth == 0:
        insights.append("No nesting detected - entirely flat conversation structure")
    elif turn_depth_stats.max_depth >= 5:
        insights.append(
            f"Very deep nesting (max depth {turn_depth_stats.max_depth}) - "
            "highly complex interaction chains"
        )

    # Depth variance insights
    if turn_depth_stats.depth_variance > 2.0:
        insights.append(
            f"High depth variance ({turn_depth_stats.depth_variance:.2f}) - "
            "conversation alternates between shallow and deep"
        )
    elif turn_depth_stats.depth_variance < 0.5:
        insights.append(
            f"Low depth variance ({turn_depth_stats.depth_variance:.2f}) - "
            "consistent conversation depth maintained"
        )

    # Context switching insights
    if context_switches:
        switches_per_10_turns = (len(context_switches) / total_turns * 10) if total_turns > 0 else 0
        insights.append(
            f"{len(context_switches)} context switches detected "
            f"({switches_per_10_turns:.1f} per 10 turns)"
        )

        # Analyze switching patterns
        deep_dives = sum(1 for cs in context_switches if cs.depth_delta > 0)
        surfacings = sum(1 for cs in context_switches if cs.depth_delta < 0)

        if deep_dives > surfacings * 2:
            insights.append("Primarily deepening - conversation tends to explore in depth")
        elif surfacings > deep_dives * 2:
            insights.append("Primarily surfacing - conversation tends to return to surface topics")

    # Deep engagement insights
    if deep_engagement_periods:
        total_deep_turns = sum(p.duration_turns for p in deep_engagement_periods)
        deep_percentage = (total_deep_turns / total_turns * 100) if total_turns > 0 else 0

        insights.append(
            f"{len(deep_engagement_periods)} deep engagement period(s) "
            f"covering {deep_percentage:.1f}% of conversation"
        )

        # Longest deep period
        longest_period = max(deep_engagement_periods, key=lambda p: p.duration_turns)
        insights.append(
            f"Longest deep period: {longest_period.duration_turns} turns "
            f"(avg depth {longest_period.avg_depth:.1f})"
        )
    else:
        insights.append("No sustained deep engagement detected")

    # Distribution insights
    depth_0_count = nesting_histogram.get(0, 0)
    if depth_0_count > total_turns * 0.5:
        insights.append(
            f"{depth_0_count}/{total_turns} turns at depth 0 - "
            "conversation dominated by new topics rather than follow-ups"
        )

    return insights
