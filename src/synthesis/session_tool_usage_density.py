"""Session tool usage density calculation for workflow pattern analysis.

Analyzes tool usage patterns in Claude sessions to identify workflow characteristics,
detect inefficiencies, and optimize development patterns.

Tool density metrics:
- Tools-per-turn ratio: Average number of tools used per conversation turn
- Tool clustering coefficient: Measure of tool usage concentration vs distribution
- Tool diversity index: Variety of different tools used in the session
- Burst density: Frequency of tool usage bursts (multiple tools in quick succession)

Usage patterns:
- Exploration: High diversity, low clustering (trying many different approaches)
- Focused execution: Low diversity, high clustering (repeated use of specific tools)
- Systematic development: Moderate diversity, moderate clustering (balanced workflow)
- Scattered work: High diversity, high clustering (context switching between tasks)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


# Tool diversity thresholds (normalized 0-1 scale)
DIVERSITY_LOW = 0.3  # Using 30% or fewer unique tools
DIVERSITY_MODERATE = 0.6  # Using 30-60% unique tools
DIVERSITY_HIGH = 0.6  # Using more than 60% unique tools

# Clustering thresholds (0-1 scale)
CLUSTERING_LOW = 0.3  # Tools evenly distributed
CLUSTERING_MODERATE = 0.6  # Some tool concentration
CLUSTERING_HIGH = 0.6  # High tool concentration

# Burst detection threshold (tools per turn)
BURST_THRESHOLD = 3.0  # 3+ tools per turn indicates burst activity


@dataclass(frozen=True)
class ToolUsageMetrics:
    """Tool usage metrics for a session."""

    tool_calls: Sequence[str]  # List of tool names in order
    turn_count: int  # Number of conversation turns
    unique_tool_count: int  # Number of unique tools used
    total_tool_count: int  # Total tool calls


@dataclass(frozen=True)
class SessionToolUsageDensity:
    """Session tool usage density analysis."""

    tools_per_turn: float  # Average tools per turn
    tool_clustering_coefficient: float  # 0-1 scale, concentration of tool usage
    tool_diversity_index: float  # 0-1 scale, variety of tools
    burst_density: float  # Proportion of turns with burst activity
    workflow_pattern: str  # exploration, focused, systematic, scattered
    metrics: ToolUsageMetrics  # Raw metrics
    insights: list[str]  # Actionable insights


def calculate_session_tool_usage_density(
    tool_calls: Sequence[str],
    turn_count: int,
) -> SessionToolUsageDensity:
    """Calculate tool usage density metrics for a session.

    Args:
        tool_calls: Ordered list of tool names used in the session
        turn_count: Number of conversation turns in the session

    Returns:
        SessionToolUsageDensity with metrics, pattern, and insights

    Raises:
        ValueError: If turn_count is negative or tool_calls contains invalid data
    """
    if turn_count < 0:
        raise ValueError("turn_count must be non-negative")

    if not isinstance(tool_calls, (list, tuple)):
        raise ValueError("tool_calls must be a sequence (list or tuple)")

    # Validate tool_calls contains strings
    if tool_calls:
        if not all(isinstance(tool, str) for tool in tool_calls):
            raise ValueError("tool_calls must contain only strings")

    # Handle empty session
    if turn_count == 0:
        return SessionToolUsageDensity(
            tools_per_turn=0.0,
            tool_clustering_coefficient=0.0,
            tool_diversity_index=0.0,
            burst_density=0.0,
            workflow_pattern="empty",
            metrics=ToolUsageMetrics(
                tool_calls=tuple(tool_calls),
                turn_count=0,
                unique_tool_count=0,
                total_tool_count=0,
            ),
            insights=["Empty session - no tool usage to analyze"],
        )

    total_tool_count = len(tool_calls)
    unique_tool_count = len(set(tool_calls)) if tool_calls else 0

    # Calculate metrics
    tools_per_turn = _calculate_tools_per_turn(total_tool_count, turn_count)
    tool_diversity = _calculate_tool_diversity_index(unique_tool_count, total_tool_count)
    clustering = _calculate_tool_clustering_coefficient(tool_calls)
    burst_density = _calculate_burst_density(tools_per_turn)

    metrics = ToolUsageMetrics(
        tool_calls=tuple(tool_calls),
        turn_count=turn_count,
        unique_tool_count=unique_tool_count,
        total_tool_count=total_tool_count,
    )

    # Classify workflow pattern
    pattern = _classify_workflow_pattern(tool_diversity, clustering)

    # Generate insights
    insights = _generate_insights(
        tools_per_turn=tools_per_turn,
        diversity=tool_diversity,
        clustering=clustering,
        burst_density=burst_density,
        pattern=pattern,
        metrics=metrics,
    )

    return SessionToolUsageDensity(
        tools_per_turn=round(tools_per_turn, 2),
        tool_clustering_coefficient=round(clustering, 3),
        tool_diversity_index=round(tool_diversity, 3),
        burst_density=round(burst_density, 3),
        workflow_pattern=pattern,
        metrics=metrics,
        insights=insights,
    )


def _calculate_tools_per_turn(total_tools: int, turns: int) -> float:
    """Calculate average tools per turn.

    Args:
        total_tools: Total number of tool calls
        turns: Number of conversation turns

    Returns:
        Average tools per turn (0 if no turns)
    """
    if turns == 0:
        return 0.0
    return total_tools / turns


def _calculate_tool_diversity_index(unique_tools: int, total_tools: int) -> float:
    """Calculate tool diversity index (0-1 normalized).

    Higher diversity means more variety in tool usage.

    Args:
        unique_tools: Number of unique tools used
        total_tools: Total number of tool calls

    Returns:
        Diversity index (0-1 scale)
    """
    if total_tools == 0:
        return 0.0
    return unique_tools / total_tools


def _calculate_tool_clustering_coefficient(tool_calls: Sequence[str]) -> float:
    """Calculate tool clustering coefficient (0-1 normalized).

    Measures how concentrated tool usage is. Higher clustering means
    repeated use of the same tools in sequence.

    Uses Gini coefficient approach:
    - 0.0: Perfectly even distribution (each tool used equally)
    - 1.0: Maximum concentration (one tool dominates)

    Args:
        tool_calls: Ordered list of tool names

    Returns:
        Clustering coefficient (0-1 scale)
    """
    if not tool_calls:
        return 0.0

    # Count tool frequencies
    from collections import Counter
    tool_counts = Counter(tool_calls)

    if len(tool_counts) == 1:
        # Only one unique tool - maximum clustering
        return 1.0

    # Calculate Gini coefficient
    # Sort counts in ascending order
    sorted_counts = sorted(tool_counts.values())
    n = len(sorted_counts)
    total = sum(sorted_counts)

    if total == 0:
        return 0.0

    # Gini = (2 * sum(i * count_i)) / (n * sum(count_i)) - (n + 1) / n
    weighted_sum = sum((i + 1) * count for i, count in enumerate(sorted_counts))
    gini = (2 * weighted_sum) / (n * total) - (n + 1) / n

    return gini


def _calculate_burst_density(tools_per_turn: float) -> float:
    """Calculate burst density (0-1 normalized).

    Measures what proportion of activity represents burst patterns
    (multiple tools used rapidly).

    Args:
        tools_per_turn: Average tools per turn

    Returns:
        Burst density (0-1 scale)
    """
    if tools_per_turn == 0:
        return 0.0

    # Normalize against burst threshold
    # If avg is at/above threshold, burst density is high
    return min(1.0, tools_per_turn / BURST_THRESHOLD)


def _classify_workflow_pattern(diversity: float, clustering: float) -> str:
    """Classify workflow pattern based on diversity and clustering.

    Patterns:
    - exploration: High diversity, low clustering (trying many approaches)
    - focused: Low diversity, high clustering (repeated use of specific tools)
    - systematic: Moderate diversity, moderate clustering (balanced workflow)
    - scattered: High diversity, high clustering (context switching)

    Args:
        diversity: Tool diversity index (0-1)
        clustering: Tool clustering coefficient (0-1)

    Returns:
        Workflow pattern name
    """
    high_diversity = diversity > DIVERSITY_HIGH
    low_diversity = diversity <= DIVERSITY_LOW
    high_clustering = clustering > CLUSTERING_HIGH
    low_clustering = clustering <= CLUSTERING_LOW

    if high_diversity and low_clustering:
        return "exploration"
    elif low_diversity and high_clustering:
        return "focused"
    elif high_diversity and high_clustering:
        return "scattered"
    else:
        return "systematic"


def _generate_insights(
    tools_per_turn: float,
    diversity: float,
    clustering: float,
    burst_density: float,
    pattern: str,
    metrics: ToolUsageMetrics,
) -> list[str]:
    """Generate actionable insights based on tool usage patterns.

    Args:
        tools_per_turn: Average tools per turn
        diversity: Tool diversity index
        clustering: Tool clustering coefficient
        burst_density: Burst density metric
        pattern: Workflow pattern classification
        metrics: Raw tool usage metrics

    Returns:
        List of actionable insights
    """
    insights = []

    # Pattern-based insights
    if pattern == "exploration":
        insights.append(
            f"Exploration workflow detected: trying {metrics.unique_tool_count} different tools "
            "with diverse approaches"
        )
        insights.append(
            "Consider: documenting successful approaches for future reference"
        )
    elif pattern == "focused":
        insights.append(
            f"Focused workflow detected: concentrated use of {metrics.unique_tool_count} tools "
            "with repeated patterns"
        )
        insights.append(
            "Efficiency: high - workflow shows consistency and familiarity"
        )
    elif pattern == "systematic":
        insights.append(
            "Systematic workflow detected: balanced tool usage with moderate variety"
        )
        insights.append(
            "Workflow quality: good balance between exploration and execution"
        )
    elif pattern == "scattered":
        insights.append(
            f"Scattered workflow detected: {metrics.unique_tool_count} tools with frequent switching"
        )
        insights.append(
            "Consider: breaking work into focused sessions to reduce context switching"
        )
    elif pattern == "empty":
        # Already handled in empty session case
        pass

    # Density-based insights
    if tools_per_turn > 0:
        if tools_per_turn < 1.0:
            insights.append(
                f"Low tool density ({tools_per_turn:.1f} tools/turn) - "
                "primarily conversation-based interaction"
            )
        elif tools_per_turn >= BURST_THRESHOLD:
            insights.append(
                f"High tool density ({tools_per_turn:.1f} tools/turn) - "
                "intensive tool usage indicating complex work"
            )

    # Burst pattern insights
    if burst_density > 0.7:
        insights.append(
            "High burst activity detected - rapid tool usage may indicate debugging or exploration"
        )

    # Diversity insights
    if diversity > 0.8 and metrics.total_tool_count > 10:
        insights.append(
            f"Very high tool diversity ({diversity:.1%}) - "
            "each tool used sparingly, potential learning or experimentation"
        )
    elif diversity < 0.2 and metrics.total_tool_count > 10:
        insights.append(
            f"Very low tool diversity ({diversity:.1%}) - "
            "heavy reliance on few tools, workflow may be automatable"
        )

    # No tools used
    if metrics.total_tool_count == 0 and metrics.turn_count > 0:
        insights.append(
            "No tools used despite multiple turns - purely conversational session"
        )

    return insights
