"""Agent preference learning analyzer for optimization pattern detection.

Detects and analyzes learned agent preferences over sessions to track
optimization pattern adoption, tool affinity development, and strategy evolution.
Helps understand how agents adapt their behavior based on experience.

Preference categories:
- Tool affinity: Preferred tools for specific task types
- Verification strategies: Read vs verify command patterns
- Read patterns: Offset/limit usage evolution
- Cache utilization: Cache adoption and effectiveness
- Optimization mode: Baseline vs optimized behavior adaptation

Evolution tracking:
- Trend analysis across sessions
- Adoption rate measurement
- Strategy consistency scoring
- Preference strength quantification
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, Callable


# Tool affinity thresholds
MIN_TOOL_USAGE_FOR_AFFINITY = 3  # Minimum uses to establish preference
STRONG_AFFINITY_THRESHOLD = 0.7  # 70%+ usage for a task type

# Optimization adoption thresholds
OPTIMIZATION_ADOPTED_THRESHOLD = 0.6  # 60%+ optimized behavior
OPTIMIZATION_EMERGING_THRESHOLD = 0.3  # 30-60% adoption

# Strategy evolution metrics
MIN_SESSIONS_FOR_EVOLUTION = 2  # Need at least 2 sessions to track evolution
SIGNIFICANT_CHANGE_THRESHOLD = 0.2  # 20%+ change is significant


@dataclass(frozen=True)
class ToolUsage:
    """Tool usage record for a specific task type."""

    tool_name: str
    task_type: str  # e.g., "file_read", "search", "verification"
    usage_count: int
    session_id: str


@dataclass(frozen=True)
class SessionBehavior:
    """Behavior patterns for a single session."""

    session_id: str
    session_number: int  # Chronological ordering
    tool_usages: list[ToolUsage]
    read_with_offset_count: int
    read_total_count: int
    verify_command_count: int
    cache_query_count: int
    cache_snapshot_count: int
    optimization_mode: str | None  # "baseline", "optimized", or None


@dataclass(frozen=True)
class ToolAffinityScore:
    """Tool affinity score for a specific task type."""

    task_type: str
    preferred_tool: str
    affinity_score: float  # 0-1 scale
    usage_count: int
    alternative_tools: dict[str, int]  # tool -> count


@dataclass(frozen=True)
class StrategyEvolution:
    """Evolution of a specific strategy over sessions."""

    strategy_name: str
    initial_adoption_rate: float  # 0-1 scale
    final_adoption_rate: float
    adoption_delta: float
    sessions_tracked: int
    trend: str  # "increasing", "decreasing", "stable"


@dataclass(frozen=True)
class OptimizationAdoption:
    """Optimization mode adoption metrics."""

    baseline_sessions: int
    optimized_sessions: int
    adoption_rate: float  # Proportion of optimized sessions
    avg_targeted_read_rate: float  # Average use of offset/limit in optimized
    avg_cache_utilization: float  # Average cache usage in optimized


@dataclass(frozen=True)
class AgentPreferenceLearningAnalysis:
    """Complete agent preference learning analysis."""

    preference_trends: dict[str, StrategyEvolution]  # strategy_name -> evolution
    strategy_evolution: list[StrategyEvolution]
    tool_affinity_scores: dict[str, ToolAffinityScore]  # task_type -> affinity
    optimization_adoption_rate: OptimizationAdoption
    total_sessions: int
    insights: list[str]


def analyze_agent_preference_learning(
    sessions: Sequence[SessionBehavior],
) -> dict:
    """Analyze agent preference learning and optimization pattern detection.

    Tracks preferred tools for specific tasks, verification strategy patterns,
    read offset/limit usage evolution, cache utilization trends, and
    optimization mode adaptation patterns.

    Args:
        sessions: Sequence of session behaviors in chronological order

    Returns:
        Dict with:
            - preference_trends: Dict of strategy evolutions by strategy name
            - strategy_evolution: List of StrategyEvolution objects
            - tool_affinity_scores: Dict of tool affinities by task type
            - optimization_adoption_rate: OptimizationAdoption metrics

    Raises:
        ValueError: If sessions contains invalid data
    """
    if not isinstance(sessions, (list, tuple)):
        raise ValueError("sessions must be a sequence (list or tuple)")

    # Validate sessions
    for session in sessions:
        if not isinstance(session, SessionBehavior):
            raise ValueError("sessions must contain SessionBehavior instances")
        if session.session_number < 0:
            raise ValueError("session_number must be non-negative")
        if session.read_total_count < 0:
            raise ValueError("read_total_count must be non-negative")
        if session.read_with_offset_count > session.read_total_count:
            raise ValueError("read_with_offset_count cannot exceed read_total_count")
        _validate_tool_usages(session)

    # Handle empty sessions
    if not sessions:
        return {
            "preference_trends": {},
            "strategy_evolution": [],
            "tool_affinity_scores": {},
            "optimization_adoption_rate": {
                "baseline_sessions": 0,
                "optimized_sessions": 0,
                "adoption_rate": 0.0,
                "avg_targeted_read_rate": 0.0,
                "avg_cache_utilization": 0.0,
            },
        }

    # Calculate tool affinity scores
    tool_affinity_scores = _calculate_tool_affinity_scores(sessions)

    # Track strategy evolution
    strategy_evolution = _track_strategy_evolution(sessions)

    # Build preference trends dict
    preference_trends = {s.strategy_name: s for s in strategy_evolution}

    # Calculate optimization adoption
    optimization_adoption = _calculate_optimization_adoption(sessions)

    # Generate insights
    insights = _generate_preference_insights(
        tool_affinity_scores=tool_affinity_scores,
        strategy_evolution=strategy_evolution,
        optimization_adoption=optimization_adoption,
        total_sessions=len(sessions),
    )

    # Build analysis object
    analysis = AgentPreferenceLearningAnalysis(
        preference_trends=preference_trends,
        strategy_evolution=strategy_evolution,
        tool_affinity_scores=tool_affinity_scores,
        optimization_adoption_rate=optimization_adoption,
        total_sessions=len(sessions),
        insights=insights,
    )

    # Convert to dict for return
    return {
        "preference_trends": {
            name: {
                "strategy_name": s.strategy_name,
                "initial_adoption_rate": s.initial_adoption_rate,
                "final_adoption_rate": s.final_adoption_rate,
                "adoption_delta": s.adoption_delta,
                "sessions_tracked": s.sessions_tracked,
                "trend": s.trend,
            }
            for name, s in analysis.preference_trends.items()
        },
        "strategy_evolution": [
            {
                "strategy_name": s.strategy_name,
                "initial_adoption_rate": s.initial_adoption_rate,
                "final_adoption_rate": s.final_adoption_rate,
                "adoption_delta": s.adoption_delta,
                "sessions_tracked": s.sessions_tracked,
                "trend": s.trend,
            }
            for s in analysis.strategy_evolution
        ],
        "tool_affinity_scores": {
            task_type: {
                "task_type": affinity.task_type,
                "preferred_tool": affinity.preferred_tool,
                "affinity_score": affinity.affinity_score,
                "usage_count": affinity.usage_count,
                "alternative_tools": affinity.alternative_tools,
            }
            for task_type, affinity in analysis.tool_affinity_scores.items()
        },
        "optimization_adoption_rate": {
            "baseline_sessions": analysis.optimization_adoption_rate.baseline_sessions,
            "optimized_sessions": analysis.optimization_adoption_rate.optimized_sessions,
            "adoption_rate": analysis.optimization_adoption_rate.adoption_rate,
            "avg_targeted_read_rate": analysis.optimization_adoption_rate.avg_targeted_read_rate,
            "avg_cache_utilization": analysis.optimization_adoption_rate.avg_cache_utilization,
        },
    }


def _validate_tool_usages(session: SessionBehavior) -> None:
    """Validate nested tool usage records before aggregation."""
    if not isinstance(session.tool_usages, (list, tuple)):
        raise ValueError("tool_usages must be a list or tuple")

    for usage in session.tool_usages:
        if not isinstance(usage, ToolUsage):
            raise ValueError("tool_usages must contain ToolUsage instances")
        if not usage.tool_name.strip():
            raise ValueError("tool_name must be non-blank")
        if not usage.task_type.strip():
            raise ValueError("task_type must be non-blank")
        if usage.usage_count < 0:
            raise ValueError("usage_count must be non-negative")
        if usage.session_id != session.session_id:
            raise ValueError("ToolUsage session_id must match parent SessionBehavior session_id")


def _calculate_tool_affinity_scores(
    sessions: Sequence[SessionBehavior],
) -> dict[str, ToolAffinityScore]:
    """Calculate tool affinity scores by task type.

    Args:
        sessions: Session behaviors

    Returns:
        Dict mapping task types to affinity scores
    """
    # Aggregate tool usage across all sessions
    task_tool_counts: dict[str, dict[str, int]] = {}  # task_type -> {tool -> count}

    for session in sessions:
        for usage in session.tool_usages:
            if usage.task_type not in task_tool_counts:
                task_tool_counts[usage.task_type] = {}

            tool = usage.tool_name
            task_tool_counts[usage.task_type][tool] = (
                task_tool_counts[usage.task_type].get(tool, 0) + usage.usage_count
            )

    # Calculate affinity scores
    affinity_scores = {}

    for task_type, tool_counts in task_tool_counts.items():
        if not tool_counts:
            continue

        # Find preferred tool (most used)
        preferred_tool = max(tool_counts.items(), key=lambda x: x[1])[0]
        preferred_count = tool_counts[preferred_tool]
        total_count = sum(tool_counts.values())

        # Calculate affinity score
        affinity_score = preferred_count / total_count if total_count > 0 else 0.0

        # Only create affinity if minimum usage is met
        if preferred_count >= MIN_TOOL_USAGE_FOR_AFFINITY:
            affinity = ToolAffinityScore(
                task_type=task_type,
                preferred_tool=preferred_tool,
                affinity_score=round(affinity_score, 3),
                usage_count=preferred_count,
                alternative_tools={
                    tool: count for tool, count in tool_counts.items() if tool != preferred_tool
                },
            )
            affinity_scores[task_type] = affinity

    return affinity_scores


def _track_strategy_evolution(
    sessions: Sequence[SessionBehavior],
) -> list[StrategyEvolution]:
    """Track evolution of strategies over sessions.

    Args:
        sessions: Session behaviors

    Returns:
        List of strategy evolution metrics
    """
    if len(sessions) < MIN_SESSIONS_FOR_EVOLUTION:
        return []

    evolutions = []

    # Strategy: Targeted reads (offset/limit usage)
    targeted_read_evolution = _track_metric_evolution(
        strategy_name="targeted_reads",
        sessions=sessions,
        metric_fn=lambda s: (
            s.read_with_offset_count / s.read_total_count if s.read_total_count > 0 else 0.0
        ),
    )
    if targeted_read_evolution:
        evolutions.append(targeted_read_evolution)

    # Strategy: Verification command usage
    verify_evolution = _track_metric_evolution(
        strategy_name="verification_commands",
        sessions=sessions,
        metric_fn=lambda s: (
            s.verify_command_count / s.read_total_count if s.read_total_count > 0 else 0.0
        ),
    )
    if verify_evolution:
        evolutions.append(verify_evolution)

    # Strategy: Cache utilization
    cache_evolution = _track_metric_evolution(
        strategy_name="cache_utilization",
        sessions=sessions,
        metric_fn=lambda s: s.cache_query_count + s.cache_snapshot_count,
    )
    if cache_evolution:
        evolutions.append(cache_evolution)

    return evolutions


def _track_metric_evolution(
    strategy_name: str,
    sessions: Sequence[SessionBehavior],
    metric_fn: Callable[[SessionBehavior], float],
) -> StrategyEvolution | None:
    """Track evolution of a specific metric over sessions.

    Args:
        strategy_name: Name of the strategy
        sessions: Session behaviors
        metric_fn: Function to extract metric from session

    Returns:
        StrategyEvolution or None if insufficient data
    """
    if not sessions:
        return None

    # Calculate metric for each session
    metrics = [metric_fn(s) for s in sessions]

    initial_rate = metrics[0]
    final_rate = metrics[-1]
    adoption_delta = final_rate - initial_rate

    # Determine trend
    if abs(adoption_delta) < SIGNIFICANT_CHANGE_THRESHOLD:
        trend = "stable"
    elif adoption_delta > 0:
        trend = "increasing"
    else:
        trend = "decreasing"

    return StrategyEvolution(
        strategy_name=strategy_name,
        initial_adoption_rate=round(initial_rate, 3),
        final_adoption_rate=round(final_rate, 3),
        adoption_delta=round(adoption_delta, 3),
        sessions_tracked=len(sessions),
        trend=trend,
    )


def _calculate_optimization_adoption(
    sessions: Sequence[SessionBehavior],
) -> OptimizationAdoption:
    """Calculate optimization mode adoption metrics.

    Args:
        sessions: Session behaviors

    Returns:
        OptimizationAdoption metrics
    """
    baseline_sessions = sum(1 for s in sessions if s.optimization_mode == "baseline")
    optimized_sessions = sum(1 for s in sessions if s.optimization_mode == "optimized")

    total_sessions = len(sessions)
    adoption_rate = optimized_sessions / total_sessions if total_sessions > 0 else 0.0

    # Calculate average targeted read rate in optimized sessions
    optimized_session_list = [s for s in sessions if s.optimization_mode == "optimized"]
    if optimized_session_list:
        targeted_read_rates = [
            s.read_with_offset_count / s.read_total_count if s.read_total_count > 0 else 0.0
            for s in optimized_session_list
        ]
        avg_targeted_read_rate = sum(targeted_read_rates) / len(targeted_read_rates)

        cache_utilizations = [
            s.cache_query_count + s.cache_snapshot_count for s in optimized_session_list
        ]
        avg_cache_utilization = sum(cache_utilizations) / len(cache_utilizations)
    else:
        avg_targeted_read_rate = 0.0
        avg_cache_utilization = 0.0

    return OptimizationAdoption(
        baseline_sessions=baseline_sessions,
        optimized_sessions=optimized_sessions,
        adoption_rate=round(adoption_rate, 3),
        avg_targeted_read_rate=round(avg_targeted_read_rate, 3),
        avg_cache_utilization=round(avg_cache_utilization, 2),
    )


def _generate_preference_insights(
    tool_affinity_scores: dict[str, ToolAffinityScore],
    strategy_evolution: list[StrategyEvolution],
    optimization_adoption: OptimizationAdoption,
    total_sessions: int,
) -> list[str]:
    """Generate actionable insights about preference learning.

    Args:
        tool_affinity_scores: Tool affinity scores
        strategy_evolution: Strategy evolution metrics
        optimization_adoption: Optimization adoption metrics
        total_sessions: Total number of sessions

    Returns:
        List of insight strings
    """
    insights = []

    # Tool affinity insights
    strong_affinities = [
        affinity
        for affinity in tool_affinity_scores.values()
        if affinity.affinity_score >= STRONG_AFFINITY_THRESHOLD
    ]

    if strong_affinities:
        insights.append(
            f"{len(strong_affinities)} strong tool affinity pattern(s) detected - "
            "agent shows consistent tool preferences"
        )
        for affinity in strong_affinities[:3]:  # Top 3
            insights.append(
                f"Strong preference for '{affinity.preferred_tool}' in {affinity.task_type} "
                f"({affinity.affinity_score:.1%} usage)"
            )

    # Strategy evolution insights
    increasing_strategies = [s for s in strategy_evolution if s.trend == "increasing"]
    decreasing_strategies = [s for s in strategy_evolution if s.trend == "decreasing"]

    if increasing_strategies:
        for strategy in increasing_strategies:
            if strategy.adoption_delta >= SIGNIFICANT_CHANGE_THRESHOLD:
                insights.append(
                    f"'{strategy.strategy_name}' adoption increasing "
                    f"({strategy.initial_adoption_rate:.1%} → {strategy.final_adoption_rate:.1%})"
                )

    if decreasing_strategies:
        for strategy in decreasing_strategies:
            if abs(strategy.adoption_delta) >= SIGNIFICANT_CHANGE_THRESHOLD:
                insights.append(
                    f"'{strategy.strategy_name}' adoption decreasing "
                    f"({strategy.initial_adoption_rate:.1%} → {strategy.final_adoption_rate:.1%})"
                )

    # Optimization adoption insights
    if optimization_adoption.adoption_rate >= OPTIMIZATION_ADOPTED_THRESHOLD:
        insights.append(
            f"Optimization mode well-adopted ({optimization_adoption.adoption_rate:.1%} of sessions) - "
            f"avg {optimization_adoption.avg_targeted_read_rate:.1%} targeted reads"
        )
    elif optimization_adoption.adoption_rate >= OPTIMIZATION_EMERGING_THRESHOLD:
        insights.append(
            f"Optimization mode emerging ({optimization_adoption.adoption_rate:.1%} of sessions) - "
            "adoption in progress"
        )
    elif optimization_adoption.optimized_sessions > 0:
        insights.append(
            f"Optimization mode experimental ({optimization_adoption.optimized_sessions} session(s)) - "
            "early adoption phase"
        )

    # Cache utilization insights
    if optimization_adoption.avg_cache_utilization > 5.0:
        insights.append(
            f"High cache utilization (avg {optimization_adoption.avg_cache_utilization:.1f} uses/session) - "
            "effective memory optimization"
        )
    elif optimization_adoption.avg_cache_utilization > 0:
        insights.append(
            f"Moderate cache utilization (avg {optimization_adoption.avg_cache_utilization:.1f} uses/session)"
        )

    # Overall learning insights
    if total_sessions >= 5 and len(strong_affinities) >= 2:
        insights.append(
            f"Clear preference learning over {total_sessions} sessions - "
            "agent has developed consistent optimization patterns"
        )

    return insights
