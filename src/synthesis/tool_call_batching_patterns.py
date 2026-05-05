"""Tool call batching patterns analyzer for parallel execution optimization.

Analyzes tool call batching and parallelization effectiveness to optimize
workflow efficiency. Tracks batch size distribution, parallel vs sequential
execution patterns, and provides recommendations for optimal batching.

Batching metrics:
- Batch size distribution: Histogram of batch sizes (1 = sequential, 2+ = batched)
- Parallelization ratio: Proportion of tool calls executed in parallel
- Batching consistency: Variance in batch sizes across session
- Idle time analysis: Gaps between batches indicating missed opportunities
- Optimal batch recommendations: Suggested improvements based on patterns

Execution patterns:
- Fully sequential: All tool calls executed one at a time
- Opportunistic batching: Occasional parallel execution
- Consistent batching: Regular use of parallel execution
- Optimized batching: High parallelization with consistent batch sizes
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


# Parallelization thresholds
PARALLELIZATION_LOW = 0.3  # <30% of calls in batches
PARALLELIZATION_MODERATE = 0.6  # 30-60% in batches
PARALLELIZATION_HIGH = 0.6  # >60% in batches

# Consistency thresholds (variance)
CONSISTENCY_HIGH = 1.0  # Low variance
CONSISTENCY_MODERATE = 4.0  # Moderate variance
CONSISTENCY_LOW = 4.0  # High variance

# Idle time thresholds (seconds)
IDLE_TIME_SHORT = 5.0  # <5s between batches
IDLE_TIME_MODERATE = 30.0  # 5-30s between batches
IDLE_TIME_LONG = 30.0  # >30s between batches

# Optimal batch size recommendation
OPTIMAL_BATCH_SIZE_MIN = 2
OPTIMAL_BATCH_SIZE_MAX = 5


@dataclass(frozen=True)
class ToolBatch:
    """A single batch of tool calls."""

    batch_number: int  # Sequential batch number
    batch_size: int  # Number of tools in batch (1 = sequential)
    tool_names: tuple[str, ...]  # Tools in this batch
    timestamp_seconds: float  # Time since session start


@dataclass(frozen=True)
class BatchingStats:
    """Statistical metrics for batching patterns."""

    total_batches: int
    total_tool_calls: int
    avg_batch_size: float
    max_batch_size: int
    min_batch_size: int
    batch_size_variance: float
    single_call_batches: int  # Count of sequential (size 1) batches
    parallel_batches: int  # Count of parallel (size 2+) batches


@dataclass(frozen=True)
class IdleAnalysis:
    """Analysis of idle time between batches."""

    avg_idle_time_seconds: float
    max_idle_time_seconds: float
    idle_time_variance: float
    long_idle_periods: int  # Count of idle periods > IDLE_TIME_LONG


@dataclass(frozen=True)
class BatchingRecommendations:
    """Recommendations for batch optimization."""

    recommended_batch_size: int
    parallelization_opportunities: int  # Estimated sequential calls that could be batched
    consistency_improvement_potential: float  # 0-1 scale
    recommendations: list[str]  # Specific actionable recommendations


@dataclass(frozen=True)
class ToolCallBatchingAnalysis:
    """Complete tool call batching pattern analysis."""

    batching_stats: BatchingStats
    parallelization_ratio: float  # 0-1 scale
    consistency_score: float  # 0-100 scale, higher is more consistent
    idle_analysis: IdleAnalysis
    recommendations: BatchingRecommendations
    batch_size_distribution: dict[int, int]  # batch_size -> count
    insights: list[str]


def analyze_tool_call_batching_patterns(
    batches: Sequence[ToolBatch],
) -> dict:
    """Analyze tool call batching and parallelization effectiveness.

    Calculates batch size distribution, parallel vs sequential execution ratio,
    batching consistency score, idle time between batches, and provides
    optimal batch size recommendations.

    Args:
        batches: Sequence of tool batches in chronological order

    Returns:
        Dict with:
            - batching_stats: BatchingStats with size metrics
            - parallelization_ratio: Proportion of parallel execution
            - consistency_score: Batching consistency (0-100)
            - idle_analysis: IdleAnalysis with timing metrics
            - recommendations: BatchingRecommendations with optimizations

    Raises:
        ValueError: If batches contains invalid data
    """
    if not isinstance(batches, (list, tuple)):
        raise ValueError("batches must be a sequence (list or tuple)")

    # Validate batches
    for batch in batches:
        if not isinstance(batch, ToolBatch):
            raise ValueError("batches must contain ToolBatch instances")
        if batch.batch_number < 0:
            raise ValueError("batch_number must be non-negative")
        if batch.batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        if batch.timestamp_seconds < 0:
            raise ValueError("timestamp_seconds must be non-negative")
        if len(batch.tool_names) != batch.batch_size:
            raise ValueError("tool_names length must equal batch_size")

    # Handle empty batches
    if not batches:
        return {
            "batching_stats": {
                "total_batches": 0,
                "total_tool_calls": 0,
                "avg_batch_size": 0.0,
                "max_batch_size": 0,
                "min_batch_size": 0,
                "batch_size_variance": 0.0,
                "single_call_batches": 0,
                "parallel_batches": 0,
            },
            "parallelization_ratio": 0.0,
            "consistency_score": 0.0,
            "idle_analysis": {
                "avg_idle_time_seconds": 0.0,
                "max_idle_time_seconds": 0.0,
                "idle_time_variance": 0.0,
                "long_idle_periods": 0,
            },
            "recommendations": {
                "recommended_batch_size": OPTIMAL_BATCH_SIZE_MIN,
                "parallelization_opportunities": 0,
                "consistency_improvement_potential": 0.0,
                "recommendations": [],
            },
            "batch_size_distribution": {},
        }

    # Calculate batching stats
    batching_stats = _calculate_batching_stats(batches)

    # Build batch size distribution
    batch_size_distribution = _build_batch_size_distribution(batches)

    # Calculate parallelization ratio
    parallelization_ratio = _calculate_parallelization_ratio(batching_stats)

    # Calculate consistency score
    consistency_score = _calculate_consistency_score(batching_stats)

    # Analyze idle time
    idle_analysis = _analyze_idle_time(batches)

    # Generate recommendations
    recommendations = _generate_recommendations(
        batching_stats=batching_stats,
        parallelization_ratio=parallelization_ratio,
        consistency_score=consistency_score,
        batch_size_distribution=batch_size_distribution,
    )

    # Generate insights
    insights = _generate_batching_insights(
        batching_stats=batching_stats,
        parallelization_ratio=parallelization_ratio,
        consistency_score=consistency_score,
        idle_analysis=idle_analysis,
        recommendations=recommendations,
    )

    # Build analysis object
    analysis = ToolCallBatchingAnalysis(
        batching_stats=batching_stats,
        parallelization_ratio=parallelization_ratio,
        consistency_score=consistency_score,
        idle_analysis=idle_analysis,
        recommendations=recommendations,
        batch_size_distribution=batch_size_distribution,
        insights=insights,
    )

    # Convert to dict for return
    return {
        "batching_stats": {
            "total_batches": analysis.batching_stats.total_batches,
            "total_tool_calls": analysis.batching_stats.total_tool_calls,
            "avg_batch_size": analysis.batching_stats.avg_batch_size,
            "max_batch_size": analysis.batching_stats.max_batch_size,
            "min_batch_size": analysis.batching_stats.min_batch_size,
            "batch_size_variance": analysis.batching_stats.batch_size_variance,
            "single_call_batches": analysis.batching_stats.single_call_batches,
            "parallel_batches": analysis.batching_stats.parallel_batches,
        },
        "parallelization_ratio": analysis.parallelization_ratio,
        "consistency_score": analysis.consistency_score,
        "idle_analysis": {
            "avg_idle_time_seconds": analysis.idle_analysis.avg_idle_time_seconds,
            "max_idle_time_seconds": analysis.idle_analysis.max_idle_time_seconds,
            "idle_time_variance": analysis.idle_analysis.idle_time_variance,
            "long_idle_periods": analysis.idle_analysis.long_idle_periods,
        },
        "recommendations": {
            "recommended_batch_size": analysis.recommendations.recommended_batch_size,
            "parallelization_opportunities": analysis.recommendations.parallelization_opportunities,
            "consistency_improvement_potential": analysis.recommendations.consistency_improvement_potential,
            "recommendations": analysis.recommendations.recommendations,
        },
        "batch_size_distribution": analysis.batch_size_distribution,
    }


def _calculate_batching_stats(batches: Sequence[ToolBatch]) -> BatchingStats:
    """Calculate statistical metrics for batching patterns.

    Args:
        batches: Tool batches

    Returns:
        BatchingStats with size metrics
    """
    if not batches:
        return BatchingStats(
            total_batches=0,
            total_tool_calls=0,
            avg_batch_size=0.0,
            max_batch_size=0,
            min_batch_size=0,
            batch_size_variance=0.0,
            single_call_batches=0,
            parallel_batches=0,
        )

    batch_sizes = [batch.batch_size for batch in batches]

    total_batches = len(batches)
    total_tool_calls = sum(batch_sizes)
    avg_batch_size = total_tool_calls / total_batches
    max_batch_size = max(batch_sizes)
    min_batch_size = min(batch_sizes)

    # Calculate variance
    variance = sum((size - avg_batch_size) ** 2 for size in batch_sizes) / total_batches

    # Count sequential vs parallel
    single_call_batches = sum(1 for size in batch_sizes if size == 1)
    parallel_batches = sum(1 for size in batch_sizes if size > 1)

    return BatchingStats(
        total_batches=total_batches,
        total_tool_calls=total_tool_calls,
        avg_batch_size=round(avg_batch_size, 2),
        max_batch_size=max_batch_size,
        min_batch_size=min_batch_size,
        batch_size_variance=round(variance, 3),
        single_call_batches=single_call_batches,
        parallel_batches=parallel_batches,
    )


def _build_batch_size_distribution(batches: Sequence[ToolBatch]) -> dict[int, int]:
    """Build histogram of batch sizes.

    Args:
        batches: Tool batches

    Returns:
        Dict mapping batch sizes to counts
    """
    distribution: dict[int, int] = {}

    for batch in batches:
        size = batch.batch_size
        distribution[size] = distribution.get(size, 0) + 1

    return distribution


def _calculate_parallelization_ratio(stats: BatchingStats) -> float:
    """Calculate parallelization ratio.

    Args:
        stats: Batching statistics

    Returns:
        Ratio of parallel execution (0-1 scale)
    """
    if stats.total_batches == 0:
        return 0.0

    return round(stats.parallel_batches / stats.total_batches, 3)


def _calculate_consistency_score(stats: BatchingStats) -> float:
    """Calculate batching consistency score (0-100).

    Lower variance = higher consistency.

    Args:
        stats: Batching statistics

    Returns:
        Consistency score (0-100)
    """
    if stats.total_batches == 0:
        return 0.0

    # Normalize variance to 0-100 scale (inverse relationship)
    # Variance of 0 = 100 (perfect consistency)
    # Variance of 10+ = 0 (very inconsistent)
    max_variance = 10.0
    normalized_variance = min(stats.batch_size_variance / max_variance, 1.0)
    consistency = (1.0 - normalized_variance) * 100

    return round(consistency, 2)


def _analyze_idle_time(batches: Sequence[ToolBatch]) -> IdleAnalysis:
    """Analyze idle time between batches.

    Args:
        batches: Tool batches

    Returns:
        IdleAnalysis with timing metrics
    """
    if len(batches) < 2:
        return IdleAnalysis(
            avg_idle_time_seconds=0.0,
            max_idle_time_seconds=0.0,
            idle_time_variance=0.0,
            long_idle_periods=0,
        )

    # Calculate idle times between consecutive batches
    idle_times = []
    for i in range(1, len(batches)):
        idle_time = batches[i].timestamp_seconds - batches[i - 1].timestamp_seconds
        idle_times.append(idle_time)

    avg_idle_time = sum(idle_times) / len(idle_times)
    max_idle_time = max(idle_times)

    # Calculate variance
    variance = sum((t - avg_idle_time) ** 2 for t in idle_times) / len(idle_times)

    # Count long idle periods
    long_idle_periods = sum(1 for t in idle_times if t > IDLE_TIME_LONG)

    return IdleAnalysis(
        avg_idle_time_seconds=round(avg_idle_time, 2),
        max_idle_time_seconds=round(max_idle_time, 2),
        idle_time_variance=round(variance, 3),
        long_idle_periods=long_idle_periods,
    )


def _generate_recommendations(
    batching_stats: BatchingStats,
    parallelization_ratio: float,
    consistency_score: float,
    batch_size_distribution: dict[int, int],
) -> BatchingRecommendations:
    """Generate batching optimization recommendations.

    Args:
        batching_stats: Batching statistics
        parallelization_ratio: Parallelization ratio
        consistency_score: Consistency score
        batch_size_distribution: Batch size distribution

    Returns:
        BatchingRecommendations with optimization suggestions
    """
    recommendations_list = []

    # Recommend optimal batch size based on distribution
    if batch_size_distribution:
        # Find most common batch size (excluding size 1)
        parallel_sizes = {k: v for k, v in batch_size_distribution.items() if k > 1}
        if parallel_sizes:
            recommended_size = max(parallel_sizes.items(), key=lambda x: x[1])[0]
        else:
            recommended_size = OPTIMAL_BATCH_SIZE_MIN
    else:
        recommended_size = OPTIMAL_BATCH_SIZE_MIN

    # Estimate parallelization opportunities
    parallelization_opportunities = batching_stats.single_call_batches

    # Calculate consistency improvement potential
    consistency_improvement = (100.0 - consistency_score) / 100.0

    # Generate specific recommendations
    if parallelization_ratio < PARALLELIZATION_LOW:
        recommendations_list.append(
            f"Low parallelization ({parallelization_ratio:.1%}) - "
            f"consider batching {parallelization_opportunities} sequential calls"
        )

    if consistency_score < 50:
        recommendations_list.append(
            f"Inconsistent batch sizes (score {consistency_score:.0f}/100) - "
            f"standardize on batch size {recommended_size}"
        )

    if batching_stats.max_batch_size > OPTIMAL_BATCH_SIZE_MAX:
        recommendations_list.append(
            f"Large batches detected (max {batching_stats.max_batch_size}) - "
            f"consider splitting batches larger than {OPTIMAL_BATCH_SIZE_MAX}"
        )

    if parallelization_ratio >= PARALLELIZATION_HIGH and consistency_score >= 70:
        recommendations_list.append(
            "Good batching pattern - maintain current parallel execution strategy"
        )

    return BatchingRecommendations(
        recommended_batch_size=recommended_size,
        parallelization_opportunities=parallelization_opportunities,
        consistency_improvement_potential=round(consistency_improvement, 3),
        recommendations=recommendations_list,
    )


def _generate_batching_insights(
    batching_stats: BatchingStats,
    parallelization_ratio: float,
    consistency_score: float,
    idle_analysis: IdleAnalysis,
    recommendations: BatchingRecommendations,
) -> list[str]:
    """Generate actionable insights about batching patterns.

    Args:
        batching_stats: Batching statistics
        parallelization_ratio: Parallelization ratio
        consistency_score: Consistency score
        idle_analysis: Idle time analysis
        recommendations: Optimization recommendations

    Returns:
        List of insight strings
    """
    insights = []

    # Overall batching pattern
    if parallelization_ratio >= PARALLELIZATION_HIGH:
        insights.append(
            f"High parallelization ({parallelization_ratio:.1%}) - "
            f"{batching_stats.parallel_batches}/{batching_stats.total_batches} batches parallelized"
        )
    elif parallelization_ratio >= PARALLELIZATION_MODERATE:
        insights.append(
            f"Moderate parallelization ({parallelization_ratio:.1%}) - "
            "room for optimization"
        )
    else:
        insights.append(
            f"Low parallelization ({parallelization_ratio:.1%}) - "
            "mostly sequential execution"
        )

    # Batch size insights
    insights.append(
        f"Average batch size: {batching_stats.avg_batch_size:.1f} "
        f"(range {batching_stats.min_batch_size}-{batching_stats.max_batch_size})"
    )

    # Consistency insights
    if consistency_score >= 70:
        insights.append(
            f"High consistency (score {consistency_score:.0f}/100) - "
            "stable batching pattern"
        )
    elif consistency_score >= 50:
        insights.append(
            f"Moderate consistency (score {consistency_score:.0f}/100) - "
            "some variation in batch sizes"
        )
    else:
        insights.append(
            f"Low consistency (score {consistency_score:.0f}/100) - "
            "highly variable batch sizes"
        )

    # Idle time insights
    if idle_analysis.long_idle_periods > 0:
        insights.append(
            f"{idle_analysis.long_idle_periods} long idle period(s) detected "
            f"(>{IDLE_TIME_LONG:.0f}s) - potential workflow interruptions"
        )

    if idle_analysis.avg_idle_time_seconds < IDLE_TIME_SHORT:
        insights.append(
            f"Fast batch execution (avg {idle_analysis.avg_idle_time_seconds:.1f}s between batches)"
        )

    # Optimization potential
    if recommendations.parallelization_opportunities > 0:
        insights.append(
            f"{recommendations.parallelization_opportunities} parallelization opportunity(ies) identified"
        )

    return insights
