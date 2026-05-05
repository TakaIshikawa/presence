"""Idea realization lag analysis for content pipeline timing optimization.

Analyzes time between idea capture and content publication to identify
bottlenecks, calculate realization velocity, and optimize pipeline timing.

Metrics calculated:
- Lag distributions: Statistical distribution of realization times
- Bottleneck identification: Stages with highest delays
- Realization velocity: Ideas published per time unit
- Incomplete chain handling: Ideas without publication

Pipeline stages tracked:
- Capture: Idea created
- Refinement: Idea edited/enhanced
- Approval: Idea approved for publication
- Publication: Content published from idea
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional, Sequence


# Lag tier thresholds (in days)
TIER_FAST = "fast"
TIER_NORMAL = "normal"
TIER_SLOW = "slow"
TIER_STALLED = "stalled"
TIER_ORPHANED = "orphaned"

THRESHOLD_FAST_DAYS = 7  # <7 days is fast
THRESHOLD_NORMAL_DAYS = 30  # 7-30 days is normal
THRESHOLD_SLOW_DAYS = 90  # 30-90 days is slow
# >90 days is stalled, never published is orphaned

# Bottleneck thresholds
BOTTLENECK_THRESHOLD_RATIO = 2.0  # Stage is bottleneck if >2x median


@dataclass(frozen=True)
class IdeaRealizationChain:
    """Single idea's journey from capture to publication."""

    idea_id: str
    captured_at: datetime
    published_at: Optional[datetime]
    lag_days: Optional[float]  # None if not yet published


@dataclass(frozen=True)
class LagDistribution:
    """Statistical distribution of realization lag times."""

    min_days: float
    max_days: float
    median_days: float
    mean_days: float
    p25_days: float  # 25th percentile
    p75_days: float  # 75th percentile
    sample_size: int


@dataclass(frozen=True)
class PipelineBottleneck:
    """Identified bottleneck in the pipeline."""

    stage: str
    avg_duration_days: float
    median_duration_days: float
    severity: str  # critical, moderate, minor


@dataclass(frozen=True)
class RealizationVelocity:
    """Publication velocity metrics."""

    ideas_per_day: float
    ideas_per_week: float
    ideas_per_month: float
    observation_days: int


@dataclass(frozen=True)
class IdeaRealizationLagAnalysis:
    """Complete idea realization lag analysis."""

    lag_distribution: Optional[LagDistribution]  # None if no published ideas
    bottlenecks: Sequence[PipelineBottleneck]
    velocity: Optional[RealizationVelocity]  # None if no published ideas
    orphaned_count: int  # Ideas never published
    tier_counts: dict[str, int]  # Count per lag tier
    insights: list[str]


def analyze_idea_realization_lag(
    chains: Sequence[IdeaRealizationChain],
    observation_start: Optional[datetime] = None,
    observation_end: Optional[datetime] = None,
) -> IdeaRealizationLagAnalysis:
    """Analyze idea realization lag across multiple ideas.

    Args:
        chains: Sequence of idea realization chains
        observation_start: Optional start of observation period
        observation_end: Optional end of observation period

    Returns:
        IdeaRealizationLagAnalysis with distributions, bottlenecks, and insights

    Raises:
        ValueError: If chains contains invalid data
    """
    if not isinstance(chains, (list, tuple)):
        raise ValueError("chains must be a sequence (list or tuple)")

    # Validate chains
    for chain in chains:
        if not isinstance(chain, IdeaRealizationChain):
            raise ValueError("chains must contain IdeaRealizationChain instances")
        if chain.captured_at.tzinfo is None:
            raise ValueError("captured_at must be timezone-aware")
        if chain.published_at is not None and chain.published_at.tzinfo is None:
            raise ValueError("published_at must be timezone-aware or None")

    # Separate published and orphaned ideas
    published = [c for c in chains if c.published_at is not None]
    orphaned = [c for c in chains if c.published_at is None]

    # Calculate lag distribution for published ideas
    if published:
        lag_distribution = _calculate_lag_distribution(published)
        velocity = _calculate_realization_velocity(
            published, observation_start, observation_end
        )
    else:
        lag_distribution = None
        velocity = None

    # Identify bottlenecks (currently simplified - no per-stage data)
    bottlenecks = []  # Would need per-stage timestamps to implement

    # Calculate tier counts
    tier_counts = _calculate_tier_counts(chains)

    # Generate insights
    insights = _generate_insights(
        lag_distribution=lag_distribution,
        velocity=velocity,
        orphaned_count=len(orphaned),
        tier_counts=tier_counts,
        total_ideas=len(chains),
    )

    return IdeaRealizationLagAnalysis(
        lag_distribution=lag_distribution,
        bottlenecks=tuple(bottlenecks),
        velocity=velocity,
        orphaned_count=len(orphaned),
        tier_counts=tier_counts,
        insights=insights,
    )


def _calculate_lag_distribution(
    published_chains: Sequence[IdeaRealizationChain],
) -> LagDistribution:
    """Calculate statistical distribution of lag times.

    Args:
        published_chains: Chains with published content (lag_days not None)

    Returns:
        LagDistribution with statistical metrics
    """
    if not published_chains:
        raise ValueError("Cannot calculate distribution with no published chains")

    # Extract lag days (filter out None, though shouldn't happen)
    lags = [c.lag_days for c in published_chains if c.lag_days is not None]

    if not lags:
        raise ValueError("No valid lag values found")

    # Sort for percentile calculations
    sorted_lags = sorted(lags)
    n = len(sorted_lags)

    # Calculate statistics
    min_days = sorted_lags[0]
    max_days = sorted_lags[-1]
    mean_days = sum(sorted_lags) / n

    # Median
    if n % 2 == 0:
        median_days = (sorted_lags[n // 2 - 1] + sorted_lags[n // 2]) / 2
    else:
        median_days = sorted_lags[n // 2]

    # Percentiles
    p25_idx = max(0, int(n * 0.25) - 1)
    p75_idx = min(n - 1, int(n * 0.75))
    p25_days = sorted_lags[p25_idx]
    p75_days = sorted_lags[p75_idx]

    return LagDistribution(
        min_days=round(min_days, 2),
        max_days=round(max_days, 2),
        median_days=round(median_days, 2),
        mean_days=round(mean_days, 2),
        p25_days=round(p25_days, 2),
        p75_days=round(p75_days, 2),
        sample_size=n,
    )


def _calculate_realization_velocity(
    published_chains: Sequence[IdeaRealizationChain],
    observation_start: Optional[datetime],
    observation_end: Optional[datetime],
) -> RealizationVelocity:
    """Calculate publication velocity metrics.

    Args:
        published_chains: Chains with published content
        observation_start: Start of observation period
        observation_end: End of observation period

    Returns:
        RealizationVelocity with per-day/week/month metrics
    """
    if not published_chains:
        raise ValueError("Cannot calculate velocity with no published chains")

    # Determine observation period
    if observation_start and observation_end:
        start = observation_start
        end = observation_end
    else:
        # Use first capture to last publication
        start = min(c.captured_at for c in published_chains)
        end = max(c.published_at for c in published_chains if c.published_at)

    # Calculate observation duration
    duration = (end - start).total_seconds() / 86400.0  # days

    if duration <= 0:
        # All published same day
        duration = 1.0

    count = len(published_chains)

    # Calculate velocities
    ideas_per_day = count / duration
    ideas_per_week = ideas_per_day * 7
    ideas_per_month = ideas_per_day * 30

    return RealizationVelocity(
        ideas_per_day=round(ideas_per_day, 3),
        ideas_per_week=round(ideas_per_week, 2),
        ideas_per_month=round(ideas_per_month, 2),
        observation_days=int(round(duration)),
    )


def _calculate_tier_counts(chains: Sequence[IdeaRealizationChain]) -> dict[str, int]:
    """Calculate count of ideas in each lag tier.

    Args:
        chains: All idea chains (published and orphaned)

    Returns:
        Dictionary mapping tier names to counts
    """
    counts = {
        TIER_FAST: 0,
        TIER_NORMAL: 0,
        TIER_SLOW: 0,
        TIER_STALLED: 0,
        TIER_ORPHANED: 0,
    }

    for chain in chains:
        tier = _classify_lag_tier(chain.lag_days)
        counts[tier] += 1

    return counts


def _classify_lag_tier(lag_days: Optional[float]) -> str:
    """Classify lag into tier.

    Args:
        lag_days: Days from capture to publication, None if orphaned

    Returns:
        Tier name
    """
    if lag_days is None:
        return TIER_ORPHANED

    if lag_days < THRESHOLD_FAST_DAYS:
        return TIER_FAST
    elif lag_days < THRESHOLD_NORMAL_DAYS:
        return TIER_NORMAL
    elif lag_days < THRESHOLD_SLOW_DAYS:
        return TIER_SLOW
    else:
        return TIER_STALLED


def _generate_insights(
    lag_distribution: Optional[LagDistribution],
    velocity: Optional[RealizationVelocity],
    orphaned_count: int,
    tier_counts: dict[str, int],
    total_ideas: int,
) -> list[str]:
    """Generate actionable insights for pipeline optimization.

    Args:
        lag_distribution: Lag distribution stats
        velocity: Realization velocity metrics
        orphaned_count: Number of orphaned ideas
        tier_counts: Count per tier
        total_ideas: Total number of ideas analyzed

    Returns:
        List of actionable insights
    """
    insights = []

    # Overall pipeline health
    if total_ideas == 0:
        insights.append("No ideas to analyze - pipeline empty")
        return insights

    published_count = total_ideas - orphaned_count
    if published_count == 0:
        insights.append(
            f"No published content from {total_ideas} ideas - critical pipeline blockage"
        )
        return insights

    # Realization rate
    realization_rate = published_count / total_ideas
    insights.append(
        f"Realization rate: {realization_rate:.1%} ({published_count}/{total_ideas} ideas published)"
    )

    # Orphaned ideas
    if orphaned_count > 0:
        orphan_rate = orphaned_count / total_ideas
        if orphan_rate > 0.5:
            insights.append(
                f"High orphan rate: {orphan_rate:.1%} ({orphaned_count} ideas) never published - "
                "review idea quality or approval process"
            )
        elif orphan_rate > 0.2:
            insights.append(
                f"Moderate orphan rate: {orphan_rate:.1%} ({orphaned_count} ideas) - "
                "consider pipeline bottleneck analysis"
            )

    # Lag insights
    if lag_distribution:
        insights.append(
            f"Median realization lag: {lag_distribution.median_days:.1f} days "
            f"(range: {lag_distribution.min_days:.1f}-{lag_distribution.max_days:.1f} days)"
        )

        # Fast tier
        fast_count = tier_counts[TIER_FAST]
        if fast_count > 0 and published_count > 0:
            fast_rate = fast_count / published_count
            if fast_rate > 0.5:
                insights.append(
                    f"Healthy fast turnaround: {fast_rate:.1%} published within {THRESHOLD_FAST_DAYS} days"
                )

        # Stalled tier
        stalled_count = tier_counts[TIER_STALLED]
        if stalled_count > 0 and published_count > 0:
            stalled_rate = stalled_count / published_count
            if stalled_rate > 0.3:
                insights.append(
                    f"High stall rate: {stalled_rate:.1%} took >{THRESHOLD_SLOW_DAYS} days - "
                    "investigate pipeline delays"
                )

        # Velocity insights
        if velocity:
            if velocity.ideas_per_week < 1.0:
                insights.append(
                    f"Low velocity: {velocity.ideas_per_month:.1f} ideas/month - "
                    "consider increasing pipeline throughput"
                )
            elif velocity.ideas_per_week > 5.0:
                insights.append(
                    f"High velocity: {velocity.ideas_per_week:.1f} ideas/week - "
                    "strong pipeline performance"
                )

        # Distribution spread
        if lag_distribution.max_days - lag_distribution.min_days > 60:
            insights.append(
                f"Wide lag variance ({lag_distribution.min_days:.0f}-{lag_distribution.max_days:.0f} days) - "
                "inconsistent pipeline timing"
            )

        # Interquartile range (IQR) insights
        iqr = lag_distribution.p75_days - lag_distribution.p25_days
        if iqr > 30:
            insights.append(
                f"Large IQR ({iqr:.0f} days) - significant timing variability in middle 50% of ideas"
            )

    return insights
