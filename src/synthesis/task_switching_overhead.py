"""Task switching overhead analyzer for workflow interruption tracking.

Calculates overhead from task context switches in Claude sessions to measure
productivity impact and identify high-frequency switching patterns that may
indicate workflow fragmentation or multitasking inefficiency.

Overhead metrics:
- Switch frequency: Rate of task transitions per session
- Average switch interval: Time between consecutive task changes
- Productivity impact: Estimated time lost to context switching
- Pattern detection: Identify rapid switching vs. focused work patterns

Patterns detected:
- Rapid switching: Frequent switches (< 5 min intervals) suggesting fragmentation
- Focused work: Long stretches (> 30 min) on single task
- Multitasking: Alternating between 2-3 tasks repeatedly
- Single-task sessions: No switching, deep focus
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import isfinite
from numbers import Real
from typing import Optional


# Overhead constants
CONTEXT_SWITCH_COST_MINUTES = 5.0  # Estimated minutes lost per switch
RAPID_SWITCH_THRESHOLD_MINUTES = 5.0  # < 5 min between switches is rapid
FOCUSED_WORK_THRESHOLD_MINUTES = 30.0  # > 30 min on task is focused

# Tier classifications
TIER_EFFICIENT = "efficient"  # Low switching overhead
TIER_MODERATE = "moderate"  # Normal switching patterns
TIER_FRAGMENTED = "fragmented"  # High switching overhead
TIER_CHAOTIC = "chaotic"  # Excessive switching

THRESHOLD_MODERATE = 15  # % of session time as overhead
THRESHOLD_FRAGMENTED = 30
THRESHOLD_CHAOTIC = 50


@dataclass(frozen=True)
class TaskSwitch:
    """Single task switch event."""

    from_task: str  # Task identifier being switched from
    to_task: str  # Task identifier being switched to
    timestamp: datetime  # When the switch occurred
    interval_minutes: Optional[float] = None  # Time since previous switch


@dataclass(frozen=True)
class SwitchingMetrics:
    """Task switching overhead metrics."""

    total_switches: int
    avg_interval_minutes: float
    rapid_switches: int  # Switches < 5 min apart
    focused_periods: int  # Periods > 30 min on same task
    overhead_minutes: float  # Total estimated time lost
    overhead_percentage: float  # Overhead as % of session
    unique_tasks: int  # Number of distinct tasks


@dataclass(frozen=True)
class TaskSwitchingOverhead:
    """Task switching overhead analysis result."""

    metrics: SwitchingMetrics
    overhead_tier: str  # Efficiency tier classification
    session_duration_minutes: float
    insights: list[str]  # Actionable insights


def analyze_task_switching_overhead(
    switches: list[TaskSwitch],
    session_duration_minutes: float,
) -> TaskSwitchingOverhead:
    """Analyze task switching overhead for a session.

    Args:
        switches: List of task switch events in chronological order
        session_duration_minutes: Total session duration in minutes

    Returns:
        TaskSwitchingOverhead with metrics, tier, and insights

    Raises:
        ValueError: If switches is not a list, contains invalid instances,
                   or has invalid values
    """
    # Validate inputs
    if not isinstance(switches, list):
        raise ValueError("switches must be a list")

    if (
        not isinstance(session_duration_minutes, Real)
        or isinstance(session_duration_minutes, bool)
        or not isfinite(session_duration_minutes)
    ):
        raise ValueError("session_duration_minutes must be a finite number")
    if session_duration_minutes <= 0:
        raise ValueError("session_duration_minutes must be positive")

    # Validate switch instances
    previous_timestamp: datetime | None = None
    for switch in switches:
        if not isinstance(switch, TaskSwitch):
            raise ValueError("All switches must be TaskSwitch instances")
        if not isinstance(switch.from_task, str) or not switch.from_task.strip():
            raise ValueError("from_task must be a non-empty string")
        if not isinstance(switch.to_task, str) or not switch.to_task.strip():
            raise ValueError("to_task must be a non-empty string")
        if switch.timestamp.tzinfo is None:
            raise ValueError("TaskSwitch timestamp must be timezone-aware")
        if previous_timestamp is not None and switch.timestamp < previous_timestamp:
            raise ValueError("TaskSwitch timestamps must be in chronological order")
        if switch.interval_minutes is not None and switch.interval_minutes < 0:
            raise ValueError("interval_minutes must be non-negative or None")
        previous_timestamp = switch.timestamp

    # Handle no-switch case
    if not switches:
        metrics = SwitchingMetrics(
            total_switches=0,
            avg_interval_minutes=0.0,
            rapid_switches=0,
            focused_periods=1,  # Entire session on one task
            overhead_minutes=0.0,
            overhead_percentage=0.0,
            unique_tasks=1,  # Assume single task if no switches
        )
        return TaskSwitchingOverhead(
            metrics=metrics,
            overhead_tier=TIER_EFFICIENT,
            session_duration_minutes=session_duration_minutes,
            insights=["No task switches - single-task focused session"],
        )

    # Calculate metrics
    avg_interval = _calculate_avg_interval(switches)
    rapid_switches = _count_rapid_switches(switches)
    focused_periods = _count_focused_periods(switches, session_duration_minutes)
    overhead_minutes = len(switches) * CONTEXT_SWITCH_COST_MINUTES
    overhead_percentage = (overhead_minutes / session_duration_minutes) * 100.0
    unique_tasks = _count_unique_tasks(switches)

    # Build metrics
    metrics = SwitchingMetrics(
        total_switches=len(switches),
        avg_interval_minutes=round(avg_interval, 2),
        rapid_switches=rapid_switches,
        focused_periods=focused_periods,
        overhead_minutes=round(overhead_minutes, 2),
        overhead_percentage=round(overhead_percentage, 2),
        unique_tasks=unique_tasks,
    )

    # Classify tier
    tier = _classify_overhead_tier(overhead_percentage)

    # Generate insights
    insights = _generate_insights(metrics, tier, session_duration_minutes)

    return TaskSwitchingOverhead(
        metrics=metrics,
        overhead_tier=tier,
        session_duration_minutes=session_duration_minutes,
        insights=insights,
    )


def _calculate_avg_interval(switches: list[TaskSwitch]) -> float:
    """Calculate average time between switches.

    Args:
        switches: List of task switches

    Returns:
        Average interval in minutes
    """
    if not switches:
        return 0.0

    intervals = [
        switch.interval_minutes
        for switch in switches
        if switch.interval_minutes is not None
    ]

    if not intervals:
        return 0.0

    return sum(intervals) / len(intervals)


def _count_rapid_switches(switches: list[TaskSwitch]) -> int:
    """Count switches that occurred rapidly (< 5 min apart).

    Args:
        switches: List of task switches

    Returns:
        Count of rapid switches
    """
    return sum(
        1
        for switch in switches
        if switch.interval_minutes is not None
        and switch.interval_minutes < RAPID_SWITCH_THRESHOLD_MINUTES
    )


def _count_focused_periods(
    switches: list[TaskSwitch],
    session_duration_minutes: float,
) -> int:
    """Count periods of focused work (> 30 min on same task).

    Args:
        switches: List of task switches
        session_duration_minutes: Total session duration

    Returns:
        Count of focused work periods
    """
    if not switches:
        # No switches = entire session focused on one task
        return 1 if session_duration_minutes >= FOCUSED_WORK_THRESHOLD_MINUTES else 0

    # Count intervals >= threshold
    focused_count = sum(
        1
        for switch in switches
        if switch.interval_minutes is not None
        and switch.interval_minutes >= FOCUSED_WORK_THRESHOLD_MINUTES
    )

    # Check if final period (after last switch) was focused
    # We need session end time to calculate this properly
    # For now, we'll just count intervals between switches
    return focused_count


def _count_unique_tasks(switches: list[TaskSwitch]) -> int:
    """Count number of unique tasks in session.

    Args:
        switches: List of task switches

    Returns:
        Count of unique tasks
    """
    if not switches:
        return 1

    tasks = set()
    for switch in switches:
        tasks.add(switch.from_task)
        tasks.add(switch.to_task)

    return len(tasks)


def _classify_overhead_tier(overhead_percentage: float) -> str:
    """Classify overhead percentage into tier.

    Args:
        overhead_percentage: Overhead as percentage of session

    Returns:
        Tier classification string
    """
    if overhead_percentage >= THRESHOLD_CHAOTIC:
        return TIER_CHAOTIC
    elif overhead_percentage >= THRESHOLD_FRAGMENTED:
        return TIER_FRAGMENTED
    elif overhead_percentage >= THRESHOLD_MODERATE:
        return TIER_MODERATE
    else:
        return TIER_EFFICIENT


def _generate_insights(
    metrics: SwitchingMetrics,
    tier: str,
    session_duration_minutes: float,
) -> list[str]:
    """Generate actionable insights for task switching overhead.

    Args:
        metrics: Switching metrics
        tier: Overhead tier classification
        session_duration_minutes: Session duration

    Returns:
        List of actionable insights
    """
    insights = []

    # Tier-based insights
    if tier == TIER_CHAOTIC:
        insights.append(
            f"Chaotic switching pattern ({metrics.overhead_percentage:.1f}% overhead) - "
            "excessive context switching severely impacting productivity"
        )
    elif tier == TIER_FRAGMENTED:
        insights.append(
            f"Fragmented workflow ({metrics.overhead_percentage:.1f}% overhead) - "
            "high task switching reducing efficiency"
        )
    elif tier == TIER_MODERATE:
        insights.append(
            f"Moderate switching overhead ({metrics.overhead_percentage:.1f}%) - "
            "normal multitasking patterns"
        )
    else:  # TIER_EFFICIENT
        insights.append(
            f"Efficient workflow ({metrics.overhead_percentage:.1f}% overhead) - "
            "minimal switching, good focus"
        )

    # Switch frequency insights
    if metrics.total_switches > 0:
        switches_per_hour = (metrics.total_switches / session_duration_minutes) * 60.0
        if switches_per_hour > 10:
            insights.append(
                f"Very high switch rate ({switches_per_hour:.1f} switches/hour) - "
                "consider batching similar tasks"
            )
        elif switches_per_hour > 5:
            insights.append(
                f"High switch rate ({switches_per_hour:.1f} switches/hour) - "
                "may benefit from better task grouping"
            )

    # Rapid switching insights
    if metrics.rapid_switches > 0:
        rapid_ratio = metrics.rapid_switches / metrics.total_switches if metrics.total_switches > 0 else 0
        if rapid_ratio > 0.5:
            insights.append(
                f"{metrics.rapid_switches} rapid switches (< {RAPID_SWITCH_THRESHOLD_MINUTES} min) - "
                "frequent interruptions preventing deep work"
            )
        elif metrics.rapid_switches >= 3:
            insights.append(
                f"{metrics.rapid_switches} rapid task changes detected - "
                "some workflow fragmentation"
            )

    # Focused work insights
    if metrics.focused_periods > 0:
        insights.append(
            f"{metrics.focused_periods} focused work period(s) (> {FOCUSED_WORK_THRESHOLD_MINUTES} min) - "
            "good depth of concentration"
        )
    elif metrics.total_switches > 5:
        insights.append(
            "No sustained focus periods - all tasks under 30 minutes"
        )

    # Task diversity insights
    if metrics.unique_tasks > 10:
        insights.append(
            f"Working across {metrics.unique_tasks} different tasks - "
            "high cognitive load from task diversity"
        )
    elif metrics.unique_tasks > 5:
        insights.append(
            f"{metrics.unique_tasks} distinct tasks in session - "
            "moderate task diversity"
        )
    elif metrics.total_switches > 0 and metrics.unique_tasks <= 3:
        # Multiple switches between few tasks = multitasking pattern
        insights.append(
            f"Alternating between {metrics.unique_tasks} tasks - "
            "multitasking pattern detected"
        )

    # Overhead impact insights
    if metrics.overhead_minutes > 60:
        insights.append(
            f"Estimated {metrics.overhead_minutes:.0f} minutes lost to context switching - "
            f"significant productivity impact"
        )
    elif metrics.overhead_minutes > 30:
        insights.append(
            f"~{metrics.overhead_minutes:.0f} minutes overhead from switching - "
            "moderate efficiency cost"
        )

    # Average interval insights
    if metrics.avg_interval_minutes > 0:
        if metrics.avg_interval_minutes < 10:
            insights.append(
                f"Average {metrics.avg_interval_minutes:.1f} min per task - "
                "very short task durations"
            )
        elif metrics.avg_interval_minutes > 45:
            insights.append(
                f"Average {metrics.avg_interval_minutes:.1f} min per task - "
                "good sustained attention spans"
            )

    return insights
