"""Session task execution timeout analyzer.

Analyzes task execution timeouts and duration patterns across sessions. Identifies
tasks that exceeded expected duration thresholds, calculates timeout rates, and
reports statistics on task duration distribution.

Timeout metrics:
- Timeout rate: Percentage of tasks exceeding duration thresholds
- Duration distribution: Statistics on task execution times
- Longest-running tasks: Tasks with highest execution duration
- Average task duration: Mean execution time across all tasks
- Timeout frequency: Number of tasks exceeding thresholds

Quality indicators:
- Low timeout rate (<10%): Most tasks complete within expected duration
- Consistent durations: Low variance in task execution times
- Few long-running tasks: Minimal tasks exceeding thresholds
- Efficient execution: Average duration within acceptable range
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypedDict


EVENT_TASK_STARTED = "task_started"
EVENT_TASK_COMPLETED = "task_completed"
EVENT_TASK_TIMEOUT = "task_timeout"

# Default timeout threshold in seconds (5 minutes)
DEFAULT_TIMEOUT_THRESHOLD = 300


class LongRunningTaskDetail(TypedDict):
    """Details of a long-running task."""

    task_id: str
    description: str
    duration_seconds: int
    exceeded_threshold_by: int
    start_turn: int
    end_turn: int


class TaskDurationDetail(TypedDict):
    """Details of task duration."""

    task_id: str
    description: str
    duration_seconds: int
    start_turn: int
    end_turn: int
    timed_out: bool


@dataclass(frozen=True)
class TaskExecutionEvent:
    """Event in task execution tracking."""

    event_type: str
    turn_index: int
    task_id: str
    task_description: str = ""
    timestamp: int = 0  # Unix timestamp or relative time in seconds


@dataclass(frozen=True)
class TimeoutMetrics:
    """Metrics for task execution timeouts."""

    total_tasks: int
    completed_tasks: int
    timed_out_tasks: int
    timeout_rate: float
    avg_task_duration_seconds: float
    median_task_duration_seconds: float
    max_task_duration_seconds: int
    min_task_duration_seconds: int
    duration_variance: float


@dataclass(frozen=True)
class SessionTaskExecutionTimeoutAnalysis:
    """Complete analysis of task execution timeouts."""

    metrics: TimeoutMetrics
    long_running_tasks: tuple[LongRunningTaskDetail, ...]
    task_durations: tuple[TaskDurationDetail, ...]
    insights: tuple[str, ...]
    timeout_threshold_seconds: int


def analyze_session_task_execution_timeout(
    events: Sequence[TaskExecutionEvent],
    timeout_threshold: int = DEFAULT_TIMEOUT_THRESHOLD,
) -> SessionTaskExecutionTimeoutAnalysis:
    """Measure task execution timeouts and duration patterns.

    Args:
        events: Sequence of task execution events
        timeout_threshold: Duration threshold in seconds for timeout detection

    Returns:
        Analysis with timeout metrics, long-running tasks, and insights
    """
    _validate_events(events)
    _validate_timeout_threshold(timeout_threshold)

    if not events:
        return SessionTaskExecutionTimeoutAnalysis(
            metrics=TimeoutMetrics(0, 0, 0, 0.0, 0.0, 0.0, 0, 0, 0.0),
            long_running_tasks=(),
            task_durations=(),
            insights=("No events provided.",),
            timeout_threshold_seconds=timeout_threshold,
        )

    # Track task states
    active_tasks: dict[str, tuple[int, str, int]] = {}  # task_id -> (turn, desc, start_time)
    completed_durations: list[TaskDurationDetail] = []
    timed_out_tasks: list[str] = []

    for event in events:
        if event.event_type == EVENT_TASK_STARTED:
            if event.task_id not in active_tasks:
                active_tasks[event.task_id] = (
                    event.turn_index,
                    event.task_description,
                    event.timestamp,
                )

        elif event.event_type == EVENT_TASK_COMPLETED:
            if event.task_id in active_tasks:
                start_turn, desc, start_time = active_tasks[event.task_id]
                duration = event.timestamp - start_time
                timed_out = duration > timeout_threshold

                completed_durations.append({
                    "task_id": event.task_id,
                    "description": desc or event.task_description,
                    "duration_seconds": duration,
                    "start_turn": start_turn,
                    "end_turn": event.turn_index,
                    "timed_out": timed_out,
                })

                if timed_out:
                    timed_out_tasks.append(event.task_id)

                del active_tasks[event.task_id]

        elif event.event_type == EVENT_TASK_TIMEOUT:
            if event.task_id in active_tasks:
                start_turn, desc, start_time = active_tasks[event.task_id]
                duration = event.timestamp - start_time

                completed_durations.append({
                    "task_id": event.task_id,
                    "description": desc or event.task_description,
                    "duration_seconds": duration,
                    "start_turn": start_turn,
                    "end_turn": event.turn_index,
                    "timed_out": True,
                })

                timed_out_tasks.append(event.task_id)
                del active_tasks[event.task_id]

    # Calculate metrics
    total_tasks = len(completed_durations)
    completed_tasks = sum(1 for d in completed_durations if not d["timed_out"])
    timed_out_count = len(timed_out_tasks)
    timeout_rate = timed_out_count / total_tasks if total_tasks > 0 else 0.0

    durations = [d["duration_seconds"] for d in completed_durations]
    avg_duration = sum(durations) / len(durations) if durations else 0.0
    median_duration = _calculate_median(durations)
    max_duration = max(durations) if durations else 0
    min_duration = min(durations) if durations else 0
    variance = _calculate_variance(durations, avg_duration)

    # Identify long-running tasks
    long_running: list[LongRunningTaskDetail] = []
    for task_detail in completed_durations:
        if task_detail["duration_seconds"] > timeout_threshold:
            long_running.append({
                "task_id": task_detail["task_id"],
                "description": task_detail["description"],
                "duration_seconds": task_detail["duration_seconds"],
                "exceeded_threshold_by": task_detail["duration_seconds"] - timeout_threshold,
                "start_turn": task_detail["start_turn"],
                "end_turn": task_detail["end_turn"],
            })

    # Sort by duration descending
    long_running.sort(key=lambda x: x["duration_seconds"], reverse=True)

    metrics = TimeoutMetrics(
        total_tasks=total_tasks,
        completed_tasks=completed_tasks,
        timed_out_tasks=timed_out_count,
        timeout_rate=round(timeout_rate, 3),
        avg_task_duration_seconds=round(avg_duration, 1),
        median_task_duration_seconds=round(median_duration, 1),
        max_task_duration_seconds=max_duration,
        min_task_duration_seconds=min_duration,
        duration_variance=round(variance, 1),
    )

    return SessionTaskExecutionTimeoutAnalysis(
        metrics=metrics,
        long_running_tasks=tuple(long_running),
        task_durations=tuple(completed_durations),
        insights=_generate_insights(metrics, timeout_threshold),
        timeout_threshold_seconds=timeout_threshold,
    )


def _validate_events(events: Sequence[TaskExecutionEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, TaskExecutionEvent):
            raise ValueError("events must contain TaskExecutionEvent instances")

        if event.event_type not in {EVENT_TASK_STARTED, EVENT_TASK_COMPLETED, EVENT_TASK_TIMEOUT}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")

        last_turn = event.turn_index

        if not isinstance(event.task_id, str) or not event.task_id.strip():
            raise ValueError(
                f"event at index {index} must have a non-empty task_id"
            )

        if not isinstance(event.timestamp, int) or isinstance(event.timestamp, bool):
            raise ValueError(f"timestamp at index {index} must be an integer")

        if event.timestamp < 0:
            raise ValueError(f"timestamp at index {index} must be non-negative")


def _validate_timeout_threshold(threshold: int) -> None:
    """Validate timeout threshold value."""
    if not isinstance(threshold, int) or isinstance(threshold, bool):
        raise ValueError("timeout_threshold must be an integer")

    if threshold <= 0:
        raise ValueError("timeout_threshold must be positive")


def _calculate_median(values: list[int]) -> float:
    """Calculate median of numeric values."""
    if not values:
        return 0.0

    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2

    if n % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2.0
    else:
        return float(sorted_values[mid])


def _calculate_variance(values: list[int], mean: float) -> float:
    """Calculate variance of numeric values."""
    if not values:
        return 0.0

    squared_diffs = [(v - mean) ** 2 for v in values]
    return sum(squared_diffs) / len(squared_diffs)


def _generate_insights(metrics: TimeoutMetrics, threshold: int) -> tuple[str, ...]:
    """Generate human-readable insights about task execution timeouts."""
    if metrics.total_tasks == 0:
        return ("No tasks tracked in session.",)

    insights = [
        f"Tracked {metrics.total_tasks} task(s) with "
        f"{metrics.timed_out_tasks} timeout(s) "
        f"({metrics.timeout_rate:.1%} timeout rate)."
    ]

    if metrics.timeout_rate > 0.2:
        insights.append(
            f"High timeout rate ({metrics.timeout_rate:.1%}). "
            f"Consider optimizing long-running tasks or increasing threshold."
        )
    elif metrics.timeout_rate == 0.0:
        insights.append("No timeouts detected. All tasks completed within threshold.")

    if metrics.avg_task_duration_seconds > 0:
        insights.append(
            f"Average task duration: {metrics.avg_task_duration_seconds:.1f}s "
            f"(median: {metrics.median_task_duration_seconds:.1f}s)."
        )

    if metrics.max_task_duration_seconds > threshold:
        exceed_by = metrics.max_task_duration_seconds - threshold
        insights.append(
            f"Longest task exceeded threshold by {exceed_by}s "
            f"({metrics.max_task_duration_seconds}s total)."
        )

    if metrics.duration_variance > 10000:
        insights.append(
            f"High duration variance ({metrics.duration_variance:.1f}). "
            "Task execution times are inconsistent."
        )

    return tuple(insights)
