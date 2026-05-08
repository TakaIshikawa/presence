"""Session background task usage analyzer for async execution patterns.

Analyzes usage patterns of background tasks (run_in_background parameter) in
agent sessions to measure async execution efficiency and identify missed
opportunities for backgrounding long-running operations.

Background task metrics:
- Background task frequency: Rate of background task usage
- Task types: Which commands/tools are backgrounded
- Completion rate: Tasks completed vs abandoned
- Average duration: Mean execution time for background tasks
- Missed opportunities: Long-running commands that could be backgrounded

Usage patterns:
- Heavy background usage: Frequent async execution for efficiency
- No background usage: All tasks run synchronously
- Selective backgrounding: Strategic use for specific task types
"""

from __future__ import annotations

from collections import Counter
from typing import Any


def analyze_session_background_task_usage(records: object) -> dict[str, Any]:
    """Analyze background task usage patterns in agent sessions.

    Tracks background task invocations, completion rates, and identifies
    opportunities for improved async execution.

    Args:
        records: List of task invocation dictionaries with keys:
            - task_id: Task identifier
            - tool_name: Name of the tool (Bash, Task, etc.)
            - run_in_background: Whether task was backgrounded
            - completed: Whether task completed successfully
            - duration_ms: Optional task duration in milliseconds

    Returns:
        Dict with:
            - total_tasks: Total number of tasks invoked
            - background_tasks: Count of tasks run in background
            - background_rate: Percentage of tasks backgrounded
            - tool_distribution: Dict mapping tool names to usage counts
            - completion_rate: Percentage of background tasks completed
            - abandoned_tasks: Count of background tasks not completed
            - avg_duration_ms: Average duration of background tasks
            - missed_opportunities: Estimated count of missed backgrounding chances

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task invocation dictionaries")

    if not records:
        return _empty_result()

    total_tasks = 0
    background_tasks = 0
    tool_distribution: Counter[str] = Counter()
    completed_count = 0
    abandoned_count = 0
    durations: list[float] = []
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, dict):
            continue

        total_tasks += 1

        tool_name = _string(record.get("tool_name"))
        run_in_background = bool(record.get("run_in_background"))
        completed = bool(record.get("completed"))
        duration_ms = record.get("duration_ms")

        if run_in_background:
            background_tasks += 1
            if tool_name:
                tool_distribution[tool_name] += 1

            if completed:
                completed_count += 1
            else:
                abandoned_count += 1

            if isinstance(duration_ms, (int, float)) and duration_ms > 0:
                durations.append(float(duration_ms))
        else:
            # Check for missed opportunities (long-running synchronous tasks)
            if isinstance(duration_ms, (int, float)) and duration_ms > 5000:
                # Tasks taking more than 5 seconds could potentially be backgrounded
                missed_opportunities += 1

    # Calculate metrics
    background_rate = _percentage(background_tasks, total_tasks)
    completion_rate = _percentage(completed_count, background_tasks)
    avg_duration = _average(sum(durations), len(durations))

    return {
        "total_tasks": total_tasks,
        "background_tasks": background_tasks,
        "background_rate": background_rate,
        "tool_distribution": dict(tool_distribution),
        "completion_rate": completion_rate,
        "abandoned_tasks": abandoned_count,
        "avg_duration_ms": avg_duration,
        "missed_opportunities": missed_opportunities,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tasks": 0,
        "background_tasks": 0,
        "background_rate": 0.0,
        "tool_distribution": {},
        "completion_rate": 0.0,
        "abandoned_tasks": 0,
        "avg_duration_ms": 0.0,
        "missed_opportunities": 0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)
