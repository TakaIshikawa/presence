"""Session background task usage analyzer for efficiency optimization.

Analyzes usage patterns of background tasks (run_in_background parameter)
in agent sessions. Background tasks allow agents to run long-running operations
concurrently while continuing with other work, improving session efficiency.

Usage metrics:
- Background task frequency: How often background execution is used
- Task types: Which commands/tools are run in background
- Duration metrics: Average duration of background tasks
- Completion rate: Percentage of background tasks that complete vs abandon
- Efficiency impact: Correlation with overall session speed

Opportunity detection:
- Long-running commands that could be backgrounded
- Sequential patterns where backgrounding would help
- Abandoned background tasks that weren't properly awaited
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_background_task_usage(records: object) -> dict[str, Any]:
    """Analyze background task usage patterns in agent sessions.

    Tracks when agents use run_in_background parameter, measures task
    durations and completion rates, and identifies missed opportunities.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Bash, Task, etc.)
            - run_in_background: Boolean indicating background execution
            - duration: Optional task duration in seconds
            - completed: Optional boolean indicating task completion
            - turn_index: Turn number when task was invoked

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - background_task_count: Number of background tasks
            - foreground_task_count: Number of foreground tasks
            - background_usage_rate: Percentage of tasks run in background
            - background_tool_distribution: Counter of tools used in background
            - average_duration: Average duration of background tasks (if available)
            - completion_rate: Percentage of background tasks that completed
            - abandoned_count: Number of background tasks abandoned
            - missed_opportunities: Estimated long tasks that could be backgrounded

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    background_task_count = 0
    foreground_task_count = 0
    background_tools: Counter[str] = Counter()
    durations: list[float] = []
    completed_count = 0
    abandoned_count = 0
    missed_opportunities = 0

    # Track long-running foreground tasks
    long_foreground_tasks: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        is_background = record.get("run_in_background") is True
        duration = _extract_duration(record)
        is_completed = record.get("completed")

        if is_background:
            background_task_count += 1
            background_tools[tool_name] += 1

            if duration is not None:
                durations.append(duration)

            # Track completion status
            if is_completed is True:
                completed_count += 1
            elif is_completed is False:
                abandoned_count += 1
        else:
            foreground_task_count += 1

            # Check for missed opportunity: long-running foreground task
            if duration is not None and duration > 30.0:  # > 30 seconds
                if _is_backgroundable_tool(tool_name):
                    missed_opportunities += 1
                    long_foreground_tasks.append({
                        "tool_name": tool_name,
                        "duration": duration,
                        "turn_index": record.get("turn_index", 0),
                    })

    # Calculate metrics
    background_usage_rate = _percentage(background_task_count, total_tool_calls)
    average_duration = _average(durations)
    completion_rate = _percentage(completed_count, background_task_count)

    # Format tool distribution
    tool_distribution = [
        {"tool_name": tool, "count": count}
        for tool, count in background_tools.most_common(5)
    ]

    return {
        "total_tool_calls": total_tool_calls,
        "background_task_count": background_task_count,
        "foreground_task_count": foreground_task_count,
        "background_usage_rate": background_usage_rate,
        "background_tool_distribution": tool_distribution,
        "average_duration": average_duration,
        "completion_rate": completion_rate,
        "abandoned_count": abandoned_count,
        "missed_opportunities": missed_opportunities,
        "long_foreground_examples": long_foreground_tasks[:3],  # Limit to 3 examples
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_duration(record: Mapping[str, Any]) -> float | None:
    """Extract duration from record if available."""
    duration = record.get("duration")
    if isinstance(duration, (int, float)) and not isinstance(duration, bool):
        return float(duration)
    return None


def _is_backgroundable_tool(tool_name: str) -> bool:
    """Check if a tool is suitable for background execution.

    Backgroundable tools:
    - Bash: For long-running commands (builds, tests, installs)
    - Task: For subagent execution
    - WebFetch: For network operations

    Not backgroundable:
    - Read, Write, Edit: Fast file operations
    - Grep, Glob: Fast search operations
    """
    tool_lower = tool_name.lower()
    backgroundable = {"bash", "task", "webfetch", "websearch"}
    return tool_lower in backgroundable


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
