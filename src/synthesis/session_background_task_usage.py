"""Session background task usage analyzer for efficiency optimization.

Analyzes usage patterns of background tasks (run_in_background parameter) in agent
sessions. Tracks how effectively agents utilize background execution for long-running
operations like builds, tests, and async tool invocations.

Background task metrics:
- Usage frequency: Number and percentage of backgrounded operations
- Tool types: Which tools are run in background (Bash, Task, etc.)
- Duration patterns: Average time for background tasks
- Completion rates: Tasks completed vs abandoned
- Missed opportunities: Long-running commands that should have been backgrounded

Efficiency patterns:
- Optimal: High background usage for long operations
- Underutilized: Missed backgrounding opportunities
- Abandoned: Background tasks started but not checked
"""

from __future__ import annotations

from typing import Any, Mapping


# Commands that typically take >5 seconds and should be backgrounded
BACKGROUNDABLE_PATTERNS = (
    "npm install",
    "npm ci",
    "yarn install",
    "pip install",
    "cargo build",
    "npm run build",
    "npm build",
    "pytest",
    "npm test",
    "npm run test",
    "make",
    "docker build",
    "docker compose",
)


def analyze_session_background_task_usage(records: object) -> dict[str, Any]:
    """Analyze background task usage patterns in a session.

    Measures how effectively agents use the run_in_background parameter
    for long-running operations.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Bash, Task, etc.)
            - run_in_background: Boolean indicating background execution
            - command: Command string (for Bash)
            - duration_seconds: Execution time in seconds
            - was_checked: Whether task output was retrieved
            - was_completed: Whether task finished successfully
            - turn_index: Turn number when tool was called

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls
            - background_task_count: Number of background tasks
            - background_usage_rate: Percentage of calls backgrounded
            - tools_backgrounded: Dict mapping tool names to counts
            - completed_tasks: Number of completed background tasks
            - abandoned_tasks: Number of unchecked background tasks
            - completion_rate: Percentage of background tasks completed
            - average_duration: Average duration of background tasks
            - missed_opportunities: Count of long commands not backgrounded
            - efficiency_pattern: Classification of usage pattern

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_calls = 0
    background_count = 0
    tools_backgrounded: dict[str, int] = {}
    completed_tasks = 0
    abandoned_tasks = 0
    durations: list[float] = []
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_calls += 1

        is_background = record.get("run_in_background") is True

        if is_background:
            background_count += 1
            tools_backgrounded[tool_name] = tools_backgrounded.get(tool_name, 0) + 1

            # Track completion
            was_checked = record.get("was_checked") is True
            was_completed = record.get("was_completed") is True

            if was_completed:
                completed_tasks += 1
            elif not was_checked:
                abandoned_tasks += 1

            # Track duration
            duration = _number(record.get("duration_seconds"))
            if duration is not None and duration > 0:
                durations.append(duration)

        else:
            # Check for missed backgrounding opportunities
            if _is_backgroundable(record):
                duration = _number(record.get("duration_seconds"))
                # Consider it a missed opportunity if it took >5 seconds
                if duration is not None and duration > 5.0:
                    missed_opportunities += 1

    background_usage_rate = _percentage(background_count, total_calls)
    completion_rate = _percentage(completed_tasks, background_count)
    average_duration = _average(durations)
    efficiency_pattern = _classify_efficiency_pattern(
        background_usage_rate,
        completion_rate,
        abandoned_tasks,
        missed_opportunities,
    )

    return {
        "total_tool_calls": total_calls,
        "background_task_count": background_count,
        "background_usage_rate": background_usage_rate,
        "tools_backgrounded": tools_backgrounded,
        "completed_tasks": completed_tasks,
        "abandoned_tasks": abandoned_tasks,
        "completion_rate": completion_rate,
        "average_duration": average_duration,
        "missed_opportunities": missed_opportunities,
        "efficiency_pattern": efficiency_pattern,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _number(value: object) -> float | None:
    """Extract number from value, handling various types."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of values, returning 0.0 if empty."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _is_backgroundable(record: Mapping[str, Any]) -> bool:
    """Check if a tool call should have been run in background.

    Considers:
    - Bash commands matching common long-running patterns
    - Task tool invocations
    """
    tool_name = _string(record.get("tool_name")).lower()
    command = _string(record.get("command")).lower()

    # Bash commands with common long-running patterns
    if tool_name == "bash":
        for pattern in BACKGROUNDABLE_PATTERNS:
            if pattern.lower() in command:
                return True

    # Task tool invocations could often be backgrounded
    if tool_name == "task":
        return True

    return False


def _classify_efficiency_pattern(
    usage_rate: float,
    completion_rate: float,
    abandoned_tasks: int,
    missed_opportunities: int,
) -> str:
    """Classify background task usage efficiency pattern.

    Patterns:
    - optimal: High usage rate (>50%), high completion rate (>80%)
    - underutilized: Low usage rate (<10%) with missed opportunities
    - abandoned: Background tasks started but not checked (>3 abandoned)
    - effective: Moderate usage with good completion rates
    - minimal: Very low usage but no missed opportunities
    - empty: No tool calls or no background usage
    """
    if usage_rate == 0.0:
        if missed_opportunities > 2:
            return "underutilized"
        return "empty"

    # High abandonment rate (check first, overrides other patterns)
    # Either >3 abandoned tasks, or >1 when all tasks are abandoned
    if abandoned_tasks > 3 or (abandoned_tasks > 1 and completion_rate == 0.0):
        return "abandoned"

    # High usage with good completion
    if usage_rate > 50.0 and completion_rate > 80.0:
        return "optimal"

    # Low usage with missed opportunities
    if usage_rate < 10.0 and missed_opportunities > 2:
        return "underutilized"

    # Moderate to high usage with good completion
    if usage_rate >= 10.0 and completion_rate > 70.0:
        return "effective"

    # Low usage but no issues
    if usage_rate < 10.0 and missed_opportunities <= 2:
        return "minimal"

    # Default: some usage but mixed results
    return "mixed"
