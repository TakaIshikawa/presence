"""Session background task usage analyzer for async execution patterns.

Analyzes usage patterns of background tasks (run_in_background parameter) in
agent sessions. Tracks frequency, types, durations, and completion rates of
background operations.

Background task metrics:
- Usage frequency: How often background tasks are used
- Command types: Bash, Task tool, etc. run in background
- Duration distribution: How long background tasks typically run
- Completion vs abandonment: Tasks finished vs left running
- Efficiency correlation: Impact on overall session efficiency
- Missed opportunities: Commands that could have been backgrounded
"""

from __future__ import annotations

from typing import Any


def analyze_session_background_task_usage(records: object) -> dict[str, Any]:
    """Analyze background task usage patterns in agent sessions.

    Tracks usage of run_in_background parameter, measures duration and
    completion rates, and identifies missed opportunities for background
    execution.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Bash, Task, etc.)
            - run_in_background: Boolean indicating background execution
            - duration_seconds: Optional duration of the task
            - completed: Boolean indicating if task finished
            - command: Optional command string for Bash tasks

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls analyzed
            - background_task_count: Number of background tasks
            - background_usage_rate: Percentage of tasks run in background
            - background_tool_types: Distribution of tools run in background
            - avg_background_duration: Average duration of background tasks
            - completion_rate: Percentage of background tasks that completed
            - abandonment_rate: Percentage of background tasks abandoned
            - missed_opportunities: Potential background execution opportunities
            - efficiency_impact: Correlation with session efficiency

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    background_task_count = 0
    background_tool_types: dict[str, int] = {}
    background_durations: list[float] = []
    completed_count = 0
    abandoned_count = 0
    long_running_commands: list[dict[str, Any]] = []
    missed_opportunities: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        total_tool_calls += 1
        run_in_background = record.get("run_in_background", False)

        if run_in_background:
            background_task_count += 1

            # Track tool types
            background_tool_types[tool_name] = background_tool_types.get(tool_name, 0) + 1

            # Track duration
            duration = record.get("duration_seconds")
            if isinstance(duration, (int, float)) and duration > 0:
                background_durations.append(float(duration))

            # Track completion
            completed = record.get("completed", True)  # Default to True if not specified
            if completed:
                completed_count += 1
            else:
                abandoned_count += 1

        # Detect long-running commands that weren't backgrounded
        else:
            duration = record.get("duration_seconds")
            if isinstance(duration, (int, float)) and duration > 10.0:  # 10+ seconds
                command = _string(record.get("command"))
                long_running_commands.append({
                    "tool_name": tool_name,
                    "duration_seconds": duration,
                    "command": command[:100] if command else None,  # Limit to 100 chars
                })

    # Detect missed opportunities
    missed_opportunities = _detect_missed_opportunities(long_running_commands)

    # Calculate metrics
    background_usage_rate = round(
        (background_task_count / total_tool_calls * 100.0) if total_tool_calls > 0 else 0.0,
        2
    )

    avg_background_duration = round(
        sum(background_durations) / len(background_durations) if background_durations else 0.0,
        2
    )

    total_background_with_status = completed_count + abandoned_count
    completion_rate = round(
        (completed_count / total_background_with_status * 100.0) if total_background_with_status > 0 else 100.0,
        2
    )

    abandonment_rate = round(
        (abandoned_count / total_background_with_status * 100.0) if total_background_with_status > 0 else 0.0,
        2
    )

    # Calculate efficiency impact
    # Higher background usage with good completion = better efficiency
    efficiency_impact = _calculate_efficiency_impact(
        background_usage_rate,
        completion_rate
    )

    return {
        "total_tool_calls": total_tool_calls,
        "background_task_count": background_task_count,
        "background_usage_rate": background_usage_rate,
        "background_tool_types": background_tool_types,
        "avg_background_duration": avg_background_duration,
        "completion_rate": completion_rate,
        "abandonment_rate": abandonment_rate,
        "missed_opportunities": missed_opportunities[:10],  # Limit to 10
        "efficiency_impact": efficiency_impact,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _detect_missed_opportunities(
    long_running_commands: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Detect commands that could have been run in background.

    Looks for:
    - Long-running Bash commands (10+ seconds)
    - Build commands (npm run, cargo build, etc.)
    - Test commands (pytest, npm test, etc.)
    """
    opportunities: list[dict[str, Any]] = []

    for cmd_info in long_running_commands:
        tool_name = cmd_info["tool_name"]
        duration = cmd_info["duration_seconds"]
        command = cmd_info.get("command", "")

        # Only suggest background for Bash and Task tools
        if tool_name not in ("Bash", "Task"):
            continue

        # Check if it's a backgroundable command
        is_backgroundable = (
            # Build commands
            any(keyword in command for keyword in ["build", "compile", "make"]) or
            # Test commands
            any(keyword in command for keyword in ["test", "pytest", "jest", "npm test"]) or
            # Install commands
            any(keyword in command for keyword in ["install", "pip install", "npm install"]) or
            # Long-running general commands
            duration > 30.0  # Very long duration suggests background potential
        )

        if is_backgroundable:
            opportunities.append({
                "tool_name": tool_name,
                "duration_seconds": duration,
                "command_snippet": command[:80] if command else None,
                "reason": _determine_reason(command, duration),
            })

    return opportunities


def _determine_reason(command: str, duration: float) -> str:
    """Determine why command could be backgrounded."""
    if "build" in command or "compile" in command or "make" in command:
        return "build_command"
    elif "test" in command or "pytest" in command or "jest" in command:
        return "test_command"
    elif "install" in command:
        return "install_command"
    elif duration > 30.0:
        return "long_running"
    else:
        return "other"


def _calculate_efficiency_impact(
    background_usage_rate: float,
    completion_rate: float
) -> str:
    """Calculate efficiency impact classification.

    Returns:
    - "high": High usage with good completion
    - "medium": Moderate usage or mixed completion
    - "low": Low usage or poor completion
    - "none": No background usage
    """
    if background_usage_rate == 0.0:
        return "none"
    elif background_usage_rate > 20.0 and completion_rate > 80.0:
        return "high"
    elif background_usage_rate > 10.0 and completion_rate > 60.0:
        return "medium"
    else:
        return "low"
