<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
"""

from __future__ import annotations

<<<<<<< HEAD
from typing import Any


def analyze_session_background_task_usage(records: object) -> dict[str, Any]:
    """Analyze background task usage patterns in agent sessions.

    Tracks usage of run_in_background parameter, measures duration and
    completion rates, and identifies missed opportunities for background
    execution.
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Bash, Task, etc.)
            - run_in_background: Boolean indicating background execution
<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

<<<<<<< HEAD
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
=======
    total_calls = 0
    background_count = 0
    tools_backgrounded: dict[str, int] = {}
    completed_tasks = 0
    abandoned_tasks = 0
    durations: list[float] = []
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
