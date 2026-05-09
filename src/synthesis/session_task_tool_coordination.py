"""Session Task tool coordination analyzer for subagent usage patterns.

Analyzes Task tool (subagent) usage coordination in Claude Code sessions to measure
delegation patterns, parallelization effectiveness, and background task management.
Tracks how effectively the agent coordinates subagent invocations.

Task tool coordination metrics:
- Total Task invocations: Number of Task tool calls
- Subagent type distribution: Count by type (Bash/general-purpose/Explore/Plan)
- Average task description length: Mean character count of task prompts
- Parallel Task calls: Multiple Tasks in single response
- Background task rate: Percentage using run_in_background
- Task completion correlation: TaskOutput calls following Task calls

Quality indicators:
- Balanced subagent usage: Appropriate mix of Bash/Explore/general-purpose
- High parallel rate (>30%): Good batching of independent subagent work
- Moderate background rate (10-40%): Strategic async delegation
- High completion correlation (>80%): Good follow-through on Task results
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_task_tool_coordination(records: object) -> dict[str, Any]:
    """Analyze Task tool usage coordination in Claude Code sessions.

    Evaluates subagent delegation patterns, parallelization, background task
    usage, and TaskOutput correlation.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_task_calls: Number of Task tool invocations
            - bash_subagent_calls: Task calls with subagent_type "Bash"
            - general_subagent_calls: Task calls with "general-purpose"
            - explore_subagent_calls: Task calls with "Explore"
            - plan_subagent_calls: Task calls with "Plan"
            - total_description_length: Sum of task prompt lengths
            - parallel_task_calls: Multiple Tasks in single response
            - background_task_calls: Tasks with run_in_background=true
            - task_output_calls: TaskOutput tool invocations
            - correlated_outputs: TaskOutput calls matching Task IDs
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_task_tool: Count using Task tool
            - avg_task_calls: Average Task invocations per session
            - avg_bash_subagent_ratio: Average % Bash subagent usage
            - avg_general_subagent_ratio: Average % general-purpose usage
            - avg_explore_subagent_ratio: Average % Explore usage
            - avg_plan_subagent_ratio: Average % Plan usage
            - avg_task_description_length: Average prompt length
            - avg_parallel_task_ratio: Average % parallel Task calls
            - avg_background_task_ratio: Average % background tasks
            - avg_output_correlation_rate: Average % TaskOutput correlation
            - high_coordination_sessions: Count with >80% output correlation
            - low_coordination_sessions: Count with <50% output correlation

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_task = 0

    task_calls: list[int | float] = []
    bash_ratios: list[float] = []
    general_ratios: list[float] = []
    explore_ratios: list[float] = []
    plan_ratios: list[float] = []
    description_lengths: list[float] = []
    parallel_ratios: list[float] = []
    background_ratios: list[float] = []
    output_correlation_rates: list[float] = []

    high_coordination_sessions = 0  # >80% output correlation
    low_coordination_sessions = 0   # <50% output correlation

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_tasks = _extract_int(record.get("total_task_calls"))
        bash_calls = _extract_int(record.get("bash_subagent_calls"))
        general_calls = _extract_int(record.get("general_subagent_calls"))
        explore_calls = _extract_int(record.get("explore_subagent_calls"))
        plan_calls = _extract_int(record.get("plan_subagent_calls"))
        total_desc_length = _extract_int(record.get("total_description_length"))
        parallel_calls = _extract_int(record.get("parallel_task_calls"))
        background_calls = _extract_int(record.get("background_task_calls"))
        task_output_calls = _extract_int(record.get("task_output_calls"))
        correlated_outputs = _extract_int(record.get("correlated_outputs"))

        # Track sessions using Task tool
        if total_tasks is not None and total_tasks > 0:
            sessions_with_task += 1
            task_calls.append(total_tasks)

            # Calculate subagent type ratios
            if bash_calls is not None:
                bash_ratios.append(_percentage(bash_calls, total_tasks))
            if general_calls is not None:
                general_ratios.append(_percentage(general_calls, total_tasks))
            if explore_calls is not None:
                explore_ratios.append(_percentage(explore_calls, total_tasks))
            if plan_calls is not None:
                plan_ratios.append(_percentage(plan_calls, total_tasks))

            # Calculate average description length
            if total_desc_length is not None:
                avg_desc = total_desc_length / total_tasks
                description_lengths.append(avg_desc)

            # Calculate parallel task ratio
            if parallel_calls is not None:
                parallel_ratios.append(_percentage(parallel_calls, total_tasks))

            # Calculate background task ratio
            if background_calls is not None:
                background_ratios.append(_percentage(background_calls, total_tasks))

            # Calculate output correlation rate
            if task_output_calls is not None and correlated_outputs is not None:
                if task_output_calls > 0:
                    corr_rate = _percentage(correlated_outputs, task_output_calls)
                    output_correlation_rates.append(corr_rate)

                    # Classify coordination quality
                    if corr_rate > 80.0:
                        high_coordination_sessions += 1
                    elif corr_rate < 50.0:
                        low_coordination_sessions += 1

    # Calculate aggregate metrics
    avg_tasks = _average(task_calls)
    avg_bash = _average(bash_ratios)
    avg_general = _average(general_ratios)
    avg_explore = _average(explore_ratios)
    avg_plan = _average(plan_ratios)
    avg_desc_length = _average(description_lengths)
    avg_parallel = _average(parallel_ratios)
    avg_background = _average(background_ratios)
    avg_correlation = _average(output_correlation_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_task_tool": sessions_with_task,
        "avg_task_calls": avg_tasks,
        "avg_bash_subagent_ratio": avg_bash,
        "avg_general_subagent_ratio": avg_general,
        "avg_explore_subagent_ratio": avg_explore,
        "avg_plan_subagent_ratio": avg_plan,
        "avg_task_description_length": avg_desc_length,
        "avg_parallel_task_ratio": avg_parallel,
        "avg_background_task_ratio": avg_background,
        "avg_output_correlation_rate": avg_correlation,
        "high_coordination_sessions": high_coordination_sessions,
        "low_coordination_sessions": low_coordination_sessions,
    }


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
