"""Session EnterPlanMode frequency analyzer.

Analyzes when and how often agents use EnterPlanMode tool during task execution.
Tracks plan mode invocations, appropriateness per guidelines, planning duration,
and correlation with task success.

EnterPlanMode frequency metrics:
- Total EnterPlanMode invocations: Number of planning mode entries
- Tasks with plan mode vs tasks without: Planning adoption rate
- Average planning duration: Time spent in planning mode
- Appropriate plan mode usage: Multi-file changes, architectural decisions
- Inappropriate skips: Complex tasks that should have used planning
- Premature implementation: Code written before ExitPlanMode
- Correlation with task success: How planning affects completion rate

Quality indicators:
- High plan mode adoption (>60%): Complex tasks use planning
- Low inappropriate skips (<15%): Planning used when needed
- No premature implementation: Code only after ExitPlanMode
- Moderate planning duration (3-10 turns): Efficient planning process
- High plan mode success correlation (>0.7): Planning improves outcomes
- Low planning-to-execution ratio (<0.3): More execution than planning
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_enterplanmode_frequency(records: object) -> dict[str, Any]:
    """Analyze EnterPlanMode tool usage frequency and appropriateness.

    Tracks when agents enter plan mode, whether it's appropriate, and impact on success.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_enterplanmode_calls: Total EnterPlanMode invocations
            - total_tasks: Total tasks in session
            - tasks_with_planning: Tasks that entered plan mode
            - tasks_without_planning: Tasks that skipped plan mode
            - appropriate_planning: Tasks appropriately using plan mode
            - inappropriate_skips: Complex tasks that should have planned
            - premature_implementation_count: Code edits before ExitPlanMode
            - avg_planning_duration_turns: Average turns spent in plan mode
            - total_planning_turns: Total turns in planning mode
            - total_execution_turns: Total turns in execution mode
            - tasks_completed: Successfully completed tasks
            - tasks_with_planning_completed: Completed tasks that used planning
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_planning: Sessions that used EnterPlanMode
            - avg_enterplanmode_calls: Average EnterPlanMode invocations
            - avg_plan_mode_adoption: Average % tasks using plan mode
            - avg_appropriate_planning_rate: Average % appropriate planning
            - avg_inappropriate_skip_rate: Average % inappropriate skips
            - avg_premature_implementation_rate: Average % premature code edits
            - avg_planning_duration_turns: Average planning duration
            - avg_planning_to_execution_ratio: Average planning vs execution time
            - avg_plan_mode_success_rate: Average success rate with planning
            - avg_no_plan_mode_success_rate: Average success rate without planning
            - high_adoption_sessions: Count with >70% plan mode adoption
            - low_adoption_sessions: Count with <30% plan mode adoption
            - sessions_with_premature_implementation: Count with premature code
            - sessions_with_high_inappropriate_skips: Count with >20% skips

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_planning = 0

    enterplanmode_calls: list[int | float] = []
    plan_mode_adoptions: list[float] = []
    appropriate_planning_rates: list[float] = []
    inappropriate_skip_rates: list[float] = []
    premature_implementation_rates: list[float] = []
    planning_durations: list[float] = []
    planning_to_execution_ratios: list[float] = []
    plan_mode_success_rates: list[float] = []
    no_plan_mode_success_rates: list[float] = []

    high_adoption_sessions = 0  # >70% plan mode adoption
    low_adoption_sessions = 0   # <30% plan mode adoption
    sessions_with_premature_implementation = 0
    sessions_with_high_inappropriate_skips = 0  # >20%

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_enterplanmode = _extract_number(record.get("total_enterplanmode_calls"))
        total_tasks = _extract_number(record.get("total_tasks"))
        tasks_with_planning = _extract_number(record.get("tasks_with_planning"))
        tasks_without_planning = _extract_number(record.get("tasks_without_planning"))
        appropriate = _extract_number(record.get("appropriate_planning"))
        inappropriate_skips = _extract_number(record.get("inappropriate_skips"))
        premature_impl = _extract_number(record.get("premature_implementation_count"))
        avg_duration = _extract_number(record.get("avg_planning_duration_turns"))
        total_planning_turns = _extract_number(record.get("total_planning_turns"))
        total_execution_turns = _extract_number(record.get("total_execution_turns"))
        tasks_completed = _extract_number(record.get("tasks_completed"))
        tasks_with_planning_completed = _extract_number(record.get("tasks_with_planning_completed"))

        # Track sessions with planning
        if total_enterplanmode is not None and total_enterplanmode > 0:
            sessions_with_planning += 1
            enterplanmode_calls.append(total_enterplanmode)

        # Calculate plan mode adoption rate
        if total_tasks is not None and total_tasks > 0:
            if tasks_with_planning is not None:
                adoption = _percentage(tasks_with_planning, total_tasks)
                plan_mode_adoptions.append(adoption)

                if adoption > 70.0:
                    high_adoption_sessions += 1
                elif adoption < 30.0:
                    low_adoption_sessions += 1

        # Calculate appropriate planning rate
        if tasks_with_planning is not None and tasks_with_planning > 0:
            if appropriate is not None:
                appropriate_planning_rates.append(_percentage(appropriate, tasks_with_planning))

        # Calculate inappropriate skip rate
        if tasks_without_planning is not None and tasks_without_planning > 0:
            if inappropriate_skips is not None:
                skip_rate = _percentage(inappropriate_skips, tasks_without_planning)
                inappropriate_skip_rates.append(skip_rate)

                if skip_rate > 20.0:
                    sessions_with_high_inappropriate_skips += 1

        # Calculate premature implementation rate
        if total_enterplanmode is not None and total_enterplanmode > 0:
            if premature_impl is not None:
                premature_implementation_rates.append(_percentage(premature_impl, total_enterplanmode))

                if premature_impl > 0:
                    sessions_with_premature_implementation += 1

        # Track planning duration
        if avg_duration is not None:
            planning_durations.append(avg_duration)

        # Calculate planning to execution ratio
        if total_planning_turns is not None and total_execution_turns is not None:
            total_turns = total_planning_turns + total_execution_turns
            if total_turns > 0:
                planning_to_execution_ratios.append(_percentage(total_planning_turns, total_turns))

        # Calculate success rates with and without planning
        if tasks_with_planning is not None and tasks_with_planning > 0:
            if tasks_with_planning_completed is not None:
                plan_mode_success_rates.append(_percentage(tasks_with_planning_completed, tasks_with_planning))

        if tasks_without_planning is not None and tasks_without_planning > 0:
            if tasks_completed is not None and tasks_with_planning_completed is not None:
                no_plan_completed = tasks_completed - tasks_with_planning_completed
                no_plan_mode_success_rates.append(_percentage(no_plan_completed, tasks_without_planning))

    # Calculate aggregate metrics
    avg_enterplanmode = _average(enterplanmode_calls)
    avg_adoption = _average(plan_mode_adoptions)
    avg_appropriate = _average(appropriate_planning_rates)
    avg_inappropriate_skip = _average(inappropriate_skip_rates)
    avg_premature = _average(premature_implementation_rates)
    avg_duration = _average(planning_durations)
    avg_plan_exec_ratio = _average(planning_to_execution_ratios)
    avg_plan_success = _average(plan_mode_success_rates)
    avg_no_plan_success = _average(no_plan_mode_success_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_planning": sessions_with_planning,
        "avg_enterplanmode_calls": avg_enterplanmode,
        "avg_plan_mode_adoption": avg_adoption,
        "avg_appropriate_planning_rate": avg_appropriate,
        "avg_inappropriate_skip_rate": avg_inappropriate_skip,
        "avg_premature_implementation_rate": avg_premature,
        "avg_planning_duration_turns": avg_duration,
        "avg_planning_to_execution_ratio": avg_plan_exec_ratio,
        "avg_plan_mode_success_rate": avg_plan_success,
        "avg_no_plan_mode_success_rate": avg_no_plan_success,
        "high_adoption_sessions": high_adoption_sessions,
        "low_adoption_sessions": low_adoption_sessions,
        "sessions_with_premature_implementation": sessions_with_premature_implementation,
        "sessions_with_high_inappropriate_skips": sessions_with_high_inappropriate_skips,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
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
