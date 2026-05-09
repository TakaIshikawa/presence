"""Session EnterPlanMode vs direct implementation analyzer.

Analyzes when agents use EnterPlanMode for planning vs implementing directly,
measuring plan mode adoption, task complexity correlation, and implementation
success rates with and without planning.

Plan mode usage metrics:
- EnterPlanMode invocations: Times plan mode was used
- Direct implementations: Tasks implemented without planning
- Plan mode ratio: Percentage using plan mode
- Task complexity correlation: Plan mode for complex tasks
- Success rate with planning: Task completion after planning
- Success rate without planning: Task completion without planning

Quality indicators:
- Appropriate plan mode usage: Used for complex/ambiguous tasks
- High success with planning: >90% completion after plan mode
- Good direct implementation: >75% completion without planning
- Strong complexity correlation: Plan mode correlates with task complexity
- Low plan mode abandonment: <10% plans abandoned without implementation
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_enterplanmode_usage(records: object) -> dict[str, Any]:
    """Analyze EnterPlanMode usage patterns and implementation strategies."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    enterplanmode_invocations = 0
    direct_implementations = 0
    tasks_with_planning = 0
    tasks_without_planning = 0
    planning_successes = 0
    direct_successes = 0
    complex_tasks_planned = 0
    complex_tasks_direct = 0
    plan_abandonments = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1
        tool_name = _string(record.get("tool_name"))

        if tool_name.lower() == "enterplanmode":
            enterplanmode_invocations += 1

        is_implementation = _bool(record.get("is_implementation", False))
        used_planning = _bool(record.get("used_planning", False))
        task_completed = _bool(record.get("task_completed", False))
        is_complex = _bool(record.get("is_complex_task", False))
        plan_abandoned = _bool(record.get("plan_abandoned", False))

        if is_implementation:
            if used_planning:
                tasks_with_planning += 1
                if task_completed:
                    planning_successes += 1
                if is_complex:
                    complex_tasks_planned += 1
            else:
                tasks_without_planning += 1
                direct_implementations += 1
                if task_completed:
                    direct_successes += 1
                if is_complex:
                    complex_tasks_direct += 1

        if plan_abandoned:
            plan_abandonments += 1

    total_implementations = tasks_with_planning + tasks_without_planning
    plan_mode_ratio = _percentage(tasks_with_planning, total_implementations)

    planning_success_rate = _percentage(planning_successes, tasks_with_planning)
    direct_success_rate = _percentage(direct_successes, tasks_without_planning)

    total_complex = complex_tasks_planned + complex_tasks_direct
    complex_planning_ratio = _percentage(complex_tasks_planned, total_complex)

    abandonment_ratio = _percentage(plan_abandonments, enterplanmode_invocations)

    usage_score = _calculate_usage_score(
        planning_success_rate,
        direct_success_rate,
        complex_planning_ratio,
        abandonment_ratio,
    )

    return {
        "total_turns": total_turns,
        "enterplanmode_invocations": enterplanmode_invocations,
        "direct_implementations": direct_implementations,
        "tasks_with_planning": tasks_with_planning,
        "tasks_without_planning": tasks_without_planning,
        "total_implementations": total_implementations,
        "plan_mode_ratio": plan_mode_ratio,
        "planning_successes": planning_successes,
        "direct_successes": direct_successes,
        "planning_success_rate": planning_success_rate,
        "direct_success_rate": direct_success_rate,
        "complex_tasks_planned": complex_tasks_planned,
        "complex_tasks_direct": complex_tasks_direct,
        "complex_planning_ratio": complex_planning_ratio,
        "plan_abandonments": plan_abandonments,
        "abandonment_ratio": abandonment_ratio,
        "usage_score": usage_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "enterplanmode_invocations": 0,
        "direct_implementations": 0,
        "tasks_with_planning": 0,
        "tasks_without_planning": 0,
        "total_implementations": 0,
        "plan_mode_ratio": 0.0,
        "planning_successes": 0,
        "direct_successes": 0,
        "planning_success_rate": 0.0,
        "direct_success_rate": 0.0,
        "complex_tasks_planned": 0,
        "complex_tasks_direct": 0,
        "complex_planning_ratio": 0.0,
        "plan_abandonments": 0,
        "abandonment_ratio": 0.0,
        "usage_score": 0.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_usage_score(
    planning_success_rate: float,
    direct_success_rate: float,
    complex_planning_ratio: float,
    abandonment_ratio: float,
) -> float:
    """Calculate overall plan mode usage score (0-1)."""
    # Planning success component (0-0.35)
    planning_component = (planning_success_rate / 100.0) * 0.35

    # Direct success component (0-0.25)
    direct_component = (direct_success_rate / 100.0) * 0.25

    # Complex task planning component (0-0.25)
    complex_component = (complex_planning_ratio / 100.0) * 0.25

    # Abandonment penalty (0-0.15)
    if abandonment_ratio <= 10.0:
        abandonment_component = 0.15
    else:
        penalty = min(abandonment_ratio - 10.0, 90.0) / 90.0
        abandonment_component = 0.15 * (1.0 - penalty)

    score = (
        planning_component +
        direct_component +
        complex_component +
        abandonment_component
    )
    return round(max(0.0, min(1.0, score)), 3)
