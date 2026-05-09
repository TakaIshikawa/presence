"""Session TodoWrite tracking coverage analyzer.

Analyzes TodoWrite tool usage quality and adherence to tracking best practices.
Tracks todo list adoption, completion rates, state transitions, and violations
of todo management guidelines.

TodoWrite tracking metrics:
- Tasks with todo lists vs tasks without: Todo adoption rate
- Todo completion rate: Completed todos / total todos
- Todo state transition patterns: pending→in_progress→completed flow
- Orphaned in_progress todos: Todos stuck in in_progress state
- Batch completion violations: Multiple todos marked complete at once
- Multiple simultaneous in_progress: More or less than one todo in_progress
- Missing activeForm: Todos without activeForm field

Quality indicators:
- High todo adoption (>70%): Most tasks use todo tracking
- High completion rate (>85%): Most todos get completed
- Clean state transitions: Proper pending→in_progress→completed flow
- No orphaned todos: All in_progress todos get completed
- No batch violations: Todos completed one at a time
- Exactly one in_progress: Only one todo active at any time
- All activeForm present: Every todo has activeForm field
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_todowrite_tracking_coverage(records: object) -> dict[str, Any]:
    """Analyze TodoWrite tool usage quality and tracking hygiene.

    Tracks todo adoption, completion, state transitions, and violations.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_tasks: Total tasks in session
            - tasks_with_todos: Tasks using TodoWrite
            - tasks_without_todos: Tasks not using TodoWrite
            - total_todos_created: Total todos created
            - total_todos_completed: Total todos marked completed
            - total_todos_abandoned: Todos never completed
            - orphaned_in_progress: Todos stuck in in_progress
            - batch_completion_violations: Multiple todos completed at once
            - multiple_in_progress_violations: >1 or 0 todos in_progress
            - missing_activeform_count: Todos missing activeForm field
            - clean_state_transitions: Todos following proper flow
            - improper_state_transitions: Todos skipping states
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_todos: Sessions using TodoWrite
            - avg_todo_adoption_rate: Average % tasks with todos
            - avg_todo_completion_rate: Average % todos completed
            - avg_orphaned_todo_rate: Average % todos stuck in_progress
            - avg_batch_violation_rate: Average % batch completions
            - avg_multi_in_progress_violation_rate: Average % multi in_progress
            - avg_missing_activeform_rate: Average % missing activeForm
            - avg_clean_transition_rate: Average % clean state transitions
            - high_adoption_sessions: Count with >80% todo adoption
            - low_adoption_sessions: Count with <50% todo adoption
            - sessions_with_batch_violations: Count with batch completions
            - sessions_with_orphaned_todos: Count with orphaned todos
            - sessions_with_multi_in_progress: Count with multi in_progress

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_todos = 0

    todo_adoption_rates: list[float] = []
    todo_completion_rates: list[float] = []
    orphaned_todo_rates: list[float] = []
    batch_violation_rates: list[float] = []
    multi_in_progress_rates: list[float] = []
    missing_activeform_rates: list[float] = []
    clean_transition_rates: list[float] = []

    high_adoption_sessions = 0  # >80% todo adoption
    low_adoption_sessions = 0   # <50% todo adoption
    sessions_with_batch_violations = 0
    sessions_with_orphaned_todos = 0
    sessions_with_multi_in_progress = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_tasks = _extract_number(record.get("total_tasks"))
        tasks_with_todos = _extract_number(record.get("tasks_with_todos"))
        tasks_without_todos = _extract_number(record.get("tasks_without_todos"))
        total_created = _extract_number(record.get("total_todos_created"))
        total_completed = _extract_number(record.get("total_todos_completed"))
        total_abandoned = _extract_number(record.get("total_todos_abandoned"))
        orphaned = _extract_number(record.get("orphaned_in_progress"))
        batch_violations = _extract_number(record.get("batch_completion_violations"))
        multi_in_progress = _extract_number(record.get("multiple_in_progress_violations"))
        missing_activeform = _extract_number(record.get("missing_activeform_count"))
        clean_transitions = _extract_number(record.get("clean_state_transitions"))
        improper_transitions = _extract_number(record.get("improper_state_transitions"))

        # Track sessions with todos
        if tasks_with_todos is not None and tasks_with_todos > 0:
            sessions_with_todos += 1

        # Calculate todo adoption rate
        if total_tasks is not None and total_tasks > 0:
            if tasks_with_todos is not None:
                adoption = _percentage(tasks_with_todos, total_tasks)
                todo_adoption_rates.append(adoption)

                if adoption > 80.0:
                    high_adoption_sessions += 1
                elif adoption < 50.0:
                    low_adoption_sessions += 1

        # Calculate todo completion rate
        if total_created is not None and total_created > 0:
            if total_completed is not None:
                todo_completion_rates.append(_percentage(total_completed, total_created))

        # Calculate orphaned todo rate
        if total_created is not None and total_created > 0:
            if orphaned is not None:
                orphaned_todo_rates.append(_percentage(orphaned, total_created))

                if orphaned > 0:
                    sessions_with_orphaned_todos += 1

        # Calculate batch violation rate
        if total_completed is not None and total_completed > 0:
            if batch_violations is not None:
                batch_violation_rates.append(_percentage(batch_violations, total_completed))

                if batch_violations > 0:
                    sessions_with_batch_violations += 1

        # Calculate multi in_progress violation rate
        if total_created is not None and total_created > 0:
            if multi_in_progress is not None:
                multi_in_progress_rates.append(_percentage(multi_in_progress, total_created))

                if multi_in_progress > 0:
                    sessions_with_multi_in_progress += 1

        # Calculate missing activeForm rate
        if total_created is not None and total_created > 0:
            if missing_activeform is not None:
                missing_activeform_rates.append(_percentage(missing_activeform, total_created))

        # Calculate clean transition rate
        if clean_transitions is not None and improper_transitions is not None:
            total_transitions = clean_transitions + improper_transitions
            if total_transitions > 0:
                clean_transition_rates.append(_percentage(clean_transitions, total_transitions))

    # Calculate aggregate metrics
    avg_adoption = _average(todo_adoption_rates)
    avg_completion = _average(todo_completion_rates)
    avg_orphaned = _average(orphaned_todo_rates)
    avg_batch_violation = _average(batch_violation_rates)
    avg_multi_in_progress = _average(multi_in_progress_rates)
    avg_missing_activeform = _average(missing_activeform_rates)
    avg_clean_transition = _average(clean_transition_rates)

    return {
        "total_sessions": total_sessions,
        "sessions_with_todos": sessions_with_todos,
        "avg_todo_adoption_rate": avg_adoption,
        "avg_todo_completion_rate": avg_completion,
        "avg_orphaned_todo_rate": avg_orphaned,
        "avg_batch_violation_rate": avg_batch_violation,
        "avg_multi_in_progress_violation_rate": avg_multi_in_progress,
        "avg_missing_activeform_rate": avg_missing_activeform,
        "avg_clean_transition_rate": avg_clean_transition,
        "high_adoption_sessions": high_adoption_sessions,
        "low_adoption_sessions": low_adoption_sessions,
        "sessions_with_batch_violations": sessions_with_batch_violations,
        "sessions_with_orphaned_todos": sessions_with_orphaned_todos,
        "sessions_with_multi_in_progress": sessions_with_multi_in_progress,
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
