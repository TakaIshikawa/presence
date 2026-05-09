"""Session TodoWrite update frequency and batching analyzer.

Analyzes TodoWrite tool usage patterns in Claude Code sessions to measure
update frequency, completion batching behavior, and todo list staleness.
Tracks how effectively the agent maintains fresh task lists.

TodoWrite frequency metrics:
- Total TodoWrite calls: Number of TodoWrite tool invocations
- Average time between updates: Mean seconds between consecutive TodoWrite calls
- Todo list size over time: Average number of todos per update
- Completion batching ratio: Multiple completions in single call vs immediate updates
- Status transition patterns: Pending→in_progress→completed progression
- Todo staleness: Average time todos spend in pending/in_progress states
- Update discipline score: Frequent small updates scored higher than batched completions

Quality indicators:
- High update frequency (<300s between updates): Good progress visibility
- Low completion batching ratio (<20%): Immediate completion marking
- Low staleness (<600s average): Fresh task state
- High discipline score (>80): Frequent updates, minimal batching
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_todowrite_frequency(records: object) -> dict[str, Any]:
    """Analyze TodoWrite tool update frequency and batching patterns.

    Evaluates update frequency, completion batching, status transitions,
    and todo list staleness to score update discipline.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_todowrite_calls: Number of TodoWrite tool invocations
            - total_update_interval_seconds: Sum of seconds between updates
            - total_todo_count: Sum of todo counts across all updates
            - completion_batching_calls: Updates with multiple completions
            - immediate_completion_calls: Updates with single completion
            - total_pending_duration_seconds: Sum of seconds todos spent pending
            - total_inprogress_duration_seconds: Sum of seconds todos spent in_progress
            - total_todos_tracked: Total unique todos tracked across session
            - avg_todos_per_update: Average number of todos per TodoWrite call
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_todowrite: Count using TodoWrite tool
            - avg_todowrite_calls: Average TodoWrite invocations per session
            - avg_time_between_updates: Average seconds between updates
            - avg_todos_per_update: Average todo count per update
            - avg_completion_batching_ratio: Average % of batched completions
            - avg_pending_staleness: Average seconds todos spent pending
            - avg_inprogress_staleness: Average seconds todos spent in_progress
            - update_discipline_score: Score 0-100 (higher = better discipline)
            - high_discipline_sessions: Count with score >80
            - low_discipline_sessions: Count with score <50

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_todowrite = 0

    todowrite_calls: list[int | float] = []
    update_intervals: list[float] = []
    todos_per_update: list[float] = []
    batching_ratios: list[float] = []
    pending_staleness: list[float] = []
    inprogress_staleness: list[float] = []
    discipline_scores: list[float] = []

    high_discipline_sessions = 0  # >80 score
    low_discipline_sessions = 0   # <50 score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_calls = _extract_number(record.get("total_todowrite_calls"))
        total_interval = _extract_number(record.get("total_update_interval_seconds"))
        total_todos = _extract_number(record.get("total_todo_count"))
        batching_calls = _extract_number(record.get("completion_batching_calls"))
        immediate_calls = _extract_number(record.get("immediate_completion_calls"))
        total_pending = _extract_number(record.get("total_pending_duration_seconds"))
        total_inprogress = _extract_number(record.get("total_inprogress_duration_seconds"))
        todos_tracked = _extract_number(record.get("total_todos_tracked"))
        avg_todos = _extract_number(record.get("avg_todos_per_update"))

        # Track sessions using TodoWrite
        if total_calls is not None and total_calls > 0:
            sessions_with_todowrite += 1
            todowrite_calls.append(total_calls)

            # Calculate average time between updates
            if total_interval is not None and total_calls > 1:
                avg_interval = total_interval / (total_calls - 1)
                update_intervals.append(avg_interval)

            # Calculate average todos per update
            if avg_todos is not None:
                todos_per_update.append(avg_todos)
            elif total_todos is not None and total_calls > 0:
                avg_todos_calc = total_todos / total_calls
                todos_per_update.append(avg_todos_calc)

            # Calculate completion batching ratio
            if batching_calls is not None and immediate_calls is not None:
                total_completion_calls = batching_calls + immediate_calls
                if total_completion_calls > 0:
                    batching_ratio = _percentage(batching_calls, total_completion_calls)
                    batching_ratios.append(batching_ratio)

            # Calculate pending staleness
            avg_pending_val: float | None = None
            if total_pending is not None and todos_tracked is not None and todos_tracked > 0:
                avg_pending_val = total_pending / todos_tracked
                pending_staleness.append(avg_pending_val)

            # Calculate in_progress staleness
            avg_inprogress_val: float | None = None
            if total_inprogress is not None and todos_tracked is not None and todos_tracked > 0:
                avg_inprogress_val = total_inprogress / todos_tracked
                inprogress_staleness.append(avg_inprogress_val)

            # Calculate update discipline score
            discipline_score = _calculate_discipline_score(
                total_calls=total_calls,
                avg_interval=total_interval / (total_calls - 1) if total_interval and total_calls > 1 else None,
                batching_ratio=batching_ratios[-1] if batching_ratios and len(batching_ratios) > len(discipline_scores) else None,
                avg_pending=avg_pending_val,
                avg_inprogress=avg_inprogress_val,
            )
            discipline_scores.append(discipline_score)

            # Classify discipline quality
            if discipline_score > 80.0:
                high_discipline_sessions += 1
            elif discipline_score < 50.0:
                low_discipline_sessions += 1

    # Calculate aggregate metrics
    avg_calls = _average(todowrite_calls)
    avg_interval = _average(update_intervals)
    avg_todos = _average(todos_per_update)
    avg_batching = _average(batching_ratios)
    avg_pending = _average(pending_staleness)
    avg_inprogress = _average(inprogress_staleness)
    avg_discipline = _average(discipline_scores)

    return {
        "total_sessions": total_sessions,
        "sessions_with_todowrite": sessions_with_todowrite,
        "avg_todowrite_calls": avg_calls,
        "avg_time_between_updates": avg_interval,
        "avg_todos_per_update": avg_todos,
        "avg_completion_batching_ratio": avg_batching,
        "avg_pending_staleness": avg_pending,
        "avg_inprogress_staleness": avg_inprogress,
        "update_discipline_score": avg_discipline,
        "high_discipline_sessions": high_discipline_sessions,
        "low_discipline_sessions": low_discipline_sessions,
    }


def _calculate_discipline_score(
    total_calls: int | float | None,
    avg_interval: float | None,
    batching_ratio: float | None,
    avg_pending: float | None,
    avg_inprogress: float | None,
) -> float:
    """Calculate update discipline score (0-100).

    Higher scores indicate better discipline:
    - Frequent updates (low avg_interval)
    - Minimal completion batching (low batching_ratio)
    - Low staleness (low avg_pending and avg_inprogress)

    Scoring breakdown:
    - Update frequency: 40 points (300s threshold)
    - Batching discipline: 30 points (20% threshold)
    - Staleness: 30 points (600s threshold)
    """
    score = 0.0

    # Update frequency component (40 points)
    if avg_interval is not None:
        if avg_interval <= 300:  # <5min = excellent
            score += 40.0
        elif avg_interval <= 600:  # <10min = good
            score += 30.0
        elif avg_interval <= 1200:  # <20min = acceptable
            score += 20.0
        else:  # >20min = poor
            score += 10.0
    elif total_calls is not None and total_calls > 5:
        # Many calls but no interval data = assume good
        score += 30.0

    # Batching discipline component (30 points)
    if batching_ratio is not None:
        if batching_ratio <= 20:  # <20% batching = excellent
            score += 30.0
        elif batching_ratio <= 40:  # <40% = good
            score += 20.0
        elif batching_ratio <= 60:  # <60% = acceptable
            score += 10.0
        # >60% = 0 points

    # Staleness component (30 points)
    if avg_pending is not None or avg_inprogress is not None:
        avg_staleness = 0.0
        staleness_count = 0
        if avg_pending is not None:
            avg_staleness += avg_pending
            staleness_count += 1
        if avg_inprogress is not None:
            avg_staleness += avg_inprogress
            staleness_count += 1

        if staleness_count > 0:
            avg_staleness /= staleness_count

            if avg_staleness <= 600:  # <10min = excellent
                score += 30.0
            elif avg_staleness <= 1200:  # <20min = good
                score += 20.0
            elif avg_staleness <= 1800:  # <30min = acceptable
                score += 10.0
            # >30min = 0 points

    return round(score, 2)


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
