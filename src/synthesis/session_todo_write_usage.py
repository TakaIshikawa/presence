"""Session TodoWrite tool usage pattern analyzer.

Analyzes TodoWrite tool usage patterns in Claude Code sessions to measure task
management discipline and completion effectiveness. Tracks how effectively the
agent uses the TodoWrite tool to organize work, track progress, and complete tasks.

TodoWrite usage metrics:
- Total TodoWrite calls: Number of times TodoWrite tool was invoked
- Average todos per call: Mean number of todo items in each TodoWrite call
- Status distribution: Count of todos by status (pending/in_progress/completed)
- Task completion rate: Ratio of completed todos to total todos
- Average task description length: Mean character count of todo content fields
- ActiveForm usage consistency: Percentage of todos with activeForm field present

Quality indicators:
- High completion rate (>80%): Good follow-through on declared tasks
- Consistent activeForm usage (>95%): Proper adherence to TodoWrite schema
- Balanced status distribution: Appropriate mix of pending/in_progress/completed
- Reasonable todos per call (3-10): Effective task breakdown granularity
- Clear descriptions (20-100 chars): Concise but meaningful task descriptions
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_todo_write_usage(records: object) -> dict[str, Any]:
    """Analyze TodoWrite tool usage patterns in Claude Code sessions.

    Evaluates task management discipline through TodoWrite tool usage patterns,
    measuring completion rates, status distribution, and schema adherence.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - todo_write_calls: Number of TodoWrite tool invocations
            - total_todos: Total number of todo items across all calls
            - pending_todos: Number of todos with status "pending"
            - in_progress_todos: Number of todos with status "in_progress"
            - completed_todos: Number of todos with status "completed"
            - total_description_length: Sum of all todo content field lengths
            - todos_with_active_form: Number of todos with activeForm field
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_todo_write: Count of sessions using TodoWrite
            - avg_todo_write_calls: Average TodoWrite invocations per session
            - avg_todos_per_call: Average todos per TodoWrite call
            - total_todos_analyzed: Total todo items across all sessions
            - avg_completion_rate: Average percentage of completed todos
            - avg_in_progress_rate: Average percentage of in-progress todos
            - avg_pending_rate: Average percentage of pending todos
            - avg_task_description_length: Average character count of todo content
            - avg_active_form_consistency: Average percentage with activeForm
            - high_completion_sessions: Count of sessions with >80% completion
            - low_completion_sessions: Count of sessions with <50% completion

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_todo_write = 0

    todo_write_calls: list[int | float] = []
    todos_per_call: list[float] = []
    completion_rates: list[float] = []
    in_progress_rates: list[float] = []
    pending_rates: list[float] = []
    description_lengths: list[float] = []
    active_form_consistency: list[float] = []

    high_completion_sessions = 0  # >80% completion
    low_completion_sessions = 0   # <50% completion

    total_todos_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        todo_calls = _extract_int(record.get("todo_write_calls"))
        total_todos = _extract_int(record.get("total_todos"))
        pending = _extract_int(record.get("pending_todos"))
        in_progress = _extract_int(record.get("in_progress_todos"))
        completed = _extract_int(record.get("completed_todos"))
        total_desc_length = _extract_int(record.get("total_description_length"))
        todos_with_active = _extract_int(record.get("todos_with_active_form"))

        # Track sessions using TodoWrite
        if todo_calls is not None and todo_calls > 0:
            sessions_with_todo_write += 1
            todo_write_calls.append(todo_calls)

            # Calculate todos per call
            if total_todos is not None and total_todos > 0:
                todos_per_call.append(total_todos / todo_calls)
                total_todos_count += total_todos

                # Calculate status distribution rates
                total_status = 0
                if pending is not None:
                    total_status += pending
                if in_progress is not None:
                    total_status += in_progress
                if completed is not None:
                    total_status += completed

                # Only calculate rates if we have status data
                if total_status > 0:
                    if completed is not None:
                        comp_rate = _percentage(completed, total_status)
                        completion_rates.append(comp_rate)

                        # Classify completion quality
                        if comp_rate > 80.0:
                            high_completion_sessions += 1
                        elif comp_rate < 50.0:
                            low_completion_sessions += 1

                    if in_progress is not None:
                        in_progress_rates.append(_percentage(in_progress, total_status))

                    if pending is not None:
                        pending_rates.append(_percentage(pending, total_status))

                # Calculate average description length
                if total_desc_length is not None:
                    avg_desc_len = total_desc_length / total_todos
                    description_lengths.append(avg_desc_len)

                # Calculate activeForm consistency
                if todos_with_active is not None:
                    active_form_consistency.append(
                        _percentage(todos_with_active, total_todos)
                    )

    # Calculate aggregate metrics
    avg_todo_calls = _average(todo_write_calls)
    avg_per_call = _average(todos_per_call)
    avg_completion = _average(completion_rates)
    avg_in_progress = _average(in_progress_rates)
    avg_pending = _average(pending_rates)
    avg_desc_length = _average(description_lengths)
    avg_active_form = _average(active_form_consistency)

    return {
        "total_sessions": total_sessions,
        "sessions_with_todo_write": sessions_with_todo_write,
        "avg_todo_write_calls": avg_todo_calls,
        "avg_todos_per_call": avg_per_call,
        "total_todos_analyzed": total_todos_count,
        "avg_completion_rate": avg_completion,
        "avg_in_progress_rate": avg_in_progress,
        "avg_pending_rate": avg_pending,
        "avg_task_description_length": avg_desc_length,
        "avg_active_form_consistency": avg_active_form,
        "high_completion_sessions": high_completion_sessions,
        "low_completion_sessions": low_completion_sessions,
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
