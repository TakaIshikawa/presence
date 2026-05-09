"""Session notification tool usage analyzer.

Analyzes how frequently agents use AskUserQuestion tool to notify or ask users during
task execution. Tracks question timing, response latency, blocking behavior, and impact
on session duration and task completion efficiency.

Notification tool metrics:
- Total AskUserQuestion calls: Number of user notification/question events
- Questions per task: Average questions asked per task
- Plan mode vs execution mode questions: Question timing relative to planning phase
- Question timing distribution: Early/mid/late distribution within tasks
- Question blocking patterns: Whether questions blocked progress or enabled parallel work
- Response time impact: How question response latency affects session duration
- Question timeout detection: Questions that exceeded response time threshold

Quality indicators:
- Moderate questions per task (1-3): Balanced consultation without over-asking
- High plan mode question ratio (>60%): Questions asked during planning, not execution
- Low blocking question ratio (<30%): Questions enable parallel work
- Low average response time (<5 min): Users respond quickly
- Few question timeouts (<5%): Questions answered promptly
- High parallel work ratio (>50%): Agent continues work while waiting for answers
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_notification_tool_usage(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion tool usage timing and blocking behavior.

    Tracks question frequency, timing, response latency, and impact on session flow.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_ask_user_questions: Total AskUserQuestion calls
            - total_tasks: Total tasks in session
            - plan_mode_questions: Questions asked during plan mode
            - execution_mode_questions: Questions asked during execution
            - early_task_questions: Questions in first 25% of task
            - mid_task_questions: Questions in middle 50% of task
            - late_task_questions: Questions in last 25% of task
            - blocking_questions: Questions that blocked agent progress
            - parallel_work_questions: Questions asked while agent continued work
            - avg_response_time_seconds: Average time to user response
            - max_response_time_seconds: Maximum response time
            - timed_out_questions: Questions exceeding response time threshold
            - session_duration_seconds: Total session duration
            - question_wait_time_seconds: Total time spent waiting for answers
            - session_title: Optional session title

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - sessions_with_questions: Sessions that used AskUserQuestion
            - avg_questions_per_session: Average questions per session
            - avg_questions_per_task: Average questions per task
            - avg_plan_mode_ratio: Average % questions in plan mode
            - avg_execution_mode_ratio: Average % questions in execution mode
            - avg_early_task_ratio: Average % questions early in task
            - avg_mid_task_ratio: Average % questions mid-task
            - avg_late_task_ratio: Average % questions late in task
            - avg_blocking_ratio: Average % questions that blocked progress
            - avg_parallel_work_ratio: Average % questions with parallel work
            - avg_response_time_seconds: Average response time across sessions
            - avg_max_response_time_seconds: Average max response time
            - avg_timeout_ratio: Average % questions that timed out
            - avg_wait_time_impact: Average % of session spent waiting
            - high_blocking_sessions: Count with >50% blocking questions
            - low_blocking_sessions: Count with <20% blocking questions
            - high_wait_impact_sessions: Count with >30% wait time impact

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    sessions_with_questions = 0

    questions_per_session: list[int | float] = []
    questions_per_task: list[float] = []
    plan_mode_ratios: list[float] = []
    execution_mode_ratios: list[float] = []
    early_task_ratios: list[float] = []
    mid_task_ratios: list[float] = []
    late_task_ratios: list[float] = []
    blocking_ratios: list[float] = []
    parallel_work_ratios: list[float] = []
    response_times: list[float] = []
    max_response_times: list[float] = []
    timeout_ratios: list[float] = []
    wait_time_impacts: list[float] = []

    high_blocking_sessions = 0  # >50% blocking questions
    low_blocking_sessions = 0   # <20% blocking questions
    high_wait_impact_sessions = 0  # >30% wait time impact

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        total_questions = _extract_number(record.get("total_ask_user_questions"))
        total_tasks = _extract_number(record.get("total_tasks"))
        plan_mode = _extract_number(record.get("plan_mode_questions"))
        execution_mode = _extract_number(record.get("execution_mode_questions"))
        early_task = _extract_number(record.get("early_task_questions"))
        mid_task = _extract_number(record.get("mid_task_questions"))
        late_task = _extract_number(record.get("late_task_questions"))
        blocking = _extract_number(record.get("blocking_questions"))
        parallel_work = _extract_number(record.get("parallel_work_questions"))
        avg_response_time = _extract_number(record.get("avg_response_time_seconds"))
        max_response_time = _extract_number(record.get("max_response_time_seconds"))
        timed_out = _extract_number(record.get("timed_out_questions"))
        session_duration = _extract_number(record.get("session_duration_seconds"))
        question_wait_time = _extract_number(record.get("question_wait_time_seconds"))

        # Track sessions with questions
        if total_questions is not None and total_questions > 0:
            sessions_with_questions += 1
            questions_per_session.append(total_questions)

            # Calculate questions per task
            if total_tasks is not None and total_tasks > 0:
                questions_per_task.append(total_questions / total_tasks)

            # Calculate mode ratios
            if plan_mode is not None:
                plan_mode_ratios.append(_percentage(plan_mode, total_questions))
            if execution_mode is not None:
                execution_mode_ratios.append(_percentage(execution_mode, total_questions))

            # Calculate timing distribution
            if early_task is not None:
                early_task_ratios.append(_percentage(early_task, total_questions))
            if mid_task is not None:
                mid_task_ratios.append(_percentage(mid_task, total_questions))
            if late_task is not None:
                late_task_ratios.append(_percentage(late_task, total_questions))

            # Calculate blocking patterns
            if blocking is not None:
                blocking_ratio = _percentage(blocking, total_questions)
                blocking_ratios.append(blocking_ratio)

                if blocking_ratio > 50.0:
                    high_blocking_sessions += 1
                elif blocking_ratio < 20.0:
                    low_blocking_sessions += 1

            if parallel_work is not None:
                parallel_work_ratios.append(_percentage(parallel_work, total_questions))

            # Track response times
            if avg_response_time is not None:
                response_times.append(avg_response_time)
            if max_response_time is not None:
                max_response_times.append(max_response_time)

            # Calculate timeout ratio
            if timed_out is not None:
                timeout_ratios.append(_percentage(timed_out, total_questions))

            # Calculate wait time impact
            if question_wait_time is not None and session_duration is not None and session_duration > 0:
                wait_impact = _percentage(question_wait_time, session_duration)
                wait_time_impacts.append(wait_impact)

                if wait_impact > 30.0:
                    high_wait_impact_sessions += 1

    # Calculate aggregate metrics
    avg_questions = _average(questions_per_session)
    avg_per_task = _average(questions_per_task)
    avg_plan_mode = _average(plan_mode_ratios)
    avg_execution_mode = _average(execution_mode_ratios)
    avg_early = _average(early_task_ratios)
    avg_mid = _average(mid_task_ratios)
    avg_late = _average(late_task_ratios)
    avg_blocking = _average(blocking_ratios)
    avg_parallel = _average(parallel_work_ratios)
    avg_response = _average(response_times)
    avg_max_response = _average(max_response_times)
    avg_timeout = _average(timeout_ratios)
    avg_wait_impact = _average(wait_time_impacts)

    return {
        "total_sessions": total_sessions,
        "sessions_with_questions": sessions_with_questions,
        "avg_questions_per_session": avg_questions,
        "avg_questions_per_task": avg_per_task,
        "avg_plan_mode_ratio": avg_plan_mode,
        "avg_execution_mode_ratio": avg_execution_mode,
        "avg_early_task_ratio": avg_early,
        "avg_mid_task_ratio": avg_mid,
        "avg_late_task_ratio": avg_late,
        "avg_blocking_ratio": avg_blocking,
        "avg_parallel_work_ratio": avg_parallel,
        "avg_response_time_seconds": avg_response,
        "avg_max_response_time_seconds": avg_max_response,
        "avg_timeout_ratio": avg_timeout,
        "avg_wait_time_impact": avg_wait_impact,
        "high_blocking_sessions": high_blocking_sessions,
        "low_blocking_sessions": low_blocking_sessions,
        "high_wait_impact_sessions": high_wait_impact_sessions,
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
