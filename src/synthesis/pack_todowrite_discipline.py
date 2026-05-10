"""Pack TodoWrite task management discipline analyzer.

Analyzes execution pack transcripts for TodoWrite task management discipline,
measuring task granularity, completion timing, state transitions, description
quality, and anti-pattern detection across sessions.

TodoWrite discipline dimensions:
1. Task granularity:
   - Task count distribution (too few = too broad, too many = over-fragmented)
   - Task description length (appropriate vs vague/overly verbose)
   - Tasks per session average

2. Completion timing:
   - Immediate completion rate (marked complete within 2 turns of creation)
   - Batch completion detection (multiple tasks completed in single turn)
   - Average task lifespan in turns

3. State transitions:
   - Proper pending→in_progress→completed flow
   - Tasks skipping in_progress state
   - Tasks stuck in in_progress state

4. Description quality:
   - activeForm presence and consistency
   - Imperative form (content) vs continuous form (activeForm) validation
   - Actionable vs vague descriptions

5. Anti-patterns:
   - Creating todos but never updating them
   - Marking multiple tasks complete in one turn
   - Tasks stuck in in_progress across multiple turns
   - Missing activeForm fields

Quality indicators:
- Granularity score (0.6-0.9): Appropriate task breakdown
- Immediate completion rate (>70%): Good discipline
- Batch completion rate (<15%): Minimal batching
- activeForm presence (>95%): Complete metadata
- Tasks stuck rate (<10%): Clean state management
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_todowrite_discipline(records: object) -> dict[str, Any]:
    """Analyze TodoWrite task management discipline across pack transcripts.

    Args:
        records: List of session dictionaries with keys:
            - total_tasks_created: Total todos created in session
            - total_tasks_completed: Total todos marked completed
            - immediate_completions: Tasks completed within 2 turns of creation
            - batch_completions: Tasks completed in batches (multiple at once)
            - tasks_skipping_in_progress: Tasks going pending→completed directly
            - tasks_stuck_in_progress: Tasks remaining in_progress for >5 turns
            - tasks_with_activeform: Tasks with activeForm field present
            - tasks_missing_activeform: Tasks without activeForm field
            - avg_task_description_length: Average length of task content field
            - avg_task_lifespan_turns: Average turns from creation to completion
            - total_todowrite_calls: Total TodoWrite tool invocations
            - tasks_never_updated: Tasks created but never state-transitioned

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - sessions_with_tasks: Sessions using TodoWrite
            - avg_tasks_per_session: Average tasks created per session
            - avg_task_description_length: Average task description length
            - task_granularity_score: Score 0-1 for appropriate task breakdown
            - immediate_completion_rate: % tasks completed within 2 turns
            - batch_completion_rate: % tasks completed in batches
            - tasks_skipping_in_progress_rate: % tasks skipping in_progress
            - tasks_stuck_rate: % tasks stuck in in_progress
            - activeform_presence_rate: % tasks with activeForm
            - avg_task_lifespan_turns: Average turns from creation to completion
            - tasks_never_updated_rate: % tasks created but never updated
            - discipline_score: Overall discipline score 0-1
            - high_discipline_sessions: Count with score >0.8
            - low_discipline_sessions: Count with score <0.5

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    sessions_with_tasks = 0
    total_tasks_created = 0
    total_tasks_completed = 0
    total_immediate_completions = 0
    total_batch_completions = 0
    total_skipping_in_progress = 0
    total_stuck_in_progress = 0
    total_with_activeform = 0
    total_missing_activeform = 0
    total_never_updated = 0

    task_counts: list[int | float] = []
    description_lengths: list[int | float] = []
    task_lifespans: list[int | float] = []
    session_discipline_scores: list[int | float] = []

    high_discipline_sessions = 0  # >0.8 score
    low_discipline_sessions = 0   # <0.5 score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        tasks_created = _int(record.get("total_tasks_created", 0))
        tasks_completed = _int(record.get("total_tasks_completed", 0))
        immediate = _int(record.get("immediate_completions", 0))
        batch = _int(record.get("batch_completions", 0))
        skipping = _int(record.get("tasks_skipping_in_progress", 0))
        stuck = _int(record.get("tasks_stuck_in_progress", 0))
        with_activeform = _int(record.get("tasks_with_activeform", 0))
        missing_activeform = _int(record.get("tasks_missing_activeform", 0))
        never_updated = _int(record.get("tasks_never_updated", 0))
        avg_desc_len = _float(record.get("avg_task_description_length", 0.0))
        avg_lifespan = _float(record.get("avg_task_lifespan_turns", 0.0))

        if tasks_created > 0:
            sessions_with_tasks += 1
            task_counts.append(tasks_created)

        total_tasks_created += tasks_created
        total_tasks_completed += tasks_completed
        total_immediate_completions += immediate
        total_batch_completions += batch
        total_skipping_in_progress += skipping
        total_stuck_in_progress += stuck
        total_with_activeform += with_activeform
        total_missing_activeform += missing_activeform
        total_never_updated += never_updated

        if avg_desc_len > 0:
            description_lengths.append(avg_desc_len)
        if avg_lifespan > 0:
            task_lifespans.append(avg_lifespan)

        # Calculate session-level discipline score
        session_score = _calculate_session_discipline_score(
            tasks_created=tasks_created,
            immediate_completions=immediate,
            batch_completions=batch,
            tasks_with_activeform=with_activeform,
            tasks_missing_activeform=missing_activeform,
            tasks_stuck=stuck,
            tasks_never_updated=never_updated,
            avg_description_length=avg_desc_len,
        )
        session_discipline_scores.append(session_score)

        if session_score > 0.8:
            high_discipline_sessions += 1
        elif session_score < 0.5:
            low_discipline_sessions += 1

    # Calculate pack-level rates
    immediate_completion_rate = _percentage(
        total_immediate_completions, total_tasks_completed
    )
    batch_completion_rate = _percentage(total_batch_completions, total_tasks_completed)
    tasks_skipping_rate = _percentage(total_skipping_in_progress, total_tasks_created)
    tasks_stuck_rate = _percentage(total_stuck_in_progress, total_tasks_created)
    activeform_presence_rate = _percentage(
        total_with_activeform, total_tasks_created
    )
    tasks_never_updated_rate = _percentage(total_never_updated, total_tasks_created)

    # Calculate averages
    avg_tasks = _average(task_counts)
    avg_description_length = _average(description_lengths)
    avg_lifespan = _average(task_lifespans)

    # Calculate task granularity score
    granularity_score = _calculate_granularity_score(
        avg_tasks_per_session=avg_tasks,
        avg_description_length=avg_description_length,
    )

    # Calculate overall discipline score
    discipline_score = _calculate_pack_discipline_score(
        granularity_score=granularity_score,
        immediate_completion_rate=immediate_completion_rate,
        batch_completion_rate=batch_completion_rate,
        activeform_presence_rate=activeform_presence_rate,
        tasks_stuck_rate=tasks_stuck_rate,
        tasks_never_updated_rate=tasks_never_updated_rate,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_with_tasks": sessions_with_tasks,
        "avg_tasks_per_session": avg_tasks,
        "avg_task_description_length": avg_description_length,
        "task_granularity_score": granularity_score,
        "immediate_completion_rate": immediate_completion_rate,
        "batch_completion_rate": batch_completion_rate,
        "tasks_skipping_in_progress_rate": tasks_skipping_rate,
        "tasks_stuck_rate": tasks_stuck_rate,
        "activeform_presence_rate": activeform_presence_rate,
        "avg_task_lifespan_turns": avg_lifespan,
        "tasks_never_updated_rate": tasks_never_updated_rate,
        "discipline_score": discipline_score,
        "high_discipline_sessions": high_discipline_sessions,
        "low_discipline_sessions": low_discipline_sessions,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "sessions_with_tasks": 0,
        "avg_tasks_per_session": 0.0,
        "avg_task_description_length": 0.0,
        "task_granularity_score": 0.0,
        "immediate_completion_rate": 0.0,
        "batch_completion_rate": 0.0,
        "tasks_skipping_in_progress_rate": 0.0,
        "tasks_stuck_rate": 0.0,
        "activeform_presence_rate": 0.0,
        "avg_task_lifespan_turns": 0.0,
        "tasks_never_updated_rate": 0.0,
        "discipline_score": 0.0,
        "high_discipline_sessions": 0,
        "low_discipline_sessions": 0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_granularity_score(
    avg_tasks_per_session: float,
    avg_description_length: float,
) -> float:
    """Calculate task granularity score (0-1).

    Optimal granularity:
    - 3-8 tasks per session (sweet spot)
    - Description length 20-60 characters (concise but clear)

    Args:
        avg_tasks_per_session: Average number of tasks per session
        avg_description_length: Average task description length

    Returns:
        Granularity score from 0.0 to 1.0
    """
    score = 0.0

    # Task count component (0-0.6)
    if 3 <= avg_tasks_per_session <= 8:
        score += 0.6
    elif 2 <= avg_tasks_per_session < 3 or 8 < avg_tasks_per_session <= 10:
        score += 0.45
    elif 1 <= avg_tasks_per_session < 2 or 10 < avg_tasks_per_session <= 15:
        score += 0.30
    else:  # <1 or >15
        score += 0.15

    # Description length component (0-0.4)
    if 20 <= avg_description_length <= 60:
        score += 0.4
    elif 15 <= avg_description_length < 20 or 60 < avg_description_length <= 80:
        score += 0.30
    elif 10 <= avg_description_length < 15 or 80 < avg_description_length <= 100:
        score += 0.20
    else:  # <10 or >100
        score += 0.10

    return round(score, 3)


def _calculate_session_discipline_score(
    tasks_created: int,
    immediate_completions: int,
    batch_completions: int,
    tasks_with_activeform: int,
    tasks_missing_activeform: int,  # noqa: ARG001
    tasks_stuck: int,
    tasks_never_updated: int,
    avg_description_length: float,  # noqa: ARG001
) -> float:
    """Calculate session-level discipline score (0-1).

    Scoring components:
    - Immediate completion rate (0-0.30)
    - Low batch completion rate (0-0.20)
    - activeForm presence (0-0.20)
    - Low stuck task rate (0-0.15)
    - Low never-updated rate (0-0.15)

    Returns:
        Session discipline score from 0.0 to 1.0
    """
    if tasks_created == 0:
        return 0.0

    score = 0.0

    # Immediate completion component (0-0.30)
    immediate_rate = _percentage(immediate_completions, tasks_created)
    if immediate_rate >= 70:
        score += 0.30
    elif immediate_rate >= 50:
        score += 0.20
    elif immediate_rate >= 30:
        score += 0.10

    # Batch completion penalty (0-0.20)
    batch_rate = _percentage(batch_completions, tasks_created)
    if batch_rate <= 10:
        score += 0.20
    elif batch_rate <= 20:
        score += 0.15
    elif batch_rate <= 30:
        score += 0.10

    # activeForm presence (0-0.20)
    activeform_rate = _percentage(tasks_with_activeform, tasks_created)
    if activeform_rate >= 95:
        score += 0.20
    elif activeform_rate >= 85:
        score += 0.15
    elif activeform_rate >= 70:
        score += 0.10

    # Low stuck task rate (0-0.15)
    stuck_rate = _percentage(tasks_stuck, tasks_created)
    if stuck_rate <= 5:
        score += 0.15
    elif stuck_rate <= 10:
        score += 0.10
    elif stuck_rate <= 20:
        score += 0.05

    # Low never-updated rate (0-0.15)
    never_updated_rate = _percentage(tasks_never_updated, tasks_created)
    if never_updated_rate <= 5:
        score += 0.15
    elif never_updated_rate <= 10:
        score += 0.10
    elif never_updated_rate <= 20:
        score += 0.05

    return round(score, 3)


def _calculate_pack_discipline_score(
    granularity_score: float,
    immediate_completion_rate: float,
    batch_completion_rate: float,
    activeform_presence_rate: float,
    tasks_stuck_rate: float,
    tasks_never_updated_rate: float,
) -> float:
    """Calculate overall pack discipline score (0-1).

    Scoring components:
    - Granularity (0-0.20): Appropriate task breakdown
    - Immediate completion (0-0.25): Quick completion discipline
    - Low batch completion (0-0.20): No batching anti-pattern
    - activeForm presence (0-0.15): Complete metadata
    - Low stuck rate (0-0.10): Clean state management
    - Low never-updated rate (0-0.10): No abandoned todos

    Returns:
        Pack discipline score from 0.0 to 1.0
    """
    score = 0.0

    # Granularity component (0-0.20)
    score += granularity_score * 0.20

    # Immediate completion component (0-0.25)
    if immediate_completion_rate >= 70:
        score += 0.25
    elif immediate_completion_rate >= 50:
        score += 0.18
    elif immediate_completion_rate >= 30:
        score += 0.10

    # Batch completion penalty (0-0.20)
    if batch_completion_rate <= 10:
        score += 0.20
    elif batch_completion_rate <= 20:
        score += 0.15
    elif batch_completion_rate <= 30:
        score += 0.10

    # activeForm presence component (0-0.15)
    if activeform_presence_rate >= 95:
        score += 0.15
    elif activeform_presence_rate >= 85:
        score += 0.10
    elif activeform_presence_rate >= 70:
        score += 0.05

    # Low stuck task component (0-0.10)
    if tasks_stuck_rate <= 5:
        score += 0.10
    elif tasks_stuck_rate <= 10:
        score += 0.07
    elif tasks_stuck_rate <= 20:
        score += 0.04

    # Low never-updated component (0-0.10)
    if tasks_never_updated_rate <= 5:
        score += 0.10
    elif tasks_never_updated_rate <= 10:
        score += 0.07
    elif tasks_never_updated_rate <= 20:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
