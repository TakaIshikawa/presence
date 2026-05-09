"""Session task completion efficiency analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypedDict


EVENT_TODO_DECLARED = "todo_declared"
EVENT_TODO_COMPLETED = "todo_completed"
EVENT_TODO_ABANDONED = "todo_abandoned"


class CompletedTaskDetail(TypedDict):
    """Details of a completed task."""

    task_id: str
    description: str
    start_turn: int
    end_turn: int
    turns_to_complete: int
    tokens_used: int


class AbandonedTaskDetail(TypedDict):
    """Details of an abandoned task."""

    task_id: str
    description: str
    start_turn: int
    abandoned_turn: int


@dataclass(frozen=True)
class TaskCompletionEvent:
    """Event in task completion tracking."""

    event_type: str
    turn_index: int
    task_id: str
    task_description: str = ""
    token_usage: int = 0


@dataclass(frozen=True)
class TaskEfficiencyMetrics:
    """Metrics for task completion efficiency."""

    total_tasks: int
    completed_tasks: int
    abandoned_tasks: int
    completion_rate: float
    abandonment_rate: float
    avg_tokens_per_completed_task: float
    total_tokens_used: int
    efficiency_score: float


@dataclass(frozen=True)
class SessionTaskCompletionEfficiencyAnalysis:
    """Complete analysis of task completion efficiency."""

    metrics: TaskEfficiencyMetrics
    completed_task_details: tuple[CompletedTaskDetail, ...]
    abandoned_task_details: tuple[AbandonedTaskDetail, ...]
    insights: tuple[str, ...]


def analyze_session_task_completion_efficiency(
    events: Sequence[TaskCompletionEvent],
) -> SessionTaskCompletionEfficiencyAnalysis:
    """Measure task completion efficiency by tracking TODOs."""

    _validate_events(events)

    if not events:
        return SessionTaskCompletionEfficiencyAnalysis(
            metrics=TaskEfficiencyMetrics(0, 0, 0, 0.0, 0.0, 0.0, 0, 0.0),
            completed_task_details=(),
            abandoned_task_details=(),
            insights=("No events provided.",),
        )

    # Track task states
    declared_tasks: dict[str, tuple[int, str, int]] = {}  # task_id -> (turn, desc, start_tokens)
    completed_tasks: list[CompletedTaskDetail] = []
    abandoned_tasks: list[AbandonedTaskDetail] = []

    cumulative_tokens = 0

    for event in events:
        if event.event_type == EVENT_TODO_DECLARED:
            if event.task_id not in declared_tasks:
                declared_tasks[event.task_id] = (
                    event.turn_index,
                    event.task_description,
                    cumulative_tokens,
                )

        elif event.event_type == EVENT_TODO_COMPLETED:
            if event.task_id in declared_tasks:
                start_turn, desc, start_tokens = declared_tasks[event.task_id]
                tokens_used = cumulative_tokens - start_tokens + event.token_usage
                completed_tasks.append({
                    "task_id": event.task_id,
                    "description": desc or event.task_description,
                    "start_turn": start_turn,
                    "end_turn": event.turn_index,
                    "turns_to_complete": event.turn_index - start_turn,
                    "tokens_used": tokens_used,
                })
                del declared_tasks[event.task_id]

        elif event.event_type == EVENT_TODO_ABANDONED:
            if event.task_id in declared_tasks:
                start_turn, desc, _ = declared_tasks[event.task_id]
                abandoned_tasks.append({
                    "task_id": event.task_id,
                    "description": desc or event.task_description,
                    "start_turn": start_turn,
                    "abandoned_turn": event.turn_index,
                })
                del declared_tasks[event.task_id]

        cumulative_tokens += event.token_usage

    # Tasks still in declared_tasks are implicitly abandoned
    for task_id, (start_turn, desc, _) in declared_tasks.items():
        abandoned_tasks.append({
            "task_id": task_id,
            "description": desc,
            "start_turn": start_turn,
            "abandoned_turn": -1,  # Never explicitly abandoned
        })

    # Calculate metrics
    total_tasks = len(completed_tasks) + len(abandoned_tasks)
    completion_rate = len(completed_tasks) / total_tasks if total_tasks > 0 else 0.0
    abandonment_rate = len(abandoned_tasks) / total_tasks if total_tasks > 0 else 0.0

    total_completion_tokens = sum(task["tokens_used"] for task in completed_tasks)
    avg_tokens = (
        total_completion_tokens / len(completed_tasks) if completed_tasks else 0.0
    )

    # Efficiency score: weighted combination of completion rate and token efficiency
    # Higher completion rate and lower token usage = higher score
    if completion_rate > 0:
        # Normalize token usage (lower is better)
        # Assume 5000 tokens per task is "reasonable", score drops for higher usage
        token_efficiency = max(0, 1 - (avg_tokens / 5000))
        efficiency_score = (completion_rate * 0.7) + (token_efficiency * 0.3)
    else:
        efficiency_score = 0.0

    metrics = TaskEfficiencyMetrics(
        total_tasks=total_tasks,
        completed_tasks=len(completed_tasks),
        abandoned_tasks=len(abandoned_tasks),
        completion_rate=round(completion_rate, 3),
        abandonment_rate=round(abandonment_rate, 3),
        avg_tokens_per_completed_task=round(avg_tokens, 0),
        total_tokens_used=cumulative_tokens,
        efficiency_score=round(efficiency_score, 3),
    )

    return SessionTaskCompletionEfficiencyAnalysis(
        metrics=metrics,
        completed_task_details=tuple(completed_tasks),
        abandoned_task_details=tuple(abandoned_tasks),
        insights=_generate_insights(metrics),
    )


def _validate_events(events: Sequence[TaskCompletionEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, TaskCompletionEvent):
            raise ValueError("events must contain TaskCompletionEvent instances")

        if event.event_type not in {EVENT_TODO_DECLARED, EVENT_TODO_COMPLETED, EVENT_TODO_ABANDONED}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")

        last_turn = event.turn_index

        if not isinstance(event.task_id, str) or not event.task_id.strip():
            raise ValueError(
                f"event at index {index} must have a non-empty task_id"
            )

        if not isinstance(event.token_usage, int) or isinstance(event.token_usage, bool):
            raise ValueError(f"token_usage at index {index} must be an integer")

        if event.token_usage < 0:
            raise ValueError(f"token_usage at index {index} must be non-negative")


def _generate_insights(metrics: TaskEfficiencyMetrics) -> tuple[str, ...]:
    """Generate human-readable insights about task completion efficiency."""
    if metrics.total_tasks == 0:
        return ("No tasks tracked in session.",)

    insights = [
        f"Completed {metrics.completed_tasks} of {metrics.total_tasks} task(s) "
        f"({metrics.completion_rate:.1%} completion rate)."
    ]

    if metrics.abandonment_rate > 0.3:
        insights.append(
            f"High abandonment rate ({metrics.abandonment_rate:.1%}). "
            "Consider breaking down tasks or clarifying requirements."
        )

    if metrics.avg_tokens_per_completed_task > 0:
        insights.append(
            f"Average token usage per completed task: {metrics.avg_tokens_per_completed_task:,.0f}."
        )

    if metrics.efficiency_score >= 0.7:
        insights.append(
            f"High efficiency score ({metrics.efficiency_score:.2f}). "
            "Good completion rate with reasonable token usage."
        )
    elif metrics.efficiency_score < 0.3:
        insights.append(
            f"Low efficiency score ({metrics.efficiency_score:.2f}). "
            "Consider improving task completion or reducing token overhead."
        )

    return tuple(insights)
