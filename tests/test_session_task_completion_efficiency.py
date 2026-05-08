"""Tests for session task completion efficiency analyzer."""

import pytest

from synthesis.session_task_completion_efficiency import (
    EVENT_TODO_ABANDONED,
    EVENT_TODO_COMPLETED,
    EVENT_TODO_DECLARED,
    TaskCompletionEvent,
    analyze_session_task_completion_efficiency,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_task_completion_efficiency([])

    assert result.metrics.total_tasks == 0
    assert result.metrics.completed_tasks == 0
    assert result.metrics.completion_rate == 0.0
    assert "No events provided" in result.insights[0]


def test_all_tasks_completed():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Implement feature A",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Write tests",
            token_usage=50,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=300,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=8,
            task_id="task-2",
            token_usage=200,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 2
    assert result.metrics.abandoned_tasks == 0
    assert result.metrics.completion_rate == 1.0
    assert result.metrics.efficiency_score > 0.7


def test_partial_completion():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Task 2",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=300,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_ABANDONED,
            turn_index=6,
            task_id="task-2",
            token_usage=50,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 1
    assert result.metrics.abandoned_tasks == 1
    assert result.metrics.completion_rate == 0.5
    assert result.metrics.abandonment_rate == 0.5


def test_high_abandonment():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Task 2",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=3,
            task_id="task-3",
            task_description="Task 3",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=200,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_ABANDONED,
            turn_index=6,
            task_id="task-2",
            token_usage=50,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_ABANDONED,
            turn_index=7,
            task_id="task-3",
            token_usage=50,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tasks == 3
    assert result.metrics.abandoned_tasks == 2
    assert result.metrics.abandonment_rate > 0.3
    assert "High abandonment rate" in " ".join(result.insights)


def test_implicit_abandonment():
    """Tasks declared but never completed or explicitly abandoned are implicitly abandoned."""
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Task 2",
            token_usage=100,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 0
    assert result.metrics.abandoned_tasks == 2
    assert len(result.abandoned_task_details) == 2


def test_token_usage_tracking():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=1000,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Task 2",
            token_usage=500,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=2000,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tokens_used == 3500
    assert result.metrics.avg_tokens_per_completed_task > 0


def test_turns_to_complete_tracking():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=10,
            task_id="task-1",
            token_usage=500,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    completed = result.completed_task_details[0]
    assert completed["turns_to_complete"] == 9
    assert completed["start_turn"] == 1
    assert completed["end_turn"] == 10


def test_no_tasks_declared():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=1,
            task_id="task-1",
            task_description="Orphan completion",
            token_usage=100,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    # Completion without declaration is ignored
    assert result.metrics.total_tasks == 0
    assert result.metrics.completed_tasks == 0


def test_efficiency_score_calculation():
    # High completion rate, low token usage = high score
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Quick task",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=2,
            task_id="task-1",
            token_usage=200,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.completion_rate == 1.0
    assert result.metrics.efficiency_score > 0.7


def test_low_efficiency_score():
    # Low completion rate = low score
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Task 2",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=3,
            task_id="task-3",
            task_description="Task 3",
            token_usage=100,
        ),
        # No completions
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.efficiency_score == 0.0
    assert "Low efficiency score" in " ".join(result.insights)


def test_completed_task_details():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Implement feature X",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=500,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert len(result.completed_task_details) == 1
    completed = result.completed_task_details[0]
    assert completed["task_id"] == "task-1"
    assert completed["description"] == "Implement feature X"
    assert completed["tokens_used"] > 0


def test_abandoned_task_details():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Abandoned task",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_ABANDONED,
            turn_index=3,
            task_id="task-1",
            token_usage=50,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert len(result.abandoned_task_details) == 1
    abandoned = result.abandoned_task_details[0]
    assert abandoned["task_id"] == "task-1"
    assert abandoned["description"] == "Abandoned task"
    assert abandoned["abandoned_turn"] == 3


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "todo"}], "TaskCompletionEvent"),
        (
            [
                TaskCompletionEvent(
                    event_type="invalid",
                    turn_index=0,
                    task_id="task-1",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index="not_int",
                    task_id="task-1",
                )
            ],
            "must be an integer",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=-1,
                    task_id="task-1",
                )
            ],
            "non-negative",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=5,
                    task_id="task-1",
                ),
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=3,
                    task_id="task-2",
                ),
            ],
            "ordered",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=0,
                    task_id="",
                )
            ],
            "non-empty task_id",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=0,
                    task_id="task-1",
                    token_usage="not_int",
                )
            ],
            "must be an integer",
        ),
        (
            [
                TaskCompletionEvent(
                    event_type=EVENT_TODO_DECLARED,
                    turn_index=0,
                    task_id="task-1",
                    token_usage=-1,
                )
            ],
            "non-negative",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_task_completion_efficiency(events)


def test_insights_generation():
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Task 1",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=400,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    insights_text = " ".join(result.insights)
    assert "Completed 1 of 1" in insights_text
    assert "100%" in insights_text or "100.0%" in insights_text


def test_multiple_tasks_same_description():
    """Tasks can have the same description but different IDs."""
    events = [
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=1,
            task_id="task-1",
            task_description="Fix bug",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_DECLARED,
            turn_index=2,
            task_id="task-2",
            task_description="Fix bug",
            token_usage=100,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=5,
            task_id="task-1",
            token_usage=200,
        ),
        TaskCompletionEvent(
            event_type=EVENT_TODO_COMPLETED,
            turn_index=8,
            task_id="task-2",
            token_usage=200,
        ),
    ]

    result = analyze_session_task_completion_efficiency(events)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 2
