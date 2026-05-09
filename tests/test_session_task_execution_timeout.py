"""Tests for session task execution timeout analyzer."""

import pytest

from synthesis.session_task_execution_timeout import (
    EVENT_TASK_COMPLETED,
    EVENT_TASK_STARTED,
    EVENT_TASK_TIMEOUT,
    TaskExecutionEvent,
    analyze_session_task_execution_timeout,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_task_execution_timeout([])

    assert result.metrics.total_tasks == 0
    assert result.metrics.completed_tasks == 0
    assert result.metrics.timed_out_tasks == 0
    assert result.metrics.timeout_rate == 0.0
    assert "No events provided" in result.insights[0]


def test_all_tasks_complete_within_threshold():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            task_description="Quick task 1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            task_description="Quick task 2",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=3,
            task_id="task-1",
            timestamp=150,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-2",
            timestamp=250,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 2
    assert result.metrics.timed_out_tasks == 0
    assert result.metrics.timeout_rate == 0.0
    assert len(result.long_running_tasks) == 0
    assert "No timeouts detected" in " ".join(result.insights)


def test_some_tasks_exceed_threshold():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            task_description="Long task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            task_description="Quick task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=3,
            task_id="task-1",
            timestamp=400,  # Exceeds 300s threshold
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-2",
            timestamp=150,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.total_tasks == 2
    assert result.metrics.completed_tasks == 1
    assert result.metrics.timed_out_tasks == 1
    assert result.metrics.timeout_rate == 0.5
    assert len(result.long_running_tasks) == 1
    assert result.long_running_tasks[0]["task_id"] == "task-1"
    assert result.long_running_tasks[0]["duration_seconds"] == 400
    assert result.long_running_tasks[0]["exceeded_threshold_by"] == 100


def test_explicit_timeout_event():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            task_description="Timeout task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_TIMEOUT,
            turn_index=5,
            task_id="task-1",
            timestamp=500,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.total_tasks == 1
    assert result.metrics.completed_tasks == 0
    assert result.metrics.timed_out_tasks == 1
    assert result.metrics.timeout_rate == 1.0
    assert len(result.long_running_tasks) == 1
    assert result.task_durations[0]["timed_out"] is True


def test_high_timeout_rate_generates_warning():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=3,
            task_id="task-3",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-1",
            timestamp=400,  # Timeout
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="task-2",
            timestamp=400,  # Timeout
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=6,
            task_id="task-3",
            timestamp=100,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.timeout_rate > 0.2
    assert "High timeout rate" in " ".join(result.insights)


def test_duration_statistics():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=3,
            task_id="task-3",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-1",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="task-2",
            timestamp=200,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=6,
            task_id="task-3",
            timestamp=300,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=500)

    assert result.metrics.total_tasks == 3
    assert result.metrics.avg_task_duration_seconds == 200.0
    assert result.metrics.median_task_duration_seconds == 200.0
    assert result.metrics.max_task_duration_seconds == 300
    assert result.metrics.min_task_duration_seconds == 100


def test_median_calculation_odd_count():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=3,
            task_id="task-3",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-1",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="task-2",
            timestamp=200,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=6,
            task_id="task-3",
            timestamp=500,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=1000)

    # Median of [100, 200, 500] is 200
    assert result.metrics.median_task_duration_seconds == 200.0


def test_median_calculation_even_count():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=3,
            task_id="task-1",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-2",
            timestamp=300,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=1000)

    # Median of [100, 300] is (100 + 300) / 2 = 200
    assert result.metrics.median_task_duration_seconds == 200.0


def test_variance_calculation():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=3,
            task_id="task-1",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-2",
            timestamp=500,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=1000)

    # Mean: (100 + 500) / 2 = 300
    # Variance: ((100-300)^2 + (500-300)^2) / 2 = (40000 + 40000) / 2 = 40000
    assert result.metrics.duration_variance == 40000.0
    assert "High duration variance" in " ".join(result.insights)


def test_long_running_tasks_sorted_by_duration():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            task_description="Medium task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="task-2",
            task_description="Longest task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=3,
            task_id="task-3",
            task_description="Shorter task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="task-1",
            timestamp=400,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="task-2",
            timestamp=600,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=6,
            task_id="task-3",
            timestamp=350,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    # All three tasks exceed threshold, should be sorted by duration descending
    assert len(result.long_running_tasks) == 3
    assert result.long_running_tasks[0]["task_id"] == "task-2"
    assert result.long_running_tasks[0]["duration_seconds"] == 600
    assert result.long_running_tasks[1]["task_id"] == "task-1"
    assert result.long_running_tasks[1]["duration_seconds"] == 400
    assert result.long_running_tasks[2]["task_id"] == "task-3"
    assert result.long_running_tasks[2]["duration_seconds"] == 350


def test_task_duration_details():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            task_description="Test task",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="task-1",
            timestamp=500,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert len(result.task_durations) == 1
    task_detail = result.task_durations[0]
    assert task_detail["task_id"] == "task-1"
    assert task_detail["description"] == "Test task"
    assert task_detail["duration_seconds"] == 400
    assert task_detail["start_turn"] == 1
    assert task_detail["end_turn"] == 5
    assert task_detail["timed_out"] is True


def test_completion_without_start_ignored():
    """Task completion without start event is ignored."""
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=1,
            task_id="task-1",
            task_description="Orphan completion",
            timestamp=100,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.total_tasks == 0
    assert result.metrics.completed_tasks == 0


def test_custom_timeout_threshold():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=2,
            task_id="task-1",
            timestamp=500,
        ),
    ]

    # With 600s threshold, should not timeout
    result = analyze_session_task_execution_timeout(events, timeout_threshold=600)
    assert result.metrics.timeout_rate == 0.0
    assert result.timeout_threshold_seconds == 600

    # With 400s threshold, should timeout
    result = analyze_session_task_execution_timeout(events, timeout_threshold=400)
    assert result.metrics.timeout_rate == 1.0
    assert result.timeout_threshold_seconds == 400


def test_longest_task_insight():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=2,
            task_id="task-1",
            timestamp=800,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    insights_text = " ".join(result.insights)
    assert "Longest task exceeded threshold by 500s" in insights_text


def test_insights_generation():
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="task-1",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=2,
            task_id="task-1",
            timestamp=150,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    insights_text = " ".join(result.insights)
    assert "Tracked 1 task" in insights_text
    assert "Average task duration" in insights_text


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "task"}], "TaskExecutionEvent"),
        (
            [
                TaskExecutionEvent(
                    event_type="invalid",
                    turn_index=0,
                    task_id="task-1",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index="not_int",
                    task_id="task-1",
                )
            ],
            "must be an integer",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=-1,
                    task_id="task-1",
                )
            ],
            "non-negative",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=5,
                    task_id="task-1",
                ),
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=3,
                    task_id="task-2",
                ),
            ],
            "ordered",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=0,
                    task_id="",
                )
            ],
            "non-empty task_id",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=0,
                    task_id="task-1",
                    timestamp="not_int",
                )
            ],
            "must be an integer",
        ),
        (
            [
                TaskExecutionEvent(
                    event_type=EVENT_TASK_STARTED,
                    turn_index=0,
                    task_id="task-1",
                    timestamp=-1,
                )
            ],
            "non-negative",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_task_execution_timeout(events)


@pytest.mark.parametrize(
    ("threshold", "error_message"),
    [
        ("not_int", "must be an integer"),
        (0, "must be positive"),
        (-100, "must be positive"),
    ],
)
def test_invalid_timeout_threshold_raises_value_error(threshold, error_message):
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=0,
            task_id="task-1",
            timestamp=0,
        ),
    ]
    with pytest.raises(ValueError, match=error_message):
        analyze_session_task_execution_timeout(events, timeout_threshold=threshold)


def test_multiple_tasks_various_durations():
    """Comprehensive test with various task durations."""
    events = [
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=1,
            task_id="quick-1",
            task_description="Quick task",
            timestamp=0,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=2,
            task_id="medium-1",
            task_description="Medium task",
            timestamp=50,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_STARTED,
            turn_index=3,
            task_id="long-1",
            task_description="Long task",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=4,
            task_id="quick-1",
            timestamp=100,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_COMPLETED,
            turn_index=5,
            task_id="medium-1",
            timestamp=300,
        ),
        TaskExecutionEvent(
            event_type=EVENT_TASK_TIMEOUT,
            turn_index=6,
            task_id="long-1",
            timestamp=500,
        ),
    ]

    result = analyze_session_task_execution_timeout(events, timeout_threshold=300)

    assert result.metrics.total_tasks == 3
    assert result.metrics.completed_tasks == 2
    assert result.metrics.timed_out_tasks == 1
    assert len(result.long_running_tasks) == 1
    assert result.long_running_tasks[0]["task_id"] == "long-1"
