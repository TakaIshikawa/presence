"""Tests for session final answer timing analyzer."""

import pytest

from synthesis.session_final_answer_timing import (
    EVENT_BOTTLENECK,
    EVENT_FINAL_ANSWER,
    EVENT_SESSION_START,
    FinalAnswerTimingEvent,
    analyze_session_final_answer_timing,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_final_answer_timing([])

    assert result.metrics.total_sessions == 0
    assert result.metrics.sessions_with_final_answer == 0
    assert "No events provided" in result.insights[0]


def test_single_session_with_final_answer():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="session-1",
            timestamp=0,
            complexity_score=0.5,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=10,
            session_id="session-1",
            timestamp=100,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.total_sessions == 1
    assert result.metrics.sessions_with_final_answer == 1
    assert result.metrics.avg_time_to_final_answer_turns == 10.0
    assert result.metrics.avg_time_to_final_answer_seconds == 100.0
    assert len(result.timing_details) == 1
    assert result.timing_details[0]["time_to_final_answer_turns"] == 10


def test_multiple_sessions():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="session-1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=5,
            session_id="session-1",
            timestamp=50,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="session-2",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=15,
            session_id="session-2",
            timestamp=150,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.total_sessions == 2
    assert result.metrics.sessions_with_final_answer == 2
    assert result.metrics.avg_time_to_final_answer_turns == 10.0  # (5 + 15) / 2
    assert result.metrics.avg_time_to_final_answer_seconds == 100.0  # (50 + 150) / 2


def test_session_with_bottleneck():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="session-1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_BOTTLENECK,
            turn_index=5,
            session_id="session-1",
            bottleneck_type="context_load",
            bottleneck_duration_turns=3,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=10,
            session_id="session-1",
            timestamp=100,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.sessions_with_bottlenecks == 1
    assert len(result.bottleneck_details) == 1
    assert result.bottleneck_details[0]["bottleneck_type"] == "context_load"
    assert result.timing_details[0]["had_bottlenecks"] is True


def test_median_calculation():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=5,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s2",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=10,
            session_id="s2",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s3",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=20,
            session_id="s3",
            timestamp=0,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    # Median of [5, 10, 20] is 10
    assert result.metrics.median_time_to_final_answer_turns == 10.0


def test_complexity_correlation():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s1",
            timestamp=0,
            complexity_score=0.2,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=5,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s2",
            timestamp=0,
            complexity_score=0.8,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=20,
            session_id="s2",
            timestamp=0,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    # Higher complexity (0.8) correlates with higher latency (20)
    # Lower complexity (0.2) correlates with lower latency (5)
    # Should show positive correlation
    assert result.metrics.complexity_latency_correlation > 0


def test_high_latency_warning():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=150,
            session_id="s1",
            timestamp=0,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.avg_time_to_final_answer_turns > 100
    assert "High average latency" in " ".join(result.insights)


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "event"}], "FinalAnswerTimingEvent"),
        (
            [
                FinalAnswerTimingEvent(
                    event_type="invalid",
                    turn_index=0,
                    session_id="s1",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                FinalAnswerTimingEvent(
                    event_type=EVENT_SESSION_START,
                    turn_index="not_int",
                    session_id="s1",
                )
            ],
            "must be an integer",
        ),
        (
            [
                FinalAnswerTimingEvent(
                    event_type=EVENT_SESSION_START,
                    turn_index=-1,
                    session_id="s1",
                )
            ],
            "non-negative",
        ),
        (
            [
                FinalAnswerTimingEvent(
                    event_type=EVENT_SESSION_START,
                    turn_index=0,
                    session_id="",
                )
            ],
            "non-empty session_id",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_final_answer_timing(events)


def test_session_without_final_answer():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s1",
            timestamp=0,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.total_sessions == 1
    assert result.metrics.sessions_with_final_answer == 0


def test_min_max_turns():
    events = [
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=5,
            session_id="s1",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_SESSION_START,
            turn_index=0,
            session_id="s2",
            timestamp=0,
        ),
        FinalAnswerTimingEvent(
            event_type=EVENT_FINAL_ANSWER,
            turn_index=20,
            session_id="s2",
            timestamp=0,
        ),
    ]

    result = analyze_session_final_answer_timing(events)

    assert result.metrics.min_time_to_final_answer_turns == 5
    assert result.metrics.max_time_to_final_answer_turns == 20
