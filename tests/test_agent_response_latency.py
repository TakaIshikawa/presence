"""Tests for agent response latency analyzer."""

from datetime import datetime, timedelta, timezone

import pytest

from engagement.agent_response_latency import (
    BUCKET_IMMEDIATE,
    BUCKET_MISSING,
    BUCKET_NORMAL,
    BUCKET_SLOW,
    ROLE_ASSISTANT,
    ROLE_USER,
    SessionEvent,
    analyze_agent_response_latency,
)


BASE = datetime(2026, 5, 8, 10, 0, tzinfo=timezone.utc)


def _event(role: str, seconds: int, turn: int, content: str | None = None) -> SessionEvent:
    return SessionEvent(role, BASE + timedelta(seconds=seconds), turn, content)


def test_empty_input_returns_zero_latency_report():
    result = analyze_agent_response_latency([])

    assert result.metrics.total_user_prompts == 0
    assert result.metrics.responded_prompts == 0
    assert result.metrics.average_latency_seconds == 0.0
    assert result.latency_buckets == {
        BUCKET_IMMEDIATE: 0,
        BUCKET_NORMAL: 0,
        BUCKET_SLOW: 0,
        BUCKET_MISSING: 0,
    }
    assert result.examples == ()


def test_latency_is_measured_to_next_assistant_event():
    result = analyze_agent_response_latency(
        [
            _event(ROLE_USER, 0, 0),
            _event(ROLE_ASSISTANT, 20, 1),
        ]
    )

    assert result.metrics.total_user_prompts == 1
    assert result.metrics.responded_prompts == 1
    assert result.metrics.average_latency_seconds == 20.0
    assert result.latency_buckets[BUCKET_IMMEDIATE] == 1


def test_normal_slow_and_missing_buckets_are_reported():
    result = analyze_agent_response_latency(
        [
            _event(ROLE_USER, 0, 0, "normal"),
            _event(ROLE_ASSISTANT, 120, 1),
            _event(ROLE_USER, 200, 2, "slow"),
            _event(ROLE_ASSISTANT, 620, 3),
            _event(ROLE_USER, 700, 4, "missing"),
        ]
    )

    assert result.latency_buckets[BUCKET_NORMAL] == 1
    assert result.latency_buckets[BUCKET_SLOW] == 1
    assert result.latency_buckets[BUCKET_MISSING] == 1
    assert result.metrics.missing_responses == 1
    assert result.metrics.average_latency_seconds == 270.0
    assert [example.bucket for example in result.examples] == [BUCKET_SLOW, BUCKET_MISSING]


def test_user_prompt_before_next_user_without_assistant_is_missing():
    result = analyze_agent_response_latency(
        [
            _event(ROLE_USER, 0, 0, "first"),
            _event(ROLE_USER, 10, 1, "second"),
            _event(ROLE_ASSISTANT, 20, 2),
        ]
    )

    assert result.metrics.total_user_prompts == 2
    assert result.metrics.responded_prompts == 1
    assert result.latency_buckets[BUCKET_MISSING] == 1
    assert result.latency_buckets[BUCKET_IMMEDIATE] == 1


def test_median_latency_is_calculated_for_responded_prompts():
    result = analyze_agent_response_latency(
        [
            _event(ROLE_USER, 0, 0),
            _event(ROLE_ASSISTANT, 10, 1),
            _event(ROLE_USER, 100, 2),
            _event(ROLE_ASSISTANT, 300, 3),
            _event(ROLE_USER, 400, 4),
            _event(ROLE_ASSISTANT, 1000, 5),
        ]
    )

    assert result.metrics.median_latency_seconds == 200.0
    assert result.latency_buckets[BUCKET_IMMEDIATE] == 1
    assert result.latency_buckets[BUCKET_NORMAL] == 1
    assert result.latency_buckets[BUCKET_SLOW] == 1


def test_slow_and_missing_examples_are_capped_at_five():
    events = []
    for index in range(6):
        turn = index * 2
        events.append(_event(ROLE_USER, index * 1000, turn, f"slow {index}"))
        events.append(_event(ROLE_ASSISTANT, index * 1000 + 400, turn + 1))

    result = analyze_agent_response_latency(events)

    assert result.latency_buckets[BUCKET_SLOW] == 6
    assert len(result.examples) == 5


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "events"),
        ([{"role": ROLE_USER}], "SessionEvent"),
        ([SessionEvent("system", BASE, 0)], "role"),
        ([SessionEvent(ROLE_USER, "2026-05-08", 0)], "timestamp"),
        ([SessionEvent(ROLE_USER, BASE, -1)], "turn_index"),
        ([SessionEvent(ROLE_USER, BASE, True)], "turn_index"),
        ([SessionEvent(ROLE_USER, BASE, 0, "")], "content"),
        ([SessionEvent(ROLE_USER, BASE, 0, 123)], "content"),
        ([SessionEvent(ROLE_USER, BASE, 2), SessionEvent(ROLE_ASSISTANT, BASE, 1)], "turn_index"),
        (
            [
                SessionEvent(ROLE_USER, BASE + timedelta(seconds=2), 0),
                SessionEvent(ROLE_ASSISTANT, BASE + timedelta(seconds=1), 1),
            ],
            "timestamp",
        ),
    ],
)
def test_invalid_session_events_raise_value_error(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_agent_response_latency(events)
