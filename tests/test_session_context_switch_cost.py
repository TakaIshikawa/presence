"""Tests for session context switch cost analyzer."""

import pytest

from synthesis.session_context_switch_cost import (
    EVENT_CONTEXT_LOAD,
    EVENT_CONTEXT_RETAIN,
    EVENT_CONTEXT_SWITCH,
    ContextSwitchEvent,
    analyze_session_context_switch_cost,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_context_switch_cost([])

    assert result.metrics.total_switches == 0
    assert result.metrics.total_loads == 0
    assert result.metrics.total_retains == 0
    assert result.metrics.switch_frequency == 0.0
    assert "No events provided" in result.insights[0]


def test_single_context_switch():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            from_context="file_a.py",
            to_context="file_b.py",
            cost_tokens=150,
            duration_seconds=5,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=1000)

    assert result.metrics.total_switches == 1
    assert result.metrics.avg_switch_cost_tokens == 150.0
    assert result.metrics.avg_switch_duration_seconds == 5.0
    assert result.metrics.cumulative_context_cost_tokens == 150
    assert len(result.switch_details) == 1
    assert result.switch_details[0]["from_context"] == "file_a.py"
    assert result.switch_details[0]["to_context"] == "file_b.py"


def test_multiple_context_switches():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            from_context="file_a.py",
            to_context="file_b.py",
            cost_tokens=100,
            duration_seconds=3,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=3,
            event_id="switch-2",
            from_context="file_b.py",
            to_context="file_c.py",
            cost_tokens=200,
            duration_seconds=7,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=2000)

    assert result.metrics.total_switches == 2
    assert result.metrics.avg_switch_cost_tokens == 150.0  # (100 + 200) / 2
    assert result.metrics.avg_switch_duration_seconds == 5.0  # (3 + 7) / 2
    assert result.metrics.cumulative_context_cost_tokens == 300


def test_context_load_events():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=1,
            event_id="load-1",
            context_name="file_a.py",
            cost_tokens=50,
            is_reload=False,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=2,
            event_id="load-2",
            context_name="file_a.py",
            cost_tokens=40,
            is_reload=True,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    assert result.metrics.total_loads == 2
    assert result.metrics.reload_ratio == 0.5  # 1 reload out of 2 loads
    assert result.metrics.cumulative_context_cost_tokens == 90
    assert len(result.load_details) == 2
    assert result.load_details[0]["is_reload"] is False
    assert result.load_details[1]["is_reload"] is True


def test_context_retain_events():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=1,
            event_id="load-1",
            context_name="file_a.py",
            cost_tokens=50,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=2,
            event_id="retain-1",
            context_name="file_a.py",
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=3,
            event_id="retain-2",
            context_name="file_a.py",
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    assert result.metrics.total_retains == 2
    assert result.metrics.total_loads == 1
    assert result.metrics.retention_ratio == round(2 / 3, 3)  # 2 retains out of 3 total events


def test_switch_frequency_calculation():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=0,
            event_id="switch-1",
            cost_tokens=100,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=2,
            event_id="switch-2",
            cost_tokens=100,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=4,
            event_id="switch-3",
            cost_tokens=100,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    # 3 switches across turns 0-4 (5 turns total)
    assert result.metrics.switch_frequency == 0.6  # 3/5


def test_switch_efficiency_score():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=200,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=1000)

    # Efficiency: (1000 - 200) / 1000 = 0.8
    assert result.metrics.switch_efficiency_score == 0.8


def test_high_switch_efficiency():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=50,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=1000)

    # Efficiency: (1000 - 50) / 1000 = 0.95
    assert result.metrics.switch_efficiency_score == 0.95
    assert "High switch efficiency" in " ".join(result.insights)


def test_low_switch_efficiency():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=300,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=1000)

    # Efficiency: (1000 - 300) / 1000 = 0.7
    assert result.metrics.switch_efficiency_score == 0.7
    assert "Low switch efficiency" in " ".join(result.insights)


def test_high_switch_frequency_warning():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=0,
            event_id="switch-1",
            cost_tokens=100,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-2",
            cost_tokens=100,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    # 2 switches in 2 turns = 1.0 frequency
    assert result.metrics.switch_frequency == 1.0
    assert "High switch frequency" in " ".join(result.insights)


def test_high_average_switch_cost_warning():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=150,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    assert result.metrics.avg_switch_cost_tokens == 150.0
    assert "High average switch cost" in " ".join(result.insights)


def test_high_reload_ratio_warning():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=1,
            event_id="load-1",
            cost_tokens=50,
            is_reload=True,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=2,
            event_id="load-2",
            cost_tokens=50,
            is_reload=True,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=3,
            event_id="load-3",
            cost_tokens=50,
            is_reload=False,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    # 2 reloads out of 3 loads = 66.7%
    assert result.metrics.reload_ratio > 0.3
    assert "High reload ratio" in " ".join(result.insights)


def test_strong_context_retention():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=1,
            event_id="load-1",
            cost_tokens=50,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=2,
            event_id="retain-1",
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=3,
            event_id="retain-2",
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=4,
            event_id="retain-3",
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    # 3 retains out of 4 total context events = 75%
    assert result.metrics.retention_ratio == 0.75
    assert "Strong context retention" in " ".join(result.insights)


def test_cumulative_cost_calculation():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=100,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=2,
            event_id="load-1",
            cost_tokens=50,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=3,
            event_id="switch-2",
            cost_tokens=150,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=5000)

    assert result.metrics.cumulative_context_cost_tokens == 300  # 100 + 50 + 150
    # Overhead: 300/5000 = 6%
    insights_text = " ".join(result.insights)
    assert "300 tokens" in insights_text
    assert "6.0%" in insights_text or "6%" in insights_text


def test_zero_session_tokens_efficiency():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=100,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=0)

    # With zero session tokens, efficiency defaults to 1.0
    assert result.metrics.switch_efficiency_score == 1.0


def test_mixed_events_comprehensive():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=0,
            event_id="load-1",
            context_name="file_a.py",
            cost_tokens=50,
            is_reload=False,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            from_context="file_a.py",
            to_context="file_b.py",
            cost_tokens=100,
            duration_seconds=3,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_RETAIN,
            turn_index=2,
            event_id="retain-1",
            context_name="file_b.py",
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=3,
            event_id="load-2",
            context_name="file_a.py",
            cost_tokens=40,
            is_reload=True,
        ),
    ]

    result = analyze_session_context_switch_cost(events, total_session_tokens=2000)

    assert result.metrics.total_switches == 1
    assert result.metrics.total_loads == 2
    assert result.metrics.total_retains == 1
    assert result.metrics.cumulative_context_cost_tokens == 190  # 50 + 100 + 40
    assert result.metrics.reload_ratio == 0.5  # 1 reload out of 2 loads
    assert result.metrics.retention_ratio == round(1 / 3, 3)  # 1 retain out of 3 context events


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "switch"}], "ContextSwitchEvent"),
        (
            [
                ContextSwitchEvent(
                    event_type="invalid",
                    turn_index=0,
                    event_id="event-1",
                )
            ],
            "invalid event_type",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index="not_int",
                    event_id="event-1",
                )
            ],
            "must be an integer",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=-1,
                    event_id="event-1",
                )
            ],
            "non-negative",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=5,
                    event_id="event-1",
                ),
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=3,
                    event_id="event-2",
                ),
            ],
            "ordered",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=0,
                    event_id="",
                )
            ],
            "non-empty event_id",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=0,
                    event_id="event-1",
                    cost_tokens="not_int",
                )
            ],
            "must be an integer",
        ),
        (
            [
                ContextSwitchEvent(
                    event_type=EVENT_CONTEXT_SWITCH,
                    turn_index=0,
                    event_id="event-1",
                    cost_tokens=-1,
                )
            ],
            "non-negative",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_context_switch_cost(events)


@pytest.mark.parametrize(
    ("total_tokens", "error_message"),
    [
        ("not_int", "must be an integer"),
        (-100, "must be non-negative"),
        (True, "must be an integer"),
    ],
)
def test_invalid_total_tokens_raises_value_error(total_tokens, error_message):
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=0,
            event_id="event-1",
            cost_tokens=100,
        ),
    ]
    with pytest.raises(ValueError, match=error_message):
        analyze_session_context_switch_cost(events, total_session_tokens=total_tokens)


def test_insights_generation():
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=50,
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_LOAD,
            turn_index=2,
            event_id="load-1",
            cost_tokens=30,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    insights_text = " ".join(result.insights)
    assert "Detected 1 context switch" in insights_text
    assert "1 context load" in insights_text


def test_switch_durations_with_zeros():
    """Test that zero durations are not included in average calculation."""
    events = [
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=1,
            event_id="switch-1",
            cost_tokens=100,
            duration_seconds=0,  # Should not be included
        ),
        ContextSwitchEvent(
            event_type=EVENT_CONTEXT_SWITCH,
            turn_index=2,
            event_id="switch-2",
            cost_tokens=100,
            duration_seconds=10,
        ),
    ]

    result = analyze_session_context_switch_cost(events)

    # Only the non-zero duration should be included
    assert result.metrics.avg_switch_duration_seconds == 10.0
