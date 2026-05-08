"""Tests for session stalled-turn recovery analyzer."""

import pytest

from synthesis.session_stalled_turn_recovery import (
    QUALITY_NO_STALLS,
    QUALITY_PARTIAL,
    QUALITY_POOR,
    QUALITY_STRONG,
    STATUS_ABANDONED,
    STATUS_BLOCKED,
    STATUS_PROGRESS,
    STATUS_STALLED,
    SessionTurn,
    analyze_session_stalled_turn_recovery,
)


def test_empty_input_returns_zero_state_result():
    result = analyze_session_stalled_turn_recovery([])

    assert result.metrics.total_turns == 0
    assert result.metrics.recovery_rate == 0.0
    assert result.stall_outcomes == ()
    assert "No turns supplied" in result.insights[0]


def test_blocked_session_without_recovery_counts_unresolved_as_no_recovery():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_PROGRESS),
            SessionTurn(1, STATUS_BLOCKED),
            SessionTurn(2, STATUS_STALLED),
        ]
    )

    assert result.metrics.blocked_turns == 1
    assert result.metrics.stalled_turns == 1
    assert result.metrics.recovered_turns == 0
    assert result.metrics.unresolved_turns == 2
    assert result.recovery_quality == QUALITY_POOR


def test_abandoned_turns_are_counted_separately():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_ABANDONED),
        ]
    )

    assert result.metrics.abandoned_turns == 1
    assert result.metrics.unresolved_turns == 0
    assert result.stall_outcomes[0].classification == STATUS_ABANDONED


def test_immediate_recovery_boundary_includes_two_turn_latency():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_STALLED),
            SessionTurn(2, STATUS_PROGRESS),
        ]
    )

    assert result.metrics.recovered_turns == 2
    assert result.metrics.immediate_recoveries == 2
    assert result.metrics.delayed_recoveries == 0
    assert result.metrics.average_recovery_latency == 1.5
    assert result.recovery_quality == QUALITY_STRONG


def test_progress_only_session_has_no_stalls_quality():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_PROGRESS),
            SessionTurn(1, STATUS_PROGRESS),
        ]
    )

    assert result.metrics.recovery_rate == 1.0
    assert result.stall_outcomes == ()
    assert result.recovery_quality == QUALITY_NO_STALLS


def test_recovery_rate_at_strong_boundary_with_no_delays_is_strong():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_PROGRESS),
            SessionTurn(2, STATUS_BLOCKED),
            SessionTurn(3, STATUS_PROGRESS),
            SessionTurn(4, STATUS_STALLED),
            SessionTurn(5, STATUS_PROGRESS),
            SessionTurn(6, STATUS_STALLED),
            SessionTurn(7, STATUS_PROGRESS),
            SessionTurn(8, STATUS_BLOCKED),
        ]
    )

    assert result.metrics.recovery_rate == 0.8
    assert result.metrics.delayed_recoveries == 0
    assert result.recovery_quality == QUALITY_STRONG


def test_recovery_rate_at_partial_boundary_is_partial():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_PROGRESS),
            SessionTurn(2, STATUS_STALLED),
        ]
    )

    assert result.metrics.recovery_rate == 0.5
    assert result.recovery_quality == QUALITY_PARTIAL


def test_recovery_rate_below_partial_boundary_is_poor():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_PROGRESS),
            SessionTurn(2, STATUS_STALLED),
            SessionTurn(3, STATUS_BLOCKED),
        ]
    )

    assert result.metrics.recovery_rate == 0.333
    assert result.recovery_quality == QUALITY_POOR


def test_delayed_recovery_counts_recovery_but_degrades_quality():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_STALLED),
            SessionTurn(2, STATUS_STALLED),
            SessionTurn(3, STATUS_PROGRESS),
        ]
    )

    assert result.metrics.recovered_turns == 3
    assert result.metrics.immediate_recoveries == 2
    assert result.metrics.delayed_recoveries == 1
    assert result.metrics.recovery_rate == 1.0
    assert result.recovery_quality == QUALITY_PARTIAL
    assert any("Delayed recoveries" in insight for insight in result.insights)


def test_mixed_outcomes_round_recovery_rate_and_latency():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_PROGRESS),
            SessionTurn(2, STATUS_STALLED),
            SessionTurn(3, STATUS_STALLED),
            SessionTurn(4, STATUS_STALLED),
            SessionTurn(5, STATUS_PROGRESS),
            SessionTurn(6, STATUS_BLOCKED),
            SessionTurn(7, STATUS_ABANDONED),
        ]
    )

    assert result.metrics.blocked_turns == 2
    assert result.metrics.stalled_turns == 3
    assert result.metrics.recovered_turns == 4
    assert result.metrics.abandoned_turns == 1
    assert result.metrics.recovery_rate == 0.8
    assert result.metrics.average_recovery_latency == 1.75


@pytest.mark.parametrize(
    ("turns", "message"),
    [
        ("bad", "turns must be a list or tuple"),
        ([{"turn_index": 0, "status": STATUS_BLOCKED}], "SessionTurn"),
        ([SessionTurn(-1, STATUS_BLOCKED)], "turn_index"),
        ([SessionTurn(1, STATUS_BLOCKED), SessionTurn(1, STATUS_PROGRESS)], "strictly"),
        ([SessionTurn(0, "waiting")], "unsupported status"),
    ],
)
def test_invalid_turn_inputs_raise_clear_value_errors(turns, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_stalled_turn_recovery(turns)


def test_insight_text_calls_out_abandoned_and_escalation_paths():
    result = analyze_session_stalled_turn_recovery(
        [
            SessionTurn(0, STATUS_BLOCKED),
            SessionTurn(1, STATUS_ABANDONED),
            SessionTurn(2, STATUS_STALLED),
        ]
    )

    joined = " ".join(result.insights)
    assert "abandoned" in joined
    assert "escalation paths" in joined
