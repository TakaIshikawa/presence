"""Tests for session verification outcome analyzer."""

import pytest

from synthesis.session_verification_outcome import (
    EVENT_IMPLEMENTATION,
    EVENT_VERIFICATION,
    QUALITY_RECOVERED,
    QUALITY_UNRESOLVED,
    STATUS_FAIL,
    STATUS_PASS,
    SessionVerificationEvent,
    analyze_session_verification_outcome,
)


def test_empty_input_returns_stable_zero_state():
    result = analyze_session_verification_outcome([])

    assert result.metrics.implemented_changes == 0
    assert result.metrics.average_turns_to_first_verification == 0.0
    assert "No implementation" in result.insights[0]


def test_passing_verification_counts_verified_change():
    result = analyze_session_verification_outcome(
        [
            SessionVerificationEvent(0, EVENT_IMPLEMENTATION),
            SessionVerificationEvent(1, EVENT_VERIFICATION, "pytest", STATUS_PASS),
        ]
    )

    assert result.metrics.implemented_changes == 1
    assert result.metrics.passing_verifications == 1
    assert result.metrics.unresolved_failures == 0
    assert result.metrics.average_turns_to_first_verification == 1.0


def test_failed_then_passing_verification_is_recovered_not_unresolved():
    result = analyze_session_verification_outcome(
        [
            SessionVerificationEvent(0, EVENT_IMPLEMENTATION),
            SessionVerificationEvent(2, EVENT_VERIFICATION, "pytest", STATUS_FAIL),
            SessionVerificationEvent(3, EVENT_VERIFICATION, "pytest", STATUS_PASS),
        ]
    )

    assert result.metrics.failing_verifications == 1
    assert result.metrics.recovered_failures == 1
    assert result.metrics.unresolved_failures == 0
    assert result.quality == QUALITY_RECOVERED


def test_implementation_without_verification_is_unresolved():
    result = analyze_session_verification_outcome(
        [SessionVerificationEvent(0, EVENT_IMPLEMENTATION)]
    )

    assert result.metrics.implemented_changes == 1
    assert result.metrics.verification_attempts == 0
    assert result.quality == QUALITY_UNRESOLVED


def test_multiple_implementation_clusters_round_latency():
    result = analyze_session_verification_outcome(
        [
            SessionVerificationEvent(0, EVENT_IMPLEMENTATION),
            SessionVerificationEvent(1, EVENT_VERIFICATION, "pytest a", STATUS_PASS),
            SessionVerificationEvent(3, EVENT_IMPLEMENTATION),
            SessionVerificationEvent(6, EVENT_VERIFICATION, "pytest b", STATUS_PASS),
        ]
    )

    assert result.metrics.implemented_changes == 2
    assert result.metrics.average_turns_to_first_verification == 2.0


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "list or tuple"),
        ([{"turn_index": 0}], "SessionVerificationEvent"),
        ([SessionVerificationEvent(-1, EVENT_IMPLEMENTATION)], "turn_index"),
        (
            [SessionVerificationEvent(1, EVENT_IMPLEMENTATION), SessionVerificationEvent(1, EVENT_IMPLEMENTATION)],
            "strictly increasing",
        ),
        ([SessionVerificationEvent(0, "note")], "unsupported event_type"),
        ([SessionVerificationEvent(0, EVENT_VERIFICATION, "pytest", "unknown")], "unsupported"),
    ],
)
def test_invalid_inputs_raise_clear_errors(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_verification_outcome(events)
