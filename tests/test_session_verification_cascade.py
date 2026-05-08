"""Tests for session verification cascade analyzer."""

import pytest

from synthesis.session_verification_cascade import (
    EVENT_FILE_MODIFICATION,
    EVENT_VERIFICATION,
    STATUS_FAIL,
    STATUS_PASS,
    CascadeDetection,
    VerificationCascadeEvent,
    analyze_session_verification_cascade,
)


def test_empty_events_returns_zero_metrics():
    result = analyze_session_verification_cascade([])

    assert result.metrics.total_verifications == 0
    assert result.metrics.cascading_failures == 0
    assert result.metrics.cascade_count == 0
    assert result.cascades == ()
    assert "No events provided" in result.insights[0]


def test_single_failure_no_cascade():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in test_foo.py:42",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 0
    assert result.metrics.cascading_failures == 0
    assert "No verification cascades detected" in result.insights[0]


def test_two_failures_no_cascade():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 0
    assert result.metrics.cascading_failures == 0


def test_three_consecutive_failures_with_same_signature_is_cascade():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    assert result.metrics.cascading_failures == 3
    assert result.metrics.average_cascade_length == 3.0
    assert len(result.cascades) == 1

    cascade = result.cascades[0]
    assert cascade.cascade_detected is True
    assert cascade.failure_count == 3
    assert cascade.first_turn_index == 1
    assert cascade.last_turn_index == 5
    assert "tests/test_foo.py" in cascade.affected_files


def test_multiple_unrelated_failures_no_cascade():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest tests/test_a.py",
            error_signature="Error in test_a.py",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="pytest tests/test_b.py",
            error_signature="Error in test_b.py",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest tests/test_c.py",
            error_signature="Error in test_c.py",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 0
    assert result.metrics.cascading_failures == 0


def test_cascade_with_ineffective_repairs():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=2,
            modified_files=("src/unrelated.py",),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=4,
            modified_files=("src/other.py",),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="pytest tests/test_foo.py",
            error_signature="AssertionError in tests/test_foo.py:42",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    assert result.metrics.cascading_failures == 3
    cascade = result.cascades[0]
    assert cascade.repair_effectiveness == 0.0  # No relevant files modified
    assert "Low repair effectiveness" in " ".join(result.insights)


def test_cascade_resolved_by_targeted_fix():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/foo.py:10",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=2,
            modified_files=("src/bar.py",),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/foo.py:10",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=4,
            modified_files=("src/foo.py",),  # Relevant fix
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/foo.py:10",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=6,
            modified_files=("src/foo.py",),  # Another relevant fix
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=7,
            status=STATUS_PASS,
            command="pytest",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    cascade = result.cascades[0]
    assert cascade.failure_count == 3
    # 2 out of 3 modification events were relevant (both touched src/foo.py)
    assert cascade.repair_effectiveness >= 0.5
    assert "src/foo.py" in cascade.affected_files


def test_pass_between_failures_breaks_cascade():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=4,
            status=STATUS_PASS,
            command="pytest",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=6,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=7,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    # Should detect 2 cascades: turns 1-3 and turns 5-7
    assert result.metrics.cascade_count == 2
    assert result.metrics.cascading_failures == 6


def test_missing_error_signature_uses_command():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    cascade = result.cascades[0]
    assert cascade.failure_signature == "npm test"


def test_no_file_modifications_zero_effectiveness():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in test.py",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in test.py",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in test.py",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    cascade = result.cascades[0]
    assert cascade.repair_effectiveness == 0.0


def test_no_affected_files_zero_effectiveness():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Generic error",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=2,
            modified_files=("src/foo.py",),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Generic error",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=4,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Generic error",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    cascade = result.cascades[0]
    assert cascade.repair_effectiveness == 0.0  # Can't judge without affected files


def test_multiple_cascades_different_signatures():
    events = [
        # Cascade 1: Error A
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error A",
        ),
        # Cascade 2: Error B
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=4,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="Error B",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="Error B",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=6,
            status=STATUS_FAIL,
            command="npm test",
            error_signature="Error B",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 2
    assert result.metrics.cascading_failures == 6
    assert result.metrics.average_cascade_length == 3.0


def test_long_cascade_highlighted_in_insights():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=i,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Persistent error",
        )
        for i in range(1, 8)
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    cascade = result.cascades[0]
    assert cascade.failure_count == 7
    insights_text = " ".join(result.insights)
    assert "Longest cascade had 7 failures" in insights_text


@pytest.mark.parametrize(
    ("events", "error_message"),
    [
        ("not a list", "events must be a list or tuple"),
        ([{"type": "verification"}], "VerificationCascadeEvent"),
        (
            [
                VerificationCascadeEvent(
                    event_type="invalid",
                    turn_index=0,
                    status=STATUS_FAIL,
                )
            ],
            "invalid event_type",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index="not_int",
                    status=STATUS_FAIL,
                )
            ],
            "turn_index",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index=-1,
                    status=STATUS_FAIL,
                )
            ],
            "non-negative",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index=5,
                    status=STATUS_FAIL,
                    command="pytest",
                ),
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index=3,
                    status=STATUS_FAIL,
                    command="pytest",
                ),
            ],
            "ordered",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index=0,
                    status="invalid_status",
                    command="pytest",
                )
            ],
            "fail or pass status",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_VERIFICATION,
                    turn_index=0,
                    status=STATUS_FAIL,
                    command="",
                )
            ],
            "non-empty command",
        ),
        (
            [
                VerificationCascadeEvent(
                    event_type=EVENT_FILE_MODIFICATION,
                    turn_index=0,
                    modified_files=["not", "a", "tuple"],
                )
            ],
            "tuple modified_files",
        ),
    ],
)
def test_invalid_events_raise_value_error(events, error_message):
    with pytest.raises(ValueError, match=error_message):
        analyze_session_verification_cascade(events)


def test_partial_effectiveness_with_mixed_modifications():
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/core.py:100",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=2,
            modified_files=("src/core.py", "src/utils.py"),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/core.py:100",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_FILE_MODIFICATION,
            turn_index=4,
            modified_files=("src/unrelated.py", "src/other.py"),
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=5,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="Error in src/core.py:100",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    cascade = result.cascades[0]
    # 1 relevant modification event out of 2 total = 0.5
    assert cascade.repair_effectiveness == 0.5


def test_file_path_extraction_various_formats():
    """Test that file paths are extracted from various error message formats."""
    events = [
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=1,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="AssertionError at tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=2,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="AssertionError at tests/test_foo.py:42",
        ),
        VerificationCascadeEvent(
            event_type=EVENT_VERIFICATION,
            turn_index=3,
            status=STATUS_FAIL,
            command="pytest",
            error_signature="AssertionError at tests/test_foo.py:42",
        ),
    ]

    result = analyze_session_verification_cascade(events)

    assert result.metrics.cascade_count == 1
    cascade = result.cascades[0]
    assert "tests/test_foo.py" in cascade.affected_files
