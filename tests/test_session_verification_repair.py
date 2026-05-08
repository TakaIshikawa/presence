"""Tests for session verification repair analyzer."""

import pytest

from synthesis.session_verification_repair import (
    EVENT_EDIT,
    EVENT_REPAIR,
    EVENT_VERIFICATION,
    STATUS_ATTEMPTED,
    STATUS_FAILED,
    STATUS_PASSED,
    VerificationRepairEvent,
    analyze_session_verification_repair,
)


def test_empty_input_returns_zero_metrics():
    result = analyze_session_verification_repair([])

    assert result.metrics.total_failures == 0
    assert result.metrics.repaired_failures == 0
    assert result.metrics.repair_rate == 0.0
    assert result.metrics.median_repair_latency == 0.0
    assert result.unresolved_examples == ()


def test_failed_verification_followed_by_edit_and_pass_counts_as_repaired():
    result = analyze_session_verification_repair(
        [
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 1, "pytest"),
            VerificationRepairEvent(EVENT_EDIT, STATUS_ATTEMPTED, 2, summary="fix assertion"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 4, "pytest"),
        ]
    )

    assert result.metrics.total_failures == 1
    assert result.metrics.repaired_failures == 1
    assert result.metrics.unresolved_failures == 0
    assert result.metrics.repair_rate == 1.0
    assert result.metrics.median_repair_latency == 3.0


def test_repair_event_also_counts_as_repair_attempt():
    result = analyze_session_verification_repair(
        [
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, "uv run pytest"),
            VerificationRepairEvent(EVENT_REPAIR, STATUS_ATTEMPTED, 1, summary="adjust test"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 2, "uv run pytest"),
        ]
    )

    assert result.metrics.repaired_failures == 1
    assert result.metrics.median_repair_latency == 2.0


def test_failed_verification_without_later_pass_is_unresolved():
    result = analyze_session_verification_repair(
        [
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, "pytest", "failed"),
            VerificationRepairEvent(EVENT_EDIT, STATUS_ATTEMPTED, 1, summary="try fix"),
        ]
    )

    assert result.metrics.unresolved_failures == 1
    assert result.metrics.repair_rate == 0.0
    assert result.unresolved_examples[0].turn_index == 0
    assert result.unresolved_examples[0].command == "pytest"


def test_pass_without_repair_attempt_does_not_count_as_repaired():
    result = analyze_session_verification_repair(
        [
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, "pytest"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 1, "pytest"),
        ]
    )

    assert result.metrics.repaired_failures == 0
    assert result.metrics.unresolved_failures == 1


def test_median_latency_and_repair_rate_are_reported():
    result = analyze_session_verification_repair(
        [
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, "pytest a"),
            VerificationRepairEvent(EVENT_EDIT, STATUS_ATTEMPTED, 1, summary="fix a"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 2, "pytest a"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 4, "pytest b"),
            VerificationRepairEvent(EVENT_REPAIR, STATUS_ATTEMPTED, 5, summary="fix b"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 10, "pytest b"),
            VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 11, "pytest c"),
        ]
    )

    assert result.metrics.total_failures == 3
    assert result.metrics.repaired_failures == 2
    assert result.metrics.unresolved_failures == 1
    assert result.metrics.repair_rate == 0.667
    assert result.metrics.median_repair_latency == 4.0


def test_unresolved_examples_are_capped_at_five():
    events = [
        VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, index, f"pytest {index}")
        for index in range(7)
    ]

    result = analyze_session_verification_repair(events)

    assert result.metrics.unresolved_failures == 7
    assert len(result.unresolved_examples) == 5


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "events"),
        ([{"event_type": "verification"}], "VerificationRepairEvent"),
        ([VerificationRepairEvent("test", STATUS_FAILED, 0)], "event_type"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, "error", 0)], "status"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, STATUS_ATTEMPTED, 0)], "verification"),
        ([VerificationRepairEvent(EVENT_EDIT, STATUS_PASSED, 0)], "edit"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, -1)], "turn_index"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, True)], "turn_index"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, "")], "command"),
        ([VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 0, 123)], "command"),
        ([VerificationRepairEvent(EVENT_EDIT, STATUS_ATTEMPTED, 0, summary="")], "summary"),
        (
            [
                VerificationRepairEvent(EVENT_VERIFICATION, STATUS_PASSED, 2),
                VerificationRepairEvent(EVENT_VERIFICATION, STATUS_FAILED, 1),
            ],
            "turn_index",
        ),
    ],
)
def test_invalid_verification_repair_records_raise_value_error(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_verification_repair(events)
