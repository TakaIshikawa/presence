"""Tests for session permission denial recovery analyzer."""

import pytest

from synthesis.session_permission_denial_recovery import (
    EVENT_PERMISSION_DENIAL,
    EVENT_TOOL_SUCCESS,
    QUALITY_NO_DENIALS,
    QUALITY_PARTIAL,
    QUALITY_RECOVERED,
    QUALITY_UNRECOVERED,
    SessionPermissionEvent,
    analyze_session_permission_denial_recovery,
)


def test_empty_input_returns_no_denials():
    result = analyze_session_permission_denial_recovery([])

    assert result.metrics.permission_denials == 0
    assert result.metrics.average_turns_to_recovery == 0.0
    assert result.quality == QUALITY_NO_DENIALS


def test_denial_followed_by_successful_tool_call_is_recovered():
    result = analyze_session_permission_denial_recovery(
        [
            SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "bash", "rm -rf build"),
            SessionPermissionEvent(2, EVENT_TOOL_SUCCESS, "bash", "rm -rf build"),
        ]
    )

    assert result.metrics.permission_denials == 1
    assert result.metrics.recovered_denials == 1
    assert result.metrics.unrecovered_denials == 0
    assert result.metrics.retried_same_command == 1
    assert result.metrics.average_turns_to_recovery == 2.0
    assert result.quality == QUALITY_RECOVERED


def test_mixed_recovered_and_unrecovered_denials_are_partial():
    result = analyze_session_permission_denial_recovery(
        [
            SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "bash", "pytest"),
            SessionPermissionEvent(1, EVENT_TOOL_SUCCESS, "bash", "pytest"),
            SessionPermissionEvent(3, EVENT_PERMISSION_DENIAL, "bash", "git push"),
        ]
    )

    assert result.metrics.recovered_denials == 1
    assert result.metrics.unrecovered_denials == 1
    assert result.quality == QUALITY_PARTIAL
    assert "no later successful tool call" in result.insights[1]


def test_unrecovered_denial_is_unrecovered_quality():
    result = analyze_session_permission_denial_recovery(
        [SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "bash", "git push")]
    )

    assert result.metrics.unrecovered_denials == 1
    assert result.quality == QUALITY_UNRECOVERED


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "list or tuple"),
        ([{"turn_index": 0}], "SessionPermissionEvent"),
        ([SessionPermissionEvent(-1, EVENT_PERMISSION_DENIAL, "bash")], "turn_index"),
        (
            [
                SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "bash"),
                SessionPermissionEvent(0, EVENT_TOOL_SUCCESS, "bash"),
            ],
            "strictly increasing",
        ),
        ([SessionPermissionEvent(0, "cancelled", "bash")], "unsupported event_type"),
        ([SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "")], "tool_name"),
        ([SessionPermissionEvent(0, EVENT_PERMISSION_DENIAL, "bash", None)], "command"),
    ],
)
def test_invalid_inputs_raise_clear_errors(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_permission_denial_recovery(events)
