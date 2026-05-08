"""Tests for tool error recovery analyzer."""

import pytest

from synthesis.tool_error_recovery import (
    STATUS_ERROR,
    STATUS_SUCCESS,
    ToolEvent,
    analyze_tool_error_recovery,
)


def test_empty_input_has_no_errors():
    result = analyze_tool_error_recovery([])

    assert result.metrics.tool_errors == 0
    assert result.metrics.recovery_rate == 1.0
    assert result.recovery_quality == "clean"


def test_no_errors_is_clean():
    result = analyze_tool_error_recovery([ToolEvent("rg", STATUS_SUCCESS, 0)])

    assert result.metrics.tool_errors == 0
    assert result.recovery_quality == "clean"


def test_immediate_retry_success_has_one_turn_latency():
    result = analyze_tool_error_recovery(
        [ToolEvent("pytest", STATUS_ERROR, 1), ToolEvent("pytest", STATUS_SUCCESS, 2)]
    )

    assert result.metrics.successful_retries == 1
    assert result.metrics.average_recovery_latency == 1.0
    assert result.recovery_quality == "strong"


def test_delayed_recovery_calculates_latency():
    result = analyze_tool_error_recovery(
        [
            ToolEvent("pytest", STATUS_ERROR, 1),
            ToolEvent("rg", STATUS_SUCCESS, 2),
            ToolEvent("pytest", STATUS_SUCCESS, 4),
        ]
    )

    assert result.metrics.recovery_rate == 1.0
    assert result.metrics.average_recovery_latency == 3.0


def test_repeated_failures_surface_failing_tool():
    result = analyze_tool_error_recovery(
        [
            ToolEvent("pytest", STATUS_ERROR, 0),
            ToolEvent("pytest", STATUS_ERROR, 1),
            ToolEvent("pytest", STATUS_SUCCESS, 2),
        ]
    )

    assert result.repeated_failing_tools == ("pytest",)
    assert result.metrics.successful_retries == 2


def test_abandoned_failure_lowers_recovery_rate():
    result = analyze_tool_error_recovery([ToolEvent("curl", STATUS_ERROR, 0)])

    assert result.metrics.abandoned_errors == 1
    assert result.metrics.recovery_rate == 0.0
    assert result.recovery_quality == "poor"


def test_mixed_tools_rounds_recovery_rate():
    result = analyze_tool_error_recovery(
        [
            ToolEvent("a", STATUS_ERROR, 0),
            ToolEvent("a", STATUS_SUCCESS, 1),
            ToolEvent("b", STATUS_ERROR, 2),
            ToolEvent("c", STATUS_ERROR, 3),
            ToolEvent("c", STATUS_SUCCESS, 5),
        ]
    )

    assert result.metrics.recovery_rate == 0.667
    assert result.metrics.average_recovery_latency == 1.5


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ("bad", "events"),
        ([{"tool_name": "pytest"}], "ToolEvent"),
        ([ToolEvent("", STATUS_ERROR, 0)], "tool_name"),
        ([ToolEvent("pytest", "failed", 0)], "status"),
        ([ToolEvent("pytest", "", 0)], "status"),
        ([ToolEvent("pytest", STATUS_ERROR, -1)], "turn_index"),
        ([ToolEvent("pytest", STATUS_ERROR, True)], "turn_index"),
        ([ToolEvent(123, STATUS_ERROR, 0)], "tool_name"),
        ([ToolEvent("pytest", STATUS_ERROR, 0, "")], "error_message"),
        ([ToolEvent("pytest", STATUS_SUCCESS, 0, "failed")], "error_message"),
        ([ToolEvent("a", STATUS_SUCCESS, 2), ToolEvent("b", STATUS_SUCCESS, 1)], "turn_index"),
    ],
)
def test_invalid_event_records_raise_value_error(events, message):
    with pytest.raises(ValueError, match=message):
        analyze_tool_error_recovery(events)
