"""Tests for tool result follow-up latency analysis."""

import pytest

from synthesis.tool_result_followup_latency import (
    ToolResultFollowupEvent,
    analyze_tool_result_followup_latency,
)


def test_empty_session_returns_zero_metrics():
    report = analyze_tool_result_followup_latency([])

    assert report.total_results == 0
    assert report.average_followup_latency == 0.0
    assert report.stale_results == 0


def test_immediate_followup_counts_zero_latency():
    report = analyze_tool_result_followup_latency(
        [
            ToolResultFollowupEvent(0, "result", "exec", "tests"),
            ToolResultFollowupEvent(0, "action", "exec", "tests"),
        ]
    )

    assert report.followed_results == 1
    assert report.average_followup_latency == 0.0


def test_delayed_followup_within_threshold_is_averaged():
    report = analyze_tool_result_followup_latency(
        [
            ToolResultFollowupEvent(0, "result", "exec", "tests"),
            ToolResultFollowupEvent(2, "action", "exec", "tests"),
        ],
        stale_threshold=3,
    )

    assert report.average_followup_latency == 2.0
    assert report.maximum_followup_latency == 2


def test_stale_results_without_followup_are_counted():
    report = analyze_tool_result_followup_latency(
        [ToolResultFollowupEvent(0, "result", "exec", "tests")]
    )

    assert report.stale_results == 1
    assert report.stale_results_by_tool == (("exec", 1),)


def test_stale_grouping_is_sorted_by_tool_name():
    report = analyze_tool_result_followup_latency(
        [
            ToolResultFollowupEvent(0, "result", "web", "docs"),
            ToolResultFollowupEvent(1, "result", "exec", "tests"),
        ]
    )

    assert report.stale_results_by_tool == (("exec", 1), ("web", 1))


def test_configurable_threshold_changes_stale_behavior():
    events = [
        ToolResultFollowupEvent(0, "result", "exec", "tests"),
        ToolResultFollowupEvent(2, "action", "exec", "tests"),
    ]

    assert analyze_tool_result_followup_latency(events, stale_threshold=1).stale_results == 1
    assert analyze_tool_result_followup_latency(events, stale_threshold=2).stale_results == 0


def test_invalid_inputs_raise_value_error():
    with pytest.raises(ValueError, match="stale_threshold"):
        analyze_tool_result_followup_latency([], stale_threshold=-1)
    with pytest.raises(ValueError, match="ordered"):
        analyze_tool_result_followup_latency(
            [
                ToolResultFollowupEvent(2, "result", "exec", "tests"),
                ToolResultFollowupEvent(1, "action", "exec", "tests"),
            ]
        )
    with pytest.raises(ValueError, match="event_type"):
        analyze_tool_result_followup_latency([ToolResultFollowupEvent(0, "other", "exec", "tests")])
