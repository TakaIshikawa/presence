"""Tests for session tool call density analyzer."""

import pytest

from synthesis.session_tool_call_density import (
    analyze_session_tool_call_density,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_tool_call_density([])

    assert report["total_sessions"] == 0
    assert report["under_verification_count"] == 0
    assert report["over_verification_count"] == 0
    assert report["long_gap_count"] == 0
    assert report["examples"] == []


def test_normal_density_has_no_issues():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
        }
    ])

    assert report["total_sessions"] == 1
    assert report["under_verification_count"] == 0
    assert report["over_verification_count"] == 0


def test_under_verification_flagged():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 3,
            "file_change_count": 5,
        }
    ])

    assert report["under_verification_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "under_verification"


def test_over_verification_flagged():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 50,
            "file_change_count": 2,
        }
    ])

    assert report["over_verification_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "over_verification"


def test_long_gap_flagged():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [0.0, 100.0, 500.0],  # 400s gap
        }
    ])

    assert report["long_gap_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "long_gap"


def test_short_gaps_do_not_flag():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [0.0, 100.0, 200.0, 250.0],
        }
    ])

    assert report["long_gap_count"] == 0


def test_sessions_with_no_file_changes_skipped():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 0,
        }
    ])

    assert report["total_sessions"] == 1
    assert report["under_verification_count"] == 0


def test_multiple_sessions_analyzed_independently():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
        },
        {
            "session_id": "sess-2",
            "tool_call_count": 3,
            "file_change_count": 5,
        }
    ])

    assert report["total_sessions"] == 2
    assert report["under_verification_count"] == 1


def test_examples_capped_at_five():
    records = []
    for i in range(10):
        records.append({
            "session_id": f"sess-{i}",
            "tool_call_count": 3,
            "file_change_count": 5,
        })

    report = analyze_session_tool_call_density(records)

    assert report["under_verification_count"] == 10
    assert len(report["examples"]) == 5


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_tool_call_density({"session_id": "sess-1"})


def test_none_input_returns_zeroed_metrics():
    report = analyze_session_tool_call_density(None)

    assert report["total_sessions"] == 0


def test_non_dict_records_are_skipped():
    report = analyze_session_tool_call_density([
        "not a dict",
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
        }
    ])

    assert report["total_sessions"] == 1


def test_list_based_counts():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_calls": ["call1", "call2", "call3"],
            "changed_files": ["file1", "file2"],
        }
    ])

    assert report["total_sessions"] == 1
    # 3 tool calls, 2 files = 1.5 ratio, which is < 2.0
    assert report["under_verification_count"] == 1


def test_alternative_field_names():
    report = analyze_session_tool_call_density([
        {
            "sessionId": "sess-1",
            "toolCallCount": 10,
            "fileChangeCount": 3,
        }
    ])

    assert report["total_sessions"] == 1


def test_issue_percentage_calculation():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 3,
            "file_change_count": 5,
        },
        {
            "session_id": "sess-2",
            "tool_call_count": 10,
            "file_change_count": 3,
        }
    ])

    assert report["total_sessions"] == 2
    assert report["under_verification_count"] == 1
    assert report["issue_percentage"] == 50.0


def test_exactly_two_tool_calls_per_file_is_okay():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 5,
        }
    ])

    # 10 / 5 = 2.0, which is exactly at threshold
    assert report["under_verification_count"] == 0


def test_exactly_ten_tool_calls_per_file_is_okay():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 30,
            "file_change_count": 3,
        }
    ])

    # 30 / 3 = 10.0, which is exactly at threshold
    assert report["over_verification_count"] == 0


def test_exactly_five_minute_gap_is_okay():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [0.0, 300.0],  # Exactly 5 minutes
        }
    ])

    assert report["long_gap_count"] == 0


def test_five_minute_one_second_gap_flags():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [0.0, 301.0],  # 5 minutes 1 second
        }
    ])

    assert report["long_gap_count"] == 1


def test_unsorted_timestamps_are_sorted():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [500.0, 0.0, 100.0],
        }
    ])

    # Gap should be calculated on sorted: [0, 100, 500]
    # Largest gap is 500 - 100 = 400s
    assert report["long_gap_count"] == 1


def test_single_timestamp_no_gap():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": 10,
            "file_change_count": 3,
            "tool_call_timestamps": [0.0],
        }
    ])

    assert report["long_gap_count"] == 0


def test_boolean_values_ignored():
    report = analyze_session_tool_call_density([
        {
            "session_id": "sess-1",
            "tool_call_count": True,
            "file_change_count": False,
        }
    ])

    # Both counts should be 0 (booleans ignored)
    assert report["total_sessions"] == 1
    assert report["under_verification_count"] == 0
