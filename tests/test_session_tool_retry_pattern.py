"""Tests for session tool retry pattern analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_tool_retry_pattern import analyze_session_tool_retry_pattern


class TestAnalyzeSessionToolRetryPattern:
    """Tests for analyze_session_tool_retry_pattern."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_tool_retry_pattern([])
        assert result["total_sessions"] == 0
        assert result["total_tool_failures"] == 0
        assert result["retry_rate"] == 0.0
        assert result["tool_retry_pattern_score"] == 0.0
        assert result["high_quality_sessions"] == 0
        assert result["low_quality_sessions"] == 0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_tool_retry_pattern(None)
        assert result["total_sessions"] == 0
        assert result["total_tool_failures"] == 0
        assert result["tool_retry_pattern_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_tool_retry_pattern("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_tool_retry_pattern(42)

    def test_single_session_high_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_tool_failures": 10,
                "total_retries": 8,
                "exact_retries": 2,
                "varied_retries": 6,
                "tool_switches_after_failure": 5,
                "retries_before_success_values": [1, 2, 1],
                "retries_before_giveup_values": [3, 4],
                "excessive_retries": 0,
                "appropriate_giveups": 2,
                "total_giveups": 2,
            }
        ]
        result = analyze_session_tool_retry_pattern(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        assert result["tool_retry_pattern_score"] > 0.7

    def test_single_session_low_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_tool_failures": 10,
                "total_retries": 10,
                "exact_retries": 10,
                "varied_retries": 0,
                "tool_switches_after_failure": 0,
                "retries_before_success_values": [5, 6],
                "retries_before_giveup_values": [8],
                "excessive_retries": 8,
                "appropriate_giveups": 0,
                "total_giveups": 3,
            }
        ]
        result = analyze_session_tool_retry_pattern(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["high_quality_sessions"] == 0
        assert result["tool_retry_pattern_score"] < 0.4

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "high",
                "total_tool_failures": 10,
                "total_retries": 8,
                "exact_retries": 2,
                "varied_retries": 6,
                "tool_switches_after_failure": 5,
                "retries_before_success_values": [1, 2],
                "retries_before_giveup_values": [3],
                "excessive_retries": 0,
                "appropriate_giveups": 1,
                "total_giveups": 1,
            },
            {
                "session_id": "low",
                "total_tool_failures": 10,
                "total_retries": 10,
                "exact_retries": 10,
                "varied_retries": 0,
                "tool_switches_after_failure": 0,
                "retries_before_success_values": [5],
                "retries_before_giveup_values": [8],
                "excessive_retries": 8,
                "appropriate_giveups": 0,
                "total_giveups": 3,
            },
        ]
        result = analyze_session_tool_retry_pattern(records)
        assert result["total_sessions"] == 2
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["avg_retries_before_success"] > 0
        assert result["avg_retries_before_giveup"] > 0

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "session_id": "valid",
                "total_tool_failures": 5,
                "total_retries": 3,
                "exact_retries": 1,
                "varied_retries": 2,
                "tool_switches_after_failure": 2,
                "retries_before_success_values": [1],
                "retries_before_giveup_values": [],
                "excessive_retries": 0,
                "appropriate_giveups": 0,
                "total_giveups": 0,
            },
        ]
        result = analyze_session_tool_retry_pattern(records)
        assert result["total_sessions"] == 1

    def test_zero_failures_session(self) -> None:
        records = [
            {
                "session_id": "clean",
                "total_tool_failures": 0,
                "total_retries": 0,
                "exact_retries": 0,
                "varied_retries": 0,
                "tool_switches_after_failure": 0,
                "retries_before_success_values": [],
                "retries_before_giveup_values": [],
                "excessive_retries": 0,
                "appropriate_giveups": 0,
                "total_giveups": 0,
            }
        ]
        result = analyze_session_tool_retry_pattern(records)
        assert result["total_sessions"] == 1
        assert result["total_tool_failures"] == 0
        # No failures means full score (no bad patterns detected)
        assert result["tool_retry_pattern_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_tool_retry_pattern([])
        expected_keys = {
            "total_sessions",
            "total_tool_failures",
            "retry_rate",
            "exact_retry_rate",
            "varied_retry_rate",
            "tool_switch_after_failure_rate",
            "avg_retries_before_success",
            "avg_retries_before_giveup",
            "excessive_retry_rate",
            "appropriate_giveup_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "tool_retry_pattern_score",
        }
        assert set(result.keys()) == expected_keys
