"""Tests for session tool call error recovery speed analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_tool_call_error_recovery_speed import (
    analyze_session_tool_call_error_recovery_speed,
)


class TestAnalyzeSessionToolCallErrorRecoverySpeed:
    """Tests for analyze_session_tool_call_error_recovery_speed."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_tool_call_error_recovery_speed([])
        assert result["total_sessions"] == 0
        assert result["total_tool_errors"] == 0
        assert result["error_recovery_speed_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_tool_call_error_recovery_speed(None)
        assert result["total_sessions"] == 0
        assert result["error_recovery_speed_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_tool_call_error_recovery_speed("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_tool_call_error_recovery_speed(42)

    def test_fast_recovery_scores_high(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_tool_errors": 5,
                "errors_recovered": 5,
                "errors_unrecovered": 0,
                "immediate_recovery": 3,
                "actions_to_recover_values": [1, 1, 2, 1, 1],
            }
        ]
        result = analyze_session_tool_call_error_recovery_speed(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["error_recovery_speed_score"] > 0.7

    def test_slow_recovery_scores_low(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_tool_errors": 10,
                "errors_recovered": 3,
                "errors_unrecovered": 7,
                "immediate_recovery": 0,
                "actions_to_recover_values": [6, 8, 7],
            }
        ]
        result = analyze_session_tool_call_error_recovery_speed(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["error_recovery_speed_score"] < 0.4

    def test_zero_errors_gets_full_score(self) -> None:
        records = [
            {
                "session_id": "clean",
                "total_tool_errors": 0,
                "errors_recovered": 0,
                "errors_unrecovered": 0,
                "immediate_recovery": 0,
                "actions_to_recover_values": [],
            }
        ]
        result = analyze_session_tool_call_error_recovery_speed(records)
        assert result["error_recovery_speed_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "fast",
                "total_tool_errors": 5,
                "errors_recovered": 5,
                "errors_unrecovered": 0,
                "immediate_recovery": 3,
                "actions_to_recover_values": [1, 1, 2, 1, 1],
            },
            {
                "session_id": "slow",
                "total_tool_errors": 10,
                "errors_recovered": 3,
                "errors_unrecovered": 7,
                "immediate_recovery": 0,
                "actions_to_recover_values": [6, 8, 7],
            },
        ]
        result = analyze_session_tool_call_error_recovery_speed(records)
        assert result["total_sessions"] == 2
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "session_id": "valid",
                "total_tool_errors": 2,
                "errors_recovered": 2,
                "errors_unrecovered": 0,
                "immediate_recovery": 1,
                "actions_to_recover_values": [1, 2],
            },
        ]
        result = analyze_session_tool_call_error_recovery_speed(records)
        assert result["total_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_tool_call_error_recovery_speed([])
        expected_keys = {
            "total_sessions",
            "total_tool_errors",
            "errors_recovered",
            "recovery_rate",
            "errors_unrecovered",
            "unrecovered_rate",
            "immediate_recovery",
            "immediate_recovery_rate",
            "avg_actions_to_recover",
            "high_quality_sessions",
            "low_quality_sessions",
            "error_recovery_speed_score",
        }
        assert set(result.keys()) == expected_keys
