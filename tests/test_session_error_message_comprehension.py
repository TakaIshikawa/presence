"""Tests for session error message comprehension and fix accuracy analyzer."""

import pytest

from synthesis.session_error_message_comprehension import (
    analyze_session_error_comprehension,
)


class TestAnalyzeSessionErrorComprehension:
    """Tests for analyze_session_error_comprehension."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_error_comprehension([])
        assert result["total_sessions"] == 0
        assert result["total_errors_encountered"] == 0
        assert result["errors_with_context_read"] == 0
        assert result["context_read_rate"] == 0.0
        assert result["first_fix_success_rate"] == 0.0
        assert result["avg_fix_attempts_per_error"] == 0.0
        assert result["error_suppression_count"] == 0
        assert result["high_quality_sessions"] == 0
        assert result["low_quality_sessions"] == 0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_error_comprehension(None)
        assert result["total_sessions"] == 0
        assert result["total_errors_encountered"] == 0
        assert result["error_comprehension_score"] == 1.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_error_comprehension("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_error_comprehension(42)

    def test_single_session_high_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_errors_encountered": 10,
                "errors_with_context_read": 9,
                "first_fix_success_count": 8,
                "total_fix_attempts": 10,
                "targeted_reads_for_errors": 8,
                "full_rereads_for_errors": 2,
                "error_suppressions": 0,
                "cascading_errors": 0,
                "session_total_tool_calls": 50,
            }
        ]
        result = analyze_session_error_comprehension(records)
        assert result["total_sessions"] == 1
        assert result["total_errors_encountered"] == 10
        assert result["errors_with_context_read"] == 9
        assert result["context_read_rate"] == 90.0
        assert result["first_fix_success_rate"] == 80.0
        assert result["error_suppression_count"] == 0
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        assert result["error_comprehension_score"] == 1.0

    def test_single_session_low_quality(self) -> None:
        records = [
            {
                "session_id": "s2",
                "total_errors_encountered": 10,
                "errors_with_context_read": 1,
                "first_fix_success_count": 2,
                "total_fix_attempts": 15,
                "targeted_reads_for_errors": 1,
                "full_rereads_for_errors": 9,
                "error_suppressions": 5,
                "cascading_errors": 3,
                "session_total_tool_calls": 80,
            }
        ]
        result = analyze_session_error_comprehension(records)
        assert result["total_sessions"] == 1
        assert result["total_errors_encountered"] == 10
        assert result["error_suppression_count"] == 5
        assert result["low_quality_sessions"] == 1
        assert result["high_quality_sessions"] == 0
        assert result["error_comprehension_score"] < 0.4

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "high",
                "total_errors_encountered": 10,
                "errors_with_context_read": 9,
                "first_fix_success_count": 8,
                "total_fix_attempts": 10,
                "targeted_reads_for_errors": 8,
                "full_rereads_for_errors": 2,
                "error_suppressions": 0,
                "cascading_errors": 0,
                "session_total_tool_calls": 50,
            },
            {
                "session_id": "low",
                "total_errors_encountered": 10,
                "errors_with_context_read": 1,
                "first_fix_success_count": 2,
                "total_fix_attempts": 15,
                "targeted_reads_for_errors": 1,
                "full_rereads_for_errors": 9,
                "error_suppressions": 5,
                "cascading_errors": 3,
                "session_total_tool_calls": 80,
            },
        ]
        result = analyze_session_error_comprehension(records)
        assert result["total_sessions"] == 2
        assert result["total_errors_encountered"] == 20
        assert result["errors_with_context_read"] == 10
        assert result["context_read_rate"] == 50.0
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["error_suppression_count"] == 5

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "session_id": "valid",
                "total_errors_encountered": 5,
                "errors_with_context_read": 5,
                "first_fix_success_count": 4,
                "total_fix_attempts": 5,
                "targeted_reads_for_errors": 4,
                "full_rereads_for_errors": 1,
                "error_suppressions": 0,
                "cascading_errors": 0,
                "session_total_tool_calls": 20,
            },
        ]
        result = analyze_session_error_comprehension(records)
        assert result["total_sessions"] == 1
        assert result["total_errors_encountered"] == 5

    def test_zero_errors_session(self) -> None:
        records = [
            {
                "session_id": "no_errors",
                "total_errors_encountered": 0,
                "errors_with_context_read": 0,
                "first_fix_success_count": 0,
                "total_fix_attempts": 0,
                "targeted_reads_for_errors": 0,
                "full_rereads_for_errors": 0,
                "error_suppressions": 0,
                "cascading_errors": 0,
                "session_total_tool_calls": 10,
            }
        ]
        result = analyze_session_error_comprehension(records)
        assert result["total_sessions"] == 1
        assert result["total_errors_encountered"] == 0
        assert result["context_read_rate"] == 0.0
        assert result["error_comprehension_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_error_comprehension([])
        expected_keys = {
            "total_sessions",
            "total_errors_encountered",
            "errors_with_context_read",
            "context_read_rate",
            "first_fix_success_rate",
            "avg_fix_attempts_per_error",
            "targeted_read_for_errors_rate",
            "full_reread_for_errors_rate",
            "error_suppression_count",
            "cascading_error_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "error_comprehension_score",
        }
        assert set(result.keys()) == expected_keys
