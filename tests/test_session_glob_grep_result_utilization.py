"""Tests for session Glob/Grep result utilization analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_glob_grep_result_utilization import (
    analyze_session_glob_grep_result_utilization,
)


class TestAnalyzeSessionGlobGrepResultUtilization:
    """Tests for analyze_session_glob_grep_result_utilization."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_glob_grep_result_utilization([])
        assert result["total_sessions"] == 0
        assert result["total_search_calls"] == 0
        assert result["glob_grep_result_utilization_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_glob_grep_result_utilization(None)
        assert result["total_sessions"] == 0
        assert result["glob_grep_result_utilization_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_glob_grep_result_utilization("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_glob_grep_result_utilization(42)

    def test_high_utilization_scores_high(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_search_calls": 10,
                "total_results_returned": 20,
                "results_subsequently_read": 15,
                "zero_result_searches": 0,
                "redundant_searches": 0,
                "searches_with_no_followup": 1,
            }
        ]
        result = analyze_session_glob_grep_result_utilization(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["glob_grep_result_utilization_score"] > 0.7

    def test_many_no_followup_scores_low(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_search_calls": 10,
                "total_results_returned": 20,
                "results_subsequently_read": 2,
                "zero_result_searches": 4,
                "redundant_searches": 4,
                "searches_with_no_followup": 8,
            }
        ]
        result = analyze_session_glob_grep_result_utilization(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["glob_grep_result_utilization_score"] < 0.4

    def test_zero_search_calls_gets_full_score(self) -> None:
        records = [
            {
                "session_id": "no_searches",
                "total_search_calls": 0,
                "total_results_returned": 0,
                "results_subsequently_read": 0,
                "zero_result_searches": 0,
                "redundant_searches": 0,
                "searches_with_no_followup": 0,
            }
        ]
        result = analyze_session_glob_grep_result_utilization(records)
        assert result["glob_grep_result_utilization_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "session_id": "valid",
                "total_search_calls": 5,
                "total_results_returned": 10,
                "results_subsequently_read": 8,
                "zero_result_searches": 0,
                "redundant_searches": 0,
                "searches_with_no_followup": 0,
            },
        ]
        result = analyze_session_glob_grep_result_utilization(records)
        assert result["total_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_glob_grep_result_utilization([])
        expected_keys = {
            "total_sessions",
            "total_search_calls",
            "total_results_returned",
            "results_subsequently_read",
            "result_utilization_rate",
            "zero_result_searches",
            "zero_result_rate",
            "redundant_searches",
            "redundant_search_rate",
            "avg_results_per_search",
            "searches_with_no_followup",
            "no_followup_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "glob_grep_result_utilization_score",
        }
        assert set(result.keys()) == expected_keys
