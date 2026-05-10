"""Tests for session Glob/Grep vs Task delegation analyzer."""

import pytest

from synthesis.session_glob_grep_task_delegation import (
    analyze_session_glob_grep_task_delegation,
)


class TestAnalyzeSessionGlobGrepTaskDelegation:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_session_glob_grep_task_delegation([])
        assert result["total_sessions"] == 0
        assert result["total_searches"] == 0
        assert result["tool_selection_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_session_glob_grep_task_delegation("not a list")

    def test_appropriate_delegation(self):
        """Verify appropriate delegation yields high score."""
        records = [
            {
                "total_glob_calls": 10,
                "total_grep_calls": 5,
                "total_task_search_delegations": 3,
                "simple_searches": 12,
                "complex_searches": 3,
                "simple_delegated_to_task": 0,
                "complex_using_direct_search": 0,
                "search_sequences_gt_3_rounds": 0,
                "grep_files_with_matches": 4,
                "grep_content_mode": 1,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["delegation_appropriateness_score"] == 100.0
        assert result["wasteful_delegation_rate"] == 0.0
        assert result["missing_delegation_rate"] == 0.0
        assert result["tool_selection_score"] > 0.85

    def test_wasteful_delegation_detection(self):
        """Verify wasteful delegation of simple searches."""
        records = [
            {
                "simple_searches": 10,
                "simple_delegated_to_task": 5,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["wasteful_delegation_rate"] == 50.0

    def test_missing_delegation_detection(self):
        """Verify complex searches not delegated."""
        records = [
            {
                "complex_searches": 10,
                "complex_using_direct_search": 7,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["missing_delegation_rate"] == 70.0

    def test_extended_sequence_detection(self):
        """Verify extended search sequences tracked."""
        records = [
            {
                "search_sequences_2_3_rounds": 5,
                "search_sequences_gt_3_rounds": 3,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["extended_sequence_rate"] == 37.5

    def test_grep_output_mode_appropriateness(self):
        """Verify Grep output mode selection tracking."""
        records = [
            {
                "grep_files_with_matches": 14,
                "grep_content_mode": 6,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["grep_output_mode_appropriateness"] == 70.0

    def test_search_success_rate(self):
        """Verify search success rate calculation."""
        records = [
            {
                "successful_searches": 17,
                "failed_searches": 3,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["search_success_rate"] == 85.0

    def test_high_appropriateness_sessions_count(self):
        """Verify high appropriateness sessions are counted."""
        records = [
            {
                "simple_searches": 10,
                "complex_searches": 3,
                "simple_delegated_to_task": 0,
                "complex_using_direct_search": 0,
                "search_sequences_gt_3_rounds": 0,
                "grep_files_with_matches": 8,
                "grep_content_mode": 2,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["high_appropriateness_sessions"] == 1

    def test_low_appropriateness_sessions_count(self):
        """Verify low appropriateness sessions are counted."""
        records = [
            {
                "simple_searches": 10,
                "complex_searches": 5,
                "simple_delegated_to_task": 7,
                "complex_using_direct_search": 4,
                "search_sequences_gt_3_rounds": 6,
                "grep_files_with_matches": 2,
                "grep_content_mode": 8,
            }
        ]
        result = analyze_session_glob_grep_task_delegation(records)
        assert result["low_appropriateness_sessions"] == 1
