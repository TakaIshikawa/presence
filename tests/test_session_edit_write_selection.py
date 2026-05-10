"""Tests for session Edit vs Write tool selection analyzer."""

import pytest

from synthesis.session_edit_write_selection import analyze_session_edit_write_selection


class TestAnalyzeSessionEditWriteSelection:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_session_edit_write_selection([])
        assert result["total_sessions"] == 0
        assert result["edit_preference_score"] == 0.0
        assert result["tool_selection_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_session_edit_write_selection("not a list")

    def test_high_discipline_edit_preference(self):
        """Verify high Edit preference yields high score."""
        records = [
            {
                "total_edit_calls": 20,
                "existing_files_edited": 18,
                "existing_files_written": 2,
                "edit_with_prior_read": 19,
                "edit_without_prior_read": 1,
                "new_files_created": 3,
                "unnecessary_new_files": 0,
                "avg_edit_old_string_size": 100.0,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["edit_preference_score"] == 90.0
        assert result["read_before_edit_compliance"] == 95.0
        assert result["tool_selection_score"] > 0.80

    def test_edit_preference_calculation(self):
        """Verify Edit preference score calculation."""
        records = [
            {
                "existing_files_edited": 15,
                "existing_files_written": 5,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["edit_preference_score"] == 75.0
        assert result["write_to_existing_rate"] == 25.0

    def test_read_before_edit_compliance(self):
        """Verify Read-before-Edit compliance tracking."""
        records = [
            {
                "total_edit_calls": 20,
                "edit_with_prior_read": 19,
                "edit_without_prior_read": 1,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["read_before_edit_compliance"] == 95.0
        assert result["edit_without_read_rate"] == 5.0

    def test_write_to_existing_anti_pattern(self):
        """Verify Write-to-existing anti-pattern detection."""
        records = [
            {
                "existing_files_edited": 5,
                "existing_files_written": 10,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["write_to_existing_rate"] == 66.67

    def test_edit_without_read_violation(self):
        """Verify Edit-without-Read violation tracking."""
        records = [
            {
                "total_edit_calls": 10,
                "edit_with_prior_read": 7,
                "edit_without_prior_read": 3,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["edit_without_read_rate"] == 30.0

    def test_unnecessary_file_creation_detection(self):
        """Verify unnecessary file creation tracking."""
        records = [
            {
                "new_files_created": 10,
                "unnecessary_new_files": 3,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["unnecessary_file_creation_rate"] == 30.0

    def test_avg_edit_size_calculation(self):
        """Verify average edit old_string size calculation."""
        records = [
            {"avg_edit_old_string_size": 100.0},
            {"avg_edit_old_string_size": 200.0},
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["avg_edit_old_string_size"] == 150.0

    def test_replace_all_usage_rate(self):
        """Verify replace_all usage tracking."""
        records = [
            {
                "total_edit_calls": 20,
                "edit_with_prior_read": 20,
                "edit_replace_all_count": 5,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["replace_all_usage_rate"] == 25.0

    def test_edit_success_rate(self):
        """Verify Edit success rate calculation."""
        records = [
            {
                "edit_success_count": 18,
                "edit_failure_count": 2,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["edit_success_rate"] == 90.0

    def test_new_file_creation_rate(self):
        """Verify new file creation rate per session."""
        records = [
            {"new_files_created": 3},
            {"new_files_created": 5},
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["new_file_creation_rate"] == 4.0

    def test_optimal_edit_size_range(self):
        """Verify optimal edit size range (30-200 chars)."""
        records = [
            {
                "existing_files_edited": 10,
                "existing_files_written": 0,
                "total_edit_calls": 10,
                "edit_with_prior_read": 10,
                "avg_edit_old_string_size": 100.0,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        # Optimal edit size should contribute to high score
        assert result["tool_selection_score"] > 0.8

    def test_edit_size_too_small(self):
        """Verify small edit size reduces score."""
        records_optimal = [
            {
                "existing_files_edited": 10,
                "total_edit_calls": 10,
                "edit_with_prior_read": 10,
                "avg_edit_old_string_size": 100.0,
            }
        ]
        records_small = [
            {
                "existing_files_edited": 10,
                "total_edit_calls": 10,
                "edit_with_prior_read": 10,
                "avg_edit_old_string_size": 5.0,
            }
        ]
        result_optimal = analyze_session_edit_write_selection(records_optimal)
        result_small = analyze_session_edit_write_selection(records_small)
        assert result_optimal["tool_selection_score"] > result_small["tool_selection_score"]

    def test_multiple_sessions_aggregation(self):
        """Verify metrics aggregate across sessions."""
        records = [
            {
                "existing_files_edited": 8,
                "existing_files_written": 2,
                "total_edit_calls": 10,
                "edit_with_prior_read": 9,
                "edit_without_prior_read": 1,
            },
            {
                "existing_files_edited": 12,
                "existing_files_written": 3,
                "total_edit_calls": 15,
                "edit_with_prior_read": 14,
                "edit_without_prior_read": 1,
            },
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["total_sessions"] == 2
        # (8+12)/(8+12+2+3) = 20/25 = 80%
        assert result["edit_preference_score"] == 80.0
        # (9+14)/(9+14+1+1) = 23/25 = 92%
        assert result["read_before_edit_compliance"] == 92.0

    def test_high_discipline_sessions_count(self):
        """Verify high discipline sessions are counted."""
        records = [
            {
                "existing_files_edited": 18,
                "existing_files_written": 2,
                "total_edit_calls": 20,
                "edit_with_prior_read": 19,
                "new_files_created": 2,
                "unnecessary_new_files": 0,
                "avg_edit_old_string_size": 100.0,
            },
            {
                "existing_files_edited": 19,
                "existing_files_written": 1,
                "total_edit_calls": 20,
                "edit_with_prior_read": 19,
                "new_files_created": 3,
                "unnecessary_new_files": 0,
                "avg_edit_old_string_size": 120.0,
            },
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["high_discipline_sessions"] == 2

    def test_low_discipline_sessions_count(self):
        """Verify low discipline sessions are counted."""
        records = [
            {
                "existing_files_edited": 3,
                "existing_files_written": 12,
                "total_edit_calls": 10,
                "edit_with_prior_read": 5,
                "edit_without_prior_read": 5,
                "new_files_created": 10,
                "unnecessary_new_files": 7,
                "avg_edit_old_string_size": 5.0,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["low_discipline_sessions"] == 1

    def test_none_values_handled_gracefully(self):
        """Verify None values are handled without errors."""
        records = [
            {
                "existing_files_edited": 5,
                "total_edit_calls": None,
                "edit_with_prior_read": None,
            }
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["total_sessions"] == 1
        assert result["read_before_edit_compliance"] == 0.0

    def test_non_mapping_records_skipped(self):
        """Verify non-mapping records are skipped gracefully."""
        records = [
            "invalid",
            {
                "existing_files_edited": 10,
                "existing_files_written": 2,
            },
            123,
        ]
        result = analyze_session_edit_write_selection(records)
        assert result["total_sessions"] == 1
