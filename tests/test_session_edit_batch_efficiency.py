"""Tests for session Edit batch efficiency analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_edit_batch_efficiency import analyze_session_edit_batch_efficiency


class TestAnalyzeSessionEditBatchEfficiency:
    """Tests for analyze_session_edit_batch_efficiency."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_edit_batch_efficiency([])
        assert result["total_sessions"] == 0
        assert result["total_edit_calls"] == 0
        assert result["total_files_edited"] == 0
        assert result["edit_batch_efficiency_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_edit_batch_efficiency(None)
        assert result["total_sessions"] == 0
        assert result["total_edit_calls"] == 0
        assert result["edit_batch_efficiency_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_edit_batch_efficiency("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_edit_batch_efficiency(42)

    def test_single_edit_per_file_scores_high(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_edit_calls": 5,
                "total_files_edited": 5,
                "consecutive_same_file_edits": 0,
                "single_edit_files": 5,
                "multi_edit_files": 0,
                "max_consecutive_same_file": 1,
                "replace_all_usage_count": 1,
            }
        ]
        result = analyze_session_edit_batch_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["edit_batch_efficiency_score"] > 0.7

    def test_many_consecutive_same_file_scores_low(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_edit_calls": 12,
                "total_files_edited": 2,
                "consecutive_same_file_edits": 10,
                "single_edit_files": 0,
                "multi_edit_files": 2,
                "max_consecutive_same_file": 10,
                "replace_all_usage_count": 0,
            }
        ]
        result = analyze_session_edit_batch_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["edit_batch_efficiency_score"] < 0.4

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "efficient",
                "total_edit_calls": 4,
                "total_files_edited": 4,
                "consecutive_same_file_edits": 0,
                "single_edit_files": 4,
                "multi_edit_files": 0,
                "max_consecutive_same_file": 1,
                "replace_all_usage_count": 1,
            },
            {
                "session_id": "inefficient",
                "total_edit_calls": 12,
                "total_files_edited": 2,
                "consecutive_same_file_edits": 10,
                "single_edit_files": 0,
                "multi_edit_files": 2,
                "max_consecutive_same_file": 10,
                "replace_all_usage_count": 0,
            },
        ]
        result = analyze_session_edit_batch_efficiency(records)
        assert result["total_sessions"] == 2
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1

    def test_zero_edit_calls_gets_full_score(self) -> None:
        records = [
            {
                "session_id": "no_edits",
                "total_edit_calls": 0,
                "total_files_edited": 0,
                "consecutive_same_file_edits": 0,
                "single_edit_files": 0,
                "multi_edit_files": 0,
                "max_consecutive_same_file": 0,
                "replace_all_usage_count": 0,
            }
        ]
        result = analyze_session_edit_batch_efficiency(records)
        assert result["edit_batch_efficiency_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "session_id": "valid",
                "total_edit_calls": 3,
                "total_files_edited": 3,
                "consecutive_same_file_edits": 0,
                "single_edit_files": 3,
                "multi_edit_files": 0,
                "max_consecutive_same_file": 1,
                "replace_all_usage_count": 0,
            },
        ]
        result = analyze_session_edit_batch_efficiency(records)
        assert result["total_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_edit_batch_efficiency([])
        expected_keys = {
            "total_sessions",
            "total_edit_calls",
            "total_files_edited",
            "edits_per_file_avg",
            "consecutive_same_file_edits",
            "consecutive_same_file_rate",
            "single_edit_files",
            "multi_edit_files",
            "max_consecutive_same_file",
            "replace_all_usage_count",
            "replace_all_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "edit_batch_efficiency_score",
        }
        assert set(result.keys()) == expected_keys
