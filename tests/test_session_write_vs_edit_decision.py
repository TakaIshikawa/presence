"""Tests for session Write vs Edit decision appropriateness analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_write_vs_edit_decision import analyze_session_write_vs_edit_decision


class TestAnalyzeSessionWriteVsEditDecision:
    """Tests for analyze_session_write_vs_edit_decision."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_write_vs_edit_decision([])
        assert result["total_sessions"] == 0
        assert result["total_write_calls"] == 0
        assert result["total_edit_calls"] == 0
        assert result["write_vs_edit_decision_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_write_vs_edit_decision(None)
        assert result["total_sessions"] == 0
        assert result["write_vs_edit_decision_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_write_vs_edit_decision("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_write_vs_edit_decision(42)

    def test_all_appropriate_decisions_scores_high(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_write_calls": 3,
                "total_edit_calls": 7,
                "write_to_existing_file": 0,
                "edit_to_new_file": 0,
                "appropriate_write": 3,
                "appropriate_edit": 7,
            }
        ]
        result = analyze_session_write_vs_edit_decision(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["write_vs_edit_decision_score"] > 0.7

    def test_writing_to_existing_files_scores_low(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_write_calls": 10,
                "total_edit_calls": 2,
                "write_to_existing_file": 8,
                "edit_to_new_file": 1,
                "appropriate_write": 2,
                "appropriate_edit": 1,
            }
        ]
        result = analyze_session_write_vs_edit_decision(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["write_vs_edit_decision_score"] < 0.4

    def test_zero_calls_gets_full_score(self) -> None:
        records = [
            {
                "session_id": "no_file_ops",
                "total_write_calls": 0,
                "total_edit_calls": 0,
                "write_to_existing_file": 0,
                "edit_to_new_file": 0,
                "appropriate_write": 0,
                "appropriate_edit": 0,
            }
        ]
        result = analyze_session_write_vs_edit_decision(records)
        assert result["write_vs_edit_decision_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "session_id": "valid",
                "total_write_calls": 2,
                "total_edit_calls": 3,
                "write_to_existing_file": 0,
                "edit_to_new_file": 0,
                "appropriate_write": 2,
                "appropriate_edit": 3,
            },
        ]
        result = analyze_session_write_vs_edit_decision(records)
        assert result["total_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_write_vs_edit_decision([])
        expected_keys = {
            "total_sessions",
            "total_write_calls",
            "total_edit_calls",
            "write_to_existing_file",
            "write_to_existing_rate",
            "edit_to_new_file",
            "edit_to_new_rate",
            "appropriate_write",
            "appropriate_write_rate",
            "appropriate_edit",
            "appropriate_edit_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "write_vs_edit_decision_score",
        }
        assert set(result.keys()) == expected_keys
