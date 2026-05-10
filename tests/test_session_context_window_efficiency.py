"""Tests for session context window efficiency analyzer."""

import pytest

from synthesis.session_context_window_efficiency import (
    analyze_session_context_window_efficiency,
)


class TestAnalyzeSessionContextWindowEfficiency:
    """Tests for analyze_session_context_window_efficiency."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_context_window_efficiency([])
        assert result["total_sessions"] == 0
        assert result["avg_total_tokens"] == 0.0
        assert result["avg_tokens_per_tool_call"] == 0.0
        assert result["redundant_read_rate"] == 0.0
        assert result["large_output_tool_calls"] == 0
        assert result["large_output_rate"] == 0.0
        assert result["summarization_triggered_sessions"] == 0
        assert result["summarization_trigger_rate"] == 0.0
        assert result["avg_information_density"] == 0.0
        assert result["tokens_per_file_change"] == 0.0
        assert result["high_quality_sessions"] == 0
        assert result["low_quality_sessions"] == 0
        assert result["context_window_efficiency_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_context_window_efficiency(None)
        assert result["total_sessions"] == 0
        assert result["avg_total_tokens"] == 0.0
        assert result["context_window_efficiency_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_context_window_efficiency("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_context_window_efficiency(123)

    def test_single_session_high_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_tokens_used": 50000,
                "total_tool_calls": 100,
                "redundant_file_reads": 1,
                "total_file_reads": 50,
                "large_output_tool_calls": 2,
                "total_tool_output_calls": 80,
                "summarization_triggered": False,
                "total_file_changes": 10,
                "information_density_score": 0.85,
            }
        ]
        result = analyze_session_context_window_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["avg_total_tokens"] == 50000.0
        assert result["avg_tokens_per_tool_call"] == 500.0
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        # Low redundant (2%), low large output (2.5%), no summarization, high density
        # All components should be at max: 0.30 + 0.25 + 0.25 + 0.20 = 1.0
        assert result["context_window_efficiency_score"] == 1.0

    def test_single_session_low_quality(self) -> None:
        records = [
            {
                "session_id": "s2",
                "total_tokens_used": 200000,
                "total_tool_calls": 50,
                "redundant_file_reads": 40,
                "total_file_reads": 50,
                "large_output_tool_calls": 30,
                "total_tool_output_calls": 50,
                "summarization_triggered": True,
                "total_file_changes": 2,
                "information_density_score": 0.2,
            }
        ]
        result = analyze_session_context_window_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["summarization_triggered_sessions"] == 1
        assert result["summarization_trigger_rate"] == 100.0
        assert result["high_quality_sessions"] == 0
        assert result["low_quality_sessions"] == 1
        assert result["context_window_efficiency_score"] < 0.4

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "high",
                "total_tokens_used": 40000,
                "total_tool_calls": 80,
                "redundant_file_reads": 2,
                "total_file_reads": 40,
                "large_output_tool_calls": 1,
                "total_tool_output_calls": 60,
                "summarization_triggered": False,
                "total_file_changes": 8,
                "information_density_score": 0.9,
            },
            {
                "session_id": "low",
                "total_tokens_used": 180000,
                "total_tool_calls": 40,
                "redundant_file_reads": 35,
                "total_file_reads": 40,
                "large_output_tool_calls": 25,
                "total_tool_output_calls": 40,
                "summarization_triggered": True,
                "total_file_changes": 1,
                "information_density_score": 0.15,
            },
        ]
        result = analyze_session_context_window_efficiency(records)
        assert result["total_sessions"] == 2
        assert result["avg_total_tokens"] == 110000.0
        assert result["summarization_triggered_sessions"] == 1
        assert result["summarization_trigger_rate"] == 50.0
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            {
                "session_id": "valid",
                "total_tokens_used": 30000,
                "total_tool_calls": 50,
                "redundant_file_reads": 1,
                "total_file_reads": 20,
                "large_output_tool_calls": 1,
                "total_tool_output_calls": 30,
                "summarization_triggered": False,
                "total_file_changes": 5,
                "information_density_score": 0.8,
            },
            "not a dict",
            42,
            None,
            ["a", "b"],
        ]
        result = analyze_session_context_window_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1

    def test_zero_tool_calls_session(self) -> None:
        records = [
            {
                "session_id": "zero",
                "total_tokens_used": 1000,
                "total_tool_calls": 0,
                "redundant_file_reads": 0,
                "total_file_reads": 0,
                "large_output_tool_calls": 0,
                "total_tool_output_calls": 0,
                "summarization_triggered": False,
                "total_file_changes": 0,
                "information_density_score": 0.0,
            }
        ]
        result = analyze_session_context_window_efficiency(records)
        assert result["total_sessions"] == 1
        assert result["avg_tokens_per_tool_call"] == 0.0
        assert result["redundant_read_rate"] == 0.0
        assert result["large_output_rate"] == 0.0
        assert result["tokens_per_file_change"] == 0.0

    def test_result_keys_complete(self) -> None:
        result = analyze_session_context_window_efficiency([])
        expected_keys = {
            "total_sessions",
            "avg_total_tokens",
            "avg_tokens_per_tool_call",
            "redundant_read_rate",
            "large_output_tool_calls",
            "large_output_rate",
            "summarization_triggered_sessions",
            "summarization_trigger_rate",
            "avg_information_density",
            "tokens_per_file_change",
            "high_quality_sessions",
            "low_quality_sessions",
            "context_window_efficiency_score",
        }
        assert set(result.keys()) == expected_keys
