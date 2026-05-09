"""Tests for pack tool call ordering analyzer."""

import pytest

from synthesis.pack_tool_call_ordering import analyze_pack_tool_call_ordering


class TestAnalyzePackToolCallOrdering:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_tool_call_ordering([])
        assert result["total_sessions"] == 0
        assert result["parallel_opportunities_count"] == 0
        assert result["dependency_correctness_rate"] == 0.0
        assert result["ordering_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_pack_tool_call_ordering("not a list")

    def test_efficient_ordering_high_score(self):
        """Verify efficient tool call ordering yields high score."""
        records = [
            {
                "parallel_opportunities_count": 30,
                "sequential_dependency_correct": 20,
                "sequential_dependency_violations": 0,
                "premature_calls_count": 0,
                "blocking_sequential_chains": 2,
                "tool_call_batches": 35,
                "total_tool_calls": 50,
            }
        ]
        result = analyze_pack_tool_call_ordering(records)
        assert result["parallelism_rate"] == 60.0
        assert result["dependency_correctness_rate"] == 100.0
        assert result["ordering_score"] > 0.80

    def test_parallelism_rate_calculation(self):
        """Verify parallelism rate is calculated correctly."""
        records = [{"parallel_opportunities_count": 45, "total_tool_calls": 100}]
        result = analyze_pack_tool_call_ordering(records)
        assert result["parallelism_rate"] == 45.0

    def test_dependency_violations_lower_score(self):
        """Verify dependency violations reduce correctness rate."""
        records = [
            {
                "sequential_dependency_correct": 7,
                "sequential_dependency_violations": 3,
            }
        ]
        result = analyze_pack_tool_call_ordering(records)
        assert result["dependency_correctness_rate"] == 70.0

    def test_premature_calls_penalty(self):
        """Verify premature calls reduce ordering score."""
        records_clean = [
            {
                "parallel_opportunities_count": 30,
                "sequential_dependency_correct": 20,
                "sequential_dependency_violations": 0,
                "premature_calls_count": 0,
                "total_tool_calls": 50,
            }
        ]
        records_premature = [
            {
                "parallel_opportunities_count": 30,
                "sequential_dependency_correct": 20,
                "sequential_dependency_violations": 0,
                "premature_calls_count": 5,
                "total_tool_calls": 50,
            }
        ]
        result_clean = analyze_pack_tool_call_ordering(records_clean)
        result_premature = analyze_pack_tool_call_ordering(records_premature)
        assert result_clean["ordering_score"] > result_premature["ordering_score"]

    def test_blocking_rate_calculation(self):
        """Verify blocking sequential chains rate."""
        records = [{"blocking_sequential_chains": 15, "total_tool_calls": 100}]
        result = analyze_pack_tool_call_ordering(records)
        assert result["blocking_rate"] == 15.0

    def test_batching_efficiency(self):
        """Verify batching efficiency calculation."""
        records = [{"parallel_opportunities_count": 60, "tool_call_batches": 80}]
        result = analyze_pack_tool_call_ordering(records)
        assert result["batching_efficiency"] == 75.0

    def test_multiple_sessions_aggregation(self):
        """Verify metrics aggregate across sessions."""
        records = [
            {
                "parallel_opportunities_count": 10,
                "total_tool_calls": 20,
                "sequential_dependency_correct": 5,
            },
            {
                "parallel_opportunities_count": 15,
                "total_tool_calls": 30,
                "sequential_dependency_correct": 8,
            },
        ]
        result = analyze_pack_tool_call_ordering(records)
        assert result["total_sessions"] == 2
        assert result["parallel_opportunities_count"] == 25
        assert result["total_tool_calls"] == 50
        assert result["sequential_dependency_correct"] == 13
