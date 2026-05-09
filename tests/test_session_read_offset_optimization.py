"""Tests for session Read offset/limit optimization analyzer."""

import pytest

from synthesis.session_read_offset_optimization import analyze_session_read_offset_optimization


class TestAnalyzeSessionReadOffsetOptimization:
    """Test main analyzer function."""

    def test_empty_session(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_read_offset_optimization([])
        assert result["total_turns"] == 0
        assert result["read_invocations"] == 0
        assert result["optimization_score"] == 0.0

    def test_none_input(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_read_offset_optimization(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_offset_optimization("not a list")

    def test_targeted_reads(self):
        """Verify reads with offset/limit parameters."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "offset": 10, "limit": 20, "lines_read": 20},
            {"turn_index": 1, "tool_name": "Read", "offset": 0, "limit": 30, "lines_read": 30},
        ])

        assert result["read_invocations"] == 2
        assert result["reads_with_offset_limit"] == 2
        assert result["offset_limit_percentage"] == 100.0
        assert result["avg_lines_read"] == 25.0

    def test_full_reads(self):
        """Verify full reads without offset/limit."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "lines_read": 100},
            {"turn_index": 1, "tool_name": "Read", "lines_read": 150},
        ])

        assert result["reads_with_offset_limit"] == 0
        assert result["offset_limit_percentage"] == 0.0

    def test_redundant_full_reads_after_edits(self):
        """Verify detection of full reads after edits."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "after_edit": True, "lines_read": 200},
            {"turn_index": 1, "tool_name": "Read", "after_edit": False, "lines_read": 50},
        ])

        assert result["redundant_full_reads"] == 1
        assert result["redundant_read_ratio"] == 50.0

    def test_cache_opportunities(self):
        """Verify cache opportunity detection."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "cache_available": True, "lines_read": 100},
            {"turn_index": 1, "tool_name": "Read", "cache_available": False, "lines_read": 50},
            {"turn_index": 2, "tool_name": "Read", "cache_available": True, "lines_read": 75},
        ])

        assert result["cache_opportunities"] == 2
        assert result["cache_opportunity_ratio"] == 66.67

    def test_token_savings_calculation(self):
        """Verify token savings estimation."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "lines_read": 30},
            {"turn_index": 1, "tool_name": "Read", "lines_read": 40},
        ])

        # Baseline: 2 reads * 64 lines * 4 tokens = 512
        # Actual: (30 + 40) * 4 = 280
        # Savings: 512 - 280 = 232 (45.31%)
        assert result["baseline_tokens"] == 512
        assert result["actual_tokens"] == 280
        assert result["token_savings"] == 232
        assert result["token_savings_percentage"] == 45.31

    def test_optimization_score_perfect(self):
        """Verify optimization score with perfect usage."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "offset": 0, "limit": 30, "lines_read": 30},
            {"turn_index": 1, "tool_name": "Read", "offset": 10, "limit": 40, "lines_read": 40},
        ])

        # 100% offset/limit, low lines, no redundant, good savings
        assert result["optimization_score"] >= 0.8

    def test_optimization_score_poor(self):
        """Verify optimization score with poor usage."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "lines_read": 200, "after_edit": True},
            {"turn_index": 1, "tool_name": "Read", "lines_read": 300, "after_edit": True},
        ])

        # 0% offset/limit, high lines, all redundant
        assert result["optimization_score"] < 0.3
