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

    def test_full_file_reread_rate(self):
        """Verify full_file_reread_rate calculation."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "file_path": "main.py", "lines_read": 100},
            {"turn_index": 1, "tool_name": "Read", "file_path": "main.py", "lines_read": 100},
            {"turn_index": 2, "tool_name": "Read", "file_path": "test.py", "lines_read": 50},
        ])

        # 1 full-file reread out of 3 reads = 33.33%
        assert result["full_file_rereads"] == 1
        assert result["full_file_reread_rate"] == 33.33

    def test_full_file_reread_not_counted_with_offset_limit(self):
        """Verify targeted rereads not counted as full-file rereads."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "file_path": "main.py", "lines_read": 100},
            {"turn_index": 1, "tool_name": "Read", "file_path": "main.py", "offset": 50, "limit": 30, "lines_read": 30},
        ])

        # Second read is targeted, so no full-file reread
        assert result["full_file_rereads"] == 0
        assert result["full_file_reread_rate"] == 0.0

    def test_post_edit_targeted_rate(self):
        """Verify post_edit_targeted_rate calculation."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "after_edit": True, "offset": -30, "limit": 30, "lines_read": 30},
            {"turn_index": 1, "tool_name": "Read", "after_edit": True, "lines_read": 100},
            {"turn_index": 2, "tool_name": "Read", "after_edit": True, "offset": 0, "limit": 50, "lines_read": 50},
        ])

        # 2 out of 3 post-edit reads are targeted = 66.67%
        assert result["post_edit_reads"] == 3
        assert result["post_edit_targeted_reads"] == 2
        assert result["post_edit_targeted_rate"] == 66.67

    def test_post_edit_targeted_rate_zero_when_no_post_edit_reads(self):
        """Verify post_edit_targeted_rate is 0 when no post-edit reads."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "after_edit": False, "lines_read": 100},
        ])

        assert result["post_edit_targeted_rate"] == 0.0

    def test_cache_query_before_read_rate(self):
        """Verify cache_query_before_read_rate calculation."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "cache_query_before": True, "lines_read": 100},
            {"turn_index": 1, "tool_name": "Read", "cache_query_before": False, "lines_read": 50},
            {"turn_index": 2, "tool_name": "Read", "cache_query_before": True, "lines_read": 75},
            {"turn_index": 3, "tool_name": "Read", "cache_query_before": False, "lines_read": 60},
        ])

        # 2 out of 4 reads had cache query before = 50%
        assert result["cache_query_before_reads"] == 2
        assert result["cache_query_before_read_rate"] == 50.0

    def test_backward_compatibility_aliases(self):
        """Verify backward compatibility aliases are present."""
        result = analyze_session_read_offset_optimization([
            {"turn_index": 0, "tool_name": "Read", "offset": 0, "limit": 30, "lines_read": 30},
        ])

        # Check that old names still work
        assert result["offset_limit_percentage"] == result["offset_limit_usage_rate"]
        assert result["avg_lines_read"] == result["avg_lines_per_read"]

    def test_realistic_optimized_session(self):
        """Verify realistic optimized session pattern."""
        result = analyze_session_read_offset_optimization([
            # Initial exploration
            {"turn_index": 0, "tool_name": "Read", "file_path": "main.py", "lines_read": 150},
            {"turn_index": 1, "tool_name": "Read", "file_path": "test.py", "lines_read": 100},
            # Edit and targeted verification
            {"turn_index": 2, "tool_name": "Edit", "file_path": "main.py"},
            {"turn_index": 3, "tool_name": "Read", "file_path": "main.py", "after_edit": True, "offset": -30, "limit": 30, "lines_read": 30, "cache_query_before": True},
            # Another edit and targeted verification
            {"turn_index": 4, "tool_name": "Edit", "file_path": "test.py"},
            {"turn_index": 5, "tool_name": "Read", "file_path": "test.py", "after_edit": True, "offset": -40, "limit": 40, "lines_read": 40, "cache_query_before": True},
        ])

        # Should show good optimization patterns
        assert result["read_invocations"] == 4
        assert result["offset_limit_usage_rate"] == 50.0  # 2 out of 4
        assert result["avg_lines_per_read"] == 80.0  # (150+100+30+40)/4
        assert result["full_file_reread_rate"] == 0.0  # No full-file rereads
        assert result["post_edit_targeted_rate"] == 100.0  # 2 out of 2 post-edit reads targeted
        assert result["cache_query_before_read_rate"] == 50.0  # 2 out of 4

    def test_realistic_baseline_session(self):
        """Verify realistic baseline (unoptimized) session pattern."""
        result = analyze_session_read_offset_optimization([
            # Initial exploration
            {"turn_index": 0, "tool_name": "Read", "file_path": "main.py", "lines_read": 200},
            {"turn_index": 1, "tool_name": "Read", "file_path": "test.py", "lines_read": 150},
            # Edit and full-file re-verification
            {"turn_index": 2, "tool_name": "Edit", "file_path": "main.py"},
            {"turn_index": 3, "tool_name": "Read", "file_path": "main.py", "after_edit": True, "lines_read": 200},
            # Another edit and full-file re-verification
            {"turn_index": 4, "tool_name": "Edit", "file_path": "test.py"},
            {"turn_index": 5, "tool_name": "Read", "file_path": "test.py", "after_edit": True, "lines_read": 150},
        ])

        # Should show poor optimization patterns
        assert result["read_invocations"] == 4
        assert result["offset_limit_usage_rate"] == 0.0  # No offset/limit usage
        assert result["avg_lines_per_read"] == 175.0  # (200+150+200+150)/4
        assert result["full_file_reread_rate"] == 50.0  # 2 full-file rereads out of 4
        assert result["post_edit_targeted_rate"] == 0.0  # 0 out of 2 post-edit reads targeted
        assert result["cache_query_before_read_rate"] == 0.0  # No cache queries
