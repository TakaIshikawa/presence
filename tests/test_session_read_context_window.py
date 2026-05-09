"""Tests for session Read tool context window analyzer."""

import pytest

from synthesis.session_read_context_window import analyze_session_read_context_window


class TestAnalyzeSessionReadContextWindow:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_read_context_window([])

        assert result["total_tool_calls"] == 0
        assert result["read_call_count"] == 0
        assert result["targeted_read_count"] == 0
        assert result["full_read_count"] == 0
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 0.0
        assert result["avg_lines_targeted_read"] == 0.0
        assert result["avg_lines_full_read"] == 0.0
        assert result["window_size_distribution"]["small_window_percentage"] == 0.0
        assert result["window_size_distribution"]["medium_window_percentage"] == 0.0
        assert result["window_size_distribution"]["large_window_percentage"] == 0.0
        assert result["small_window_reads"] == 0
        assert result["medium_window_reads"] == 0
        assert result["large_window_reads"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_read_context_window(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_context_window("not a list")

    def test_single_targeted_read(self):
        """Verify single targeted Read call is tracked."""
        result = analyze_session_read_context_window([
            {
                "tool_name": "Read",
                "file_path": "file.py",
                "offset": 100,
                "limit": 30,
                "lines_read": 30,
                "turn_index": 0
            }
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0
        assert result["targeted_read_percentage"] == 100.0
        assert result["avg_lines_per_read"] == 30.0
        assert result["avg_lines_targeted_read"] == 30.0

    def test_single_full_read(self):
        """Verify single full-file Read call is tracked."""
        result = analyze_session_read_context_window([
            {
                "tool_name": "Read",
                "file_path": "file.py",
                "lines_read": 500,
                "turn_index": 0
            }
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 0
        assert result["full_read_count"] == 1
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 500.0
        assert result["avg_lines_full_read"] == 500.0

    def test_mixed_targeted_and_full_reads(self):
        """Verify mixed targeted and full reads are categorized correctly."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "file1.py", "offset": 0, "limit": 50, "lines_read": 50, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 300, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "offset": 200, "lines_read": 40, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "lines_read": 250, "turn_index": 3},
        ])

        assert result["read_call_count"] == 4
        assert result["targeted_read_count"] == 2  # offset or limit present
        assert result["full_read_count"] == 2  # no offset or limit
        assert result["targeted_read_percentage"] == 50.0

    def test_targeted_read_percentage_calculation(self):
        """Verify targeted read percentage calculation."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "offset": 0, "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "offset": 100, "limit": 20, "lines_read": 20, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "offset": 50, "limit": 40, "lines_read": 40, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "lines_read": 500, "turn_index": 3},
        ])

        # 3 targeted / 4 total = 75%
        assert result["targeted_read_percentage"] == 75.0

    def test_average_lines_per_read_calculation(self):
        """Verify average lines per read calculation."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 50, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 100, "turn_index": 2},
        ])

        # (30 + 50 + 100) / 3 = 60
        assert result["avg_lines_per_read"] == 60.0

    def test_average_lines_targeted_vs_full_reads(self):
        """Verify separate averages for targeted and full reads."""
        result = analyze_session_read_context_window([
            # Targeted reads
            {"tool_name": "Read", "file_path": "f1.py", "offset": 0, "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "offset": 100, "limit": 50, "lines_read": 50, "turn_index": 1},
            # Full reads
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 400, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "lines_read": 600, "turn_index": 3},
        ])

        # Targeted: (30 + 50) / 2 = 40
        assert result["avg_lines_targeted_read"] == 40.0
        # Full: (400 + 600) / 2 = 500
        assert result["avg_lines_full_read"] == 500.0

    def test_window_size_distribution_small(self):
        """Verify small window size categorization (< 50 lines)."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 10, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 45, "turn_index": 2},
        ])

        assert result["small_window_reads"] == 3
        assert result["medium_window_reads"] == 0
        assert result["large_window_reads"] == 0
        assert result["window_size_distribution"]["small_window_percentage"] == 100.0

    def test_window_size_distribution_medium(self):
        """Verify medium window size categorization (50-200 lines)."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 50, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 100, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 200, "turn_index": 2},
        ])

        assert result["small_window_reads"] == 0
        assert result["medium_window_reads"] == 3
        assert result["large_window_reads"] == 0
        assert result["window_size_distribution"]["medium_window_percentage"] == 100.0

    def test_window_size_distribution_large(self):
        """Verify large window size categorization (> 200 lines)."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 201, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 500, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 1000, "turn_index": 2},
        ])

        assert result["small_window_reads"] == 0
        assert result["medium_window_reads"] == 0
        assert result["large_window_reads"] == 3
        assert result["window_size_distribution"]["large_window_percentage"] == 100.0

    def test_window_size_distribution_mixed(self):
        """Verify mixed window size distribution."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 100, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "lines_read": 100, "turn_index": 3},
            {"tool_name": "Read", "file_path": "f5.py", "lines_read": 500, "turn_index": 4},
        ])

        assert result["small_window_reads"] == 2
        assert result["medium_window_reads"] == 2
        assert result["large_window_reads"] == 1
        # 2 small / 5 total = 40%
        assert result["window_size_distribution"]["small_window_percentage"] == 40.0
        # 2 medium / 5 total = 40%
        assert result["window_size_distribution"]["medium_window_percentage"] == 40.0
        # 1 large / 5 total = 20%
        assert result["window_size_distribution"]["large_window_percentage"] == 20.0

    def test_read_with_only_offset_is_targeted(self):
        """Verify Read with offset only is considered targeted."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "offset": 100, "lines_read": 50, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0

    def test_read_with_only_limit_is_targeted(self):
        """Verify Read with limit only is considered targeted."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "limit": 50, "lines_read": 50, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0

    def test_read_with_both_offset_and_limit_is_targeted(self):
        """Verify Read with both offset and limit is considered targeted."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "offset": 100, "limit": 50, "lines_read": 50, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0

    def test_read_without_lines_read_is_counted(self):
        """Verify Read without lines_read field is still counted."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "offset": 0, "limit": 30, "turn_index": 0}
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 1
        # No lines_read, so no averages calculated
        assert result["avg_lines_per_read"] == 0.0

    def test_mixed_tool_calls_only_counts_reads(self):
        """Verify only Read tool calls are analyzed."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 50, "turn_index": 0},
            {"tool_name": "Write", "file_path": "f2.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "f3.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "lines_read": 100, "turn_index": 3},
            {"tool_name": "Bash", "command": "ls", "turn_index": 4},
        ])

        assert result["total_tool_calls"] == 5
        assert result["read_call_count"] == 2

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_read_context_window([
            {"tool_name": "READ", "file_path": "f1.py", "lines_read": 50, "turn_index": 0},
            {"tool_name": "read", "file_path": "f2.py", "lines_read": 100, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 150, "turn_index": 2},
        ])

        assert result["read_call_count"] == 3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_read_context_window([
            "not a dict",
            {"tool_name": "Read", "file_path": "f.py", "lines_read": 50, "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_read_context_window([
            {"file_path": "f.py", "lines_read": 50, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 100, "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_lines_read_boolean_ignored(self):
        """Verify boolean lines_read is ignored."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "lines_read": True, "turn_index": 0},
        ])

        assert result["read_call_count"] == 1
        assert result["avg_lines_per_read"] == 0.0

    def test_zero_denominator_in_percentage(self):
        """Verify zero denominator in percentage calculation."""
        result = analyze_session_read_context_window([
            {"tool_name": "Write", "file_path": "f.py", "turn_index": 0},
        ])

        # No Read calls
        assert result["targeted_read_percentage"] == 0.0

    def test_optimal_pattern_high_targeted_reads(self):
        """Verify optimal pattern of high targeted read usage."""
        result = analyze_session_read_context_window([
            # Mostly targeted reads with small windows
            {"tool_name": "Read", "file_path": "f1.py", "offset": 0, "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "offset": 100, "limit": 40, "lines_read": 40, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "offset": 200, "limit": 50, "lines_read": 50, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "offset": 50, "limit": 30, "lines_read": 30, "turn_index": 3},
            # One strategic full read for exploration
            {"tool_name": "Read", "file_path": "f5.py", "lines_read": 300, "turn_index": 4},
        ])

        # 80% targeted reads = good optimization
        assert result["targeted_read_percentage"] == 80.0
        # Small average window for targeted reads = efficient
        assert result["avg_lines_targeted_read"] == 37.5
        # 30, 40, 30 are small (< 50); 50 is medium
        assert result["small_window_reads"] == 3

    def test_anti_pattern_excessive_full_reads(self):
        """Verify anti-pattern of excessive full-file reads."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f1.py", "lines_read": 500, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "lines_read": 600, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "lines_read": 700, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "lines_read": 800, "turn_index": 3},
        ])

        # All full reads = anti-pattern (should use targeted)
        assert result["targeted_read_percentage"] == 0.0
        assert result["full_read_count"] == 4
        # High average lines per read = inefficient
        assert result["avg_lines_per_read"] == 650.0
        assert result["large_window_reads"] == 4

    def test_anti_pattern_large_targeted_reads(self):
        """Verify anti-pattern of large targeted reads."""
        result = analyze_session_read_context_window([
            # Targeted but with large limits
            {"tool_name": "Read", "file_path": "f1.py", "offset": 0, "limit": 500, "lines_read": 500, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "offset": 100, "limit": 600, "lines_read": 600, "turn_index": 1},
        ])

        # Technically targeted, but window too large
        assert result["targeted_read_percentage"] == 100.0
        assert result["avg_lines_targeted_read"] == 550.0
        assert result["large_window_reads"] == 2

    def test_optimal_pattern_87_percent_targeted(self):
        """Verify optimal pattern matching Run #1 results (87% targeted)."""
        result = analyze_session_read_context_window([
            # 13 targeted reads with small windows
            {"tool_name": "Read", "file_path": "f1.py", "offset": 0, "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "f2.py", "offset": 100, "limit": 50, "lines_read": 50, "turn_index": 1},
            {"tool_name": "Read", "file_path": "f3.py", "offset": 200, "limit": 40, "lines_read": 40, "turn_index": 2},
            {"tool_name": "Read", "file_path": "f4.py", "offset": 50, "limit": 60, "lines_read": 60, "turn_index": 3},
            {"tool_name": "Read", "file_path": "f5.py", "offset": 150, "limit": 70, "lines_read": 70, "turn_index": 4},
            {"tool_name": "Read", "file_path": "f6.py", "offset": 80, "limit": 45, "lines_read": 45, "turn_index": 5},
            {"tool_name": "Read", "file_path": "f7.py", "offset": 120, "limit": 55, "lines_read": 55, "turn_index": 6},
            {"tool_name": "Read", "file_path": "f8.py", "offset": 90, "limit": 65, "lines_read": 65, "turn_index": 7},
            {"tool_name": "Read", "file_path": "f9.py", "offset": 110, "limit": 75, "lines_read": 75, "turn_index": 8},
            {"tool_name": "Read", "file_path": "f10.py", "offset": 60, "limit": 80, "lines_read": 80, "turn_index": 9},
            {"tool_name": "Read", "file_path": "f11.py", "offset": 70, "limit": 90, "lines_read": 90, "turn_index": 10},
            {"tool_name": "Read", "file_path": "f12.py", "offset": 130, "limit": 85, "lines_read": 85, "turn_index": 11},
            {"tool_name": "Read", "file_path": "f13.py", "offset": 140, "limit": 95, "lines_read": 95, "turn_index": 12},
            # 2 strategic full reads
            {"tool_name": "Read", "file_path": "f14.py", "lines_read": 200, "turn_index": 13},
            {"tool_name": "Read", "file_path": "f15.py", "lines_read": 250, "turn_index": 14},
        ])

        # 13/15 = 86.67% (close to 87%)
        assert result["targeted_read_percentage"] == 86.67
        # Average: (30+50+40+60+70+45+55+65+75+80+90+85+95+200+250)/15 = 86.0
        assert result["avg_lines_per_read"] == 86.0

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_read_context_window([
            {"tool_name": "  Read  ", "file_path": "f.py", "lines_read": 50, "turn_index": 0},
        ])

        assert result["read_call_count"] == 1

    def test_offset_zero_is_targeted(self):
        """Verify offset=0 is considered targeted (explicit start position)."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "offset": 0, "limit": 50, "lines_read": 50, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1

    def test_negative_offset_is_targeted(self):
        """Verify negative offset (read from end) is considered targeted."""
        result = analyze_session_read_context_window([
            {"tool_name": "Read", "file_path": "f.py", "offset": -30, "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0
