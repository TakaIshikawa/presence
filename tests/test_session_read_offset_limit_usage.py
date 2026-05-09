"""Tests for session Read tool offset/limit usage analyzer."""

import pytest

from synthesis.session_read_offset_limit_usage import analyze_session_read_offset_limit_usage


class TestAnalyzeSessionReadOffsetLimitUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_read_offset_limit_usage([])

        assert result["total_tool_calls"] == 0
        assert result["read_call_count"] == 0
        assert result["targeted_read_count"] == 0
        assert result["full_read_count"] == 0
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 0.0
        assert result["avg_lines_targeted_read"] == 0.0
        assert result["avg_lines_full_read"] == 0.0
        assert result["reads_with_offset"] == 0
        assert result["reads_with_limit"] == 0
        assert result["reads_with_both"] == 0
        assert result["reads_with_negative_offset"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_read_offset_limit_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_read_offset_limit_usage("not a list")

    def test_single_full_read_no_parameters(self):
        """Verify single full Read call without offset/limit."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "lines_read": 100, "turn_index": 0}
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 0
        assert result["full_read_count"] == 1
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 100.0

    def test_single_targeted_read_with_limit(self):
        """Verify single targeted Read with limit parameter."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0
        assert result["targeted_read_percentage"] == 100.0
        assert result["reads_with_limit"] == 1
        assert result["avg_lines_per_read"] == 30.0

    def test_single_targeted_read_with_offset(self):
        """Verify single targeted Read with offset parameter."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "offset": 50, "lines_read": 150, "turn_index": 0}
        ])

        assert result["read_call_count"] == 1
        assert result["targeted_read_count"] == 1
        assert result["full_read_count"] == 0
        assert result["targeted_read_percentage"] == 100.0
        assert result["reads_with_offset"] == 1

    def test_targeted_read_with_both_offset_and_limit(self):
        """Verify targeted Read with both offset and limit parameters."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "offset": 100, "limit": 20, "lines_read": 20, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["reads_with_offset"] == 1
        assert result["reads_with_limit"] == 1
        assert result["reads_with_both"] == 1
        assert result["avg_lines_per_read"] == 20.0

    def test_negative_offset_for_tail_read(self):
        """Verify negative offset is tracked for tail reads."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "offset": -30, "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["reads_with_offset"] == 1
        assert result["reads_with_negative_offset"] == 1
        assert result["avg_lines_per_read"] == 30.0

    def test_mixed_targeted_and_full_reads(self):
        """Verify mix of targeted and full reads calculates correctly."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "lines_read": 200, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 30, "lines_read": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "offset": -50, "limit": 50, "lines_read": 50, "turn_index": 2},
        ])

        assert result["read_call_count"] == 3
        assert result["targeted_read_count"] == 2
        assert result["full_read_count"] == 1
        assert result["targeted_read_percentage"] == 66.67
        assert result["avg_lines_per_read"] == 93.33  # (200 + 30 + 50) / 3

    def test_targeted_read_percentage_calculation(self):
        """Verify targeted read percentage calculation with different ratios."""
        # 87% targeted (Run #1 benchmark)
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 20, "lines_read": 20, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "limit": 40, "lines_read": 40, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "limit": 50, "lines_read": 50, "turn_index": 3},
            {"tool_name": "Read", "file_path": "file5.py", "limit": 25, "lines_read": 25, "turn_index": 4},
            {"tool_name": "Read", "file_path": "file6.py", "limit": 35, "lines_read": 35, "turn_index": 5},
            {"tool_name": "Read", "file_path": "file7.py", "limit": 45, "lines_read": 45, "turn_index": 6},
            {"tool_name": "Read", "file_path": "file8.py", "lines_read": 300, "turn_index": 7},
        ])

        # 7 targeted / 8 total = 87.5%
        assert result["targeted_read_percentage"] == 87.5
        assert result["targeted_read_count"] == 7
        assert result["full_read_count"] == 1

    def test_average_lines_per_read_for_targeted_vs_full(self):
        """Verify separate averages for targeted and full reads."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 50, "lines_read": 50, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "lines_read": 200, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "lines_read": 300, "turn_index": 3},
        ])

        # Targeted: (30 + 50) / 2 = 40
        # Full: (200 + 300) / 2 = 250
        # Overall: (30 + 50 + 200 + 300) / 4 = 145
        assert result["avg_lines_targeted_read"] == 40.0
        assert result["avg_lines_full_read"] == 250.0
        assert result["avg_lines_per_read"] == 145.0

    def test_read_size_distribution_buckets(self):
        """Verify read size distribution into buckets."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},  # <50
            {"tool_name": "Read", "file_path": "file2.py", "limit": 70, "lines_read": 70, "turn_index": 1},  # 50-100
            {"tool_name": "Read", "file_path": "file3.py", "limit": 150, "lines_read": 150, "turn_index": 2},  # 100-200
            {"tool_name": "Read", "file_path": "file4.py", "lines_read": 250, "turn_index": 3},  # 200+
        ])

        dist = result["read_size_distribution"]
        assert dist["under_50_lines"] == 1
        assert dist["50_to_100_lines"] == 1
        assert dist["100_to_200_lines"] == 1
        assert dist["over_200_lines"] == 1
        assert dist["under_50_percentage"] == 25.0
        assert dist["50_to_100_percentage"] == 25.0
        assert dist["100_to_200_percentage"] == 25.0
        assert dist["over_200_percentage"] == 25.0

    def test_read_size_distribution_edge_cases(self):
        """Verify bucket boundaries are handled correctly."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "lines_read": 49, "turn_index": 0},  # <50
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 50, "turn_index": 1},  # 50-100
            {"tool_name": "Read", "file_path": "file3.py", "lines_read": 99, "turn_index": 2},  # 50-100
            {"tool_name": "Read", "file_path": "file4.py", "lines_read": 100, "turn_index": 3},  # 100-200
            {"tool_name": "Read", "file_path": "file5.py", "lines_read": 199, "turn_index": 4},  # 100-200
            {"tool_name": "Read", "file_path": "file6.py", "lines_read": 200, "turn_index": 5},  # 200+
        ])

        dist = result["read_size_distribution"]
        assert dist["under_50_lines"] == 1  # 49
        assert dist["50_to_100_lines"] == 2  # 50, 99
        assert dist["100_to_200_lines"] == 2  # 100, 199
        assert dist["over_200_lines"] == 1  # 200

    def test_optimized_session_pattern_run1_benchmark(self):
        """Verify Run #1 optimized pattern: 87% targeted, 64 lines avg."""
        # Simulating optimized session with mostly targeted reads
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "offset": -30, "limit": 30, "lines_read": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "limit": 50, "lines_read": 50, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "offset": 100, "limit": 20, "lines_read": 20, "turn_index": 3},
            {"tool_name": "Read", "file_path": "file5.py", "limit": 40, "lines_read": 40, "turn_index": 4},
            {"tool_name": "Read", "file_path": "file6.py", "limit": 60, "lines_read": 60, "turn_index": 5},
            {"tool_name": "Read", "file_path": "file7.py", "limit": 80, "lines_read": 80, "turn_index": 6},
            {"tool_name": "Read", "file_path": "file8.py", "limit": 100, "lines_read": 100, "turn_index": 7},
            {"tool_name": "Read", "file_path": "file9.py", "lines_read": 150, "turn_index": 8},  # 1 full read
        ])

        # 8 targeted / 9 total = 88.89%
        # Avg: (30+30+50+20+40+60+80+100+150) / 9 = 62.22
        assert result["targeted_read_percentage"] >= 85.0
        assert result["avg_lines_per_read"] < 70.0
        assert result["reads_with_negative_offset"] == 1

    def test_baseline_session_pattern(self):
        """Verify baseline pattern: mostly full reads, high avg lines."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "lines_read": 250, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 300, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "lines_read": 200, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "limit": 50, "lines_read": 50, "turn_index": 3},  # 1 targeted
            {"tool_name": "Read", "file_path": "file5.py", "lines_read": 280, "turn_index": 4},
        ])

        # 1 targeted / 5 total = 20%
        # Avg: (250+300+200+50+280) / 5 = 216
        assert result["targeted_read_percentage"] == 20.0
        assert result["avg_lines_per_read"] > 200.0
        assert result["full_read_count"] == 4

    def test_offset_without_limit_edge_case(self):
        """Verify offset without limit is still counted as targeted."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "offset": 100, "lines_read": 150, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["reads_with_offset"] == 1
        assert result["reads_with_limit"] == 0
        assert result["reads_with_both"] == 0

    def test_limit_without_offset_edge_case(self):
        """Verify limit without offset is counted as targeted."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 50, "lines_read": 50, "turn_index": 0}
        ])

        assert result["targeted_read_count"] == 1
        assert result["reads_with_offset"] == 0
        assert result["reads_with_limit"] == 1
        assert result["reads_with_both"] == 0

    def test_lines_read_inferred_from_limit(self):
        """Verify lines_read can be inferred from limit when not provided."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "turn_index": 0}
        ])

        # Should infer lines_read=30 from limit
        assert result["avg_lines_per_read"] == 30.0

    def test_read_without_lines_read_or_limit_skipped(self):
        """Verify reads without lines_read or limit don't affect averages."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 50, "lines_read": 50, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 1},  # No lines_read or limit
            {"tool_name": "Read", "file_path": "file3.py", "limit": 30, "lines_read": 30, "turn_index": 2},
        ])

        # Average should be (50 + 30) / 2 = 40, ignoring the middle read
        assert result["read_call_count"] == 3
        assert result["avg_lines_per_read"] == 40.0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "lines_read": 100, "turn_index": 2},
            {"tool_name": "Edit", "file_path": "file4.py", "turn_index": 3},
            {"tool_name": "Bash", "command": "ls", "turn_index": 4},
        ])

        assert result["total_tool_calls"] == 5
        assert result["read_call_count"] == 2
        assert result["targeted_read_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "READ", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "read", "file_path": "file2.py", "lines_read": 100, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "offset": 50, "lines_read": 50, "turn_index": 2},
        ])

        assert result["read_call_count"] == 3
        assert result["targeted_read_count"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_read_offset_limit_usage([
            "not a dict",
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_read_offset_limit_usage([
            {"file_path": "file.py", "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 30, "lines_read": 30, "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_zero_limit_handled(self):
        """Verify zero limit is handled gracefully."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 0, "lines_read": 0, "turn_index": 0},
        ])

        assert result["read_call_count"] == 1
        # Zero limit is still a limit parameter, so it's targeted
        assert result["targeted_read_count"] == 1

    def test_negative_limit_edge_case(self):
        """Verify negative limit is handled as targeted read."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "limit": -10, "turn_index": 0},
        ])

        assert result["targeted_read_count"] == 1
        assert result["reads_with_limit"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values for lines_read are ignored."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "lines_read": True, "turn_index": 0},
        ])

        assert result["avg_lines_per_read"] == 0.0

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "  Read  ", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0},
        ])

        assert result["read_call_count"] == 1

    def test_empty_distribution_when_no_lines_read(self):
        """Verify distribution is zero when no lines_read data available."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
        ])

        dist = result["read_size_distribution"]
        assert dist["under_50_lines"] == 0
        assert dist["50_to_100_lines"] == 0
        assert dist["100_to_200_lines"] == 0
        assert dist["over_200_lines"] == 0
        assert dist["under_50_percentage"] == 0.0

    def test_all_reads_under_50_lines(self):
        """Verify highly optimized session with all small reads."""
        result = analyze_session_read_offset_limit_usage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 20, "lines_read": 20, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 30, "lines_read": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "limit": 40, "lines_read": 40, "turn_index": 2},
        ])

        dist = result["read_size_distribution"]
        assert dist["under_50_lines"] == 3
        assert dist["under_50_percentage"] == 100.0
        assert result["avg_lines_per_read"] == 30.0
