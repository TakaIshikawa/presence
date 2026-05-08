"""Tests for session glob tool usage pattern analyzer."""

import pytest

from synthesis.session_glob_tool_usage import analyze_session_glob_tool_usage


class TestAnalyzeSessionGlobToolUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_glob_tool_usage([])

        assert result["total_tool_calls"] == 0
        assert result["glob_call_count"] == 0
        assert result["read_call_count"] == 0
        assert result["directory_read_count"] == 0
        assert result["glob_usage_rate"] == 0.0
        assert result["avg_pattern_specificity"] == 0.0
        assert result["avg_wildcards_per_pattern"] == 0.0
        assert result["avg_path_depth"] == 0.0
        assert result["common_patterns"] == []
        assert result["pattern_reuse_rate"] == 0.0
        assert result["false_negative_count"] == 0
        assert result["missed_glob_opportunities"] == 0
        assert result["overly_broad_pattern_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_glob_tool_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_glob_tool_usage("not a list")

    def test_single_glob_call_tracked(self):
        """Verify single glob call is tracked correctly."""
        result = analyze_session_glob_tool_usage([
            {
                "tool_name": "Glob",
                "pattern": "src/**/*.py",
                "turn_index": 0,
            }
        ])

        assert result["glob_call_count"] == 1
        assert result["total_tool_calls"] == 1
        assert len(result["common_patterns"]) == 1
        assert result["common_patterns"][0]["pattern"] == "src/**/*.py"

    def test_glob_usage_rate_calculation(self):
        """Verify glob usage rate is calculated correctly."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "tests/", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/", "turn_index": 2},
        ])

        # 1 glob, 2 directory reads = 1/3 = 33.33%
        assert result["glob_call_count"] == 1
        assert result["directory_read_count"] == 2
        assert result["glob_usage_rate"] == 33.33

    def test_pattern_specificity_high_for_concrete_path(self):
        """Verify specific patterns have high specificity scores."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/synthesis/*.py", "turn_index": 0}
        ])

        # Concrete path + extension = high specificity
        assert result["avg_pattern_specificity"] > 70.0

    def test_pattern_specificity_low_for_broad_pattern(self):
        """Verify broad patterns have low specificity scores."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "**/*", "turn_index": 0}
        ])

        # Double wildcard + no extension = low specificity
        assert result["avg_pattern_specificity"] < 30.0

    def test_wildcard_counting(self):
        """Verify wildcards are counted correctly."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/**/*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "tests/*.js", "turn_index": 1},
        ])

        # First: 3 wildcards (**, *), Second: 1 wildcard (*) = avg 2.0
        assert result["avg_wildcards_per_pattern"] == 2.0

    def test_path_depth_calculation(self):
        """Verify path depth is calculated correctly."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/synthesis/*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "*.js", "turn_index": 1},
        ])

        # First: 2 slashes, Second: 0 slashes = avg 1.0
        assert result["avg_path_depth"] == 1.0

    def test_pattern_reuse_detection(self):
        """Verify pattern reuse is tracked correctly."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 1},
            {"tool_name": "Glob", "pattern": "*.js", "turn_index": 2},
        ])

        # 1 pattern reused (*.py), 2 total unique patterns = 50%
        assert result["pattern_reuse_rate"] == 50.0

    def test_no_pattern_reuse(self):
        """Verify sessions without pattern reuse."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "*.js", "turn_index": 1},
        ])

        assert result["pattern_reuse_rate"] == 0.0

    def test_common_patterns_sorted_by_frequency(self):
        """Verify common patterns are sorted by usage count."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 1},
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 2},
            {"tool_name": "Glob", "pattern": "*.js", "turn_index": 3},
        ])

        assert len(result["common_patterns"]) == 2
        assert result["common_patterns"][0]["pattern"] == "*.py"
        assert result["common_patterns"][0]["count"] == 3
        assert result["common_patterns"][1]["pattern"] == "*.js"
        assert result["common_patterns"][1]["count"] == 1

    def test_common_patterns_limited_to_five(self):
        """Verify common patterns list is capped at 5."""
        records = [
            {"tool_name": "Glob", "pattern": f"*.ext{i}", "turn_index": i}
            for i in range(10)
        ]

        result = analyze_session_glob_tool_usage(records)

        assert len(result["common_patterns"]) == 5

    def test_false_negative_detection(self):
        """Verify false negatives are detected (read after glob)."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/**/*.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/main.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/utils.py", "turn_index": 2},
        ])

        # Both reads are in src/ which was already globbed
        assert result["false_negative_count"] == 2

    def test_missed_glob_opportunity_detected(self):
        """Verify missed glob opportunities are detected (3+ sequential reads)."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/file3.py", "turn_index": 2},
        ])

        # 3 sequential reads in same directory = missed opportunity
        assert result["missed_glob_opportunities"] == 1

    def test_missed_glob_opportunity_not_detected_for_different_dirs(self):
        """Verify no false positive for reads in different directories."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "tests/file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "lib/file3.py", "turn_index": 2},
        ])

        # Different directories = no missed opportunity
        assert result["missed_glob_opportunities"] == 0

    def test_directory_read_detection_by_trailing_slash(self):
        """Verify directory reads detected by trailing slash."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/", "turn_index": 0},
            {"tool_name": "Read", "file_path": "tests/", "turn_index": 1},
        ])

        assert result["directory_read_count"] == 2

    def test_directory_read_detection_by_common_names(self):
        """Verify directory reads detected by common directory names."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src", "turn_index": 0},
            {"tool_name": "Read", "file_path": "tests", "turn_index": 1},
            {"tool_name": "Read", "file_path": "docs", "turn_index": 2},
        ])

        assert result["directory_read_count"] == 3

    def test_directory_read_detection_by_flag(self):
        """Verify directory reads detected by explicit flag."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "mydir", "is_directory": True, "turn_index": 0},
        ])

        assert result["directory_read_count"] == 1

    def test_file_read_not_counted_as_directory(self):
        """Verify regular file reads are not counted as directory reads."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "README.md", "turn_index": 1},
        ])

        assert result["directory_read_count"] == 0
        assert result["read_call_count"] == 2

    def test_overly_broad_patterns_detected(self):
        """Verify overly broad patterns are flagged."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "**/*", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "**", "turn_index": 1},
            {"tool_name": "Glob", "pattern": "src/**/*.py", "turn_index": 2},
        ])

        # First two are overly broad (< 30), third is specific
        assert result["overly_broad_pattern_count"] == 2
        assert len(result["overly_broad_examples"]) == 2

    def test_overly_broad_examples_limited(self):
        """Verify overly broad examples are limited to 3."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "**/*", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "**", "turn_index": 1},
            {"tool_name": "Glob", "pattern": "**/.*", "turn_index": 2},
            {"tool_name": "Glob", "pattern": "**/**", "turn_index": 3},
            {"tool_name": "Glob", "pattern": "***", "turn_index": 4},
        ])

        assert result["overly_broad_pattern_count"] >= 3
        assert len(result["overly_broad_examples"]) <= 3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_glob_tool_usage([
            "not a dict",
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["glob_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_glob_tool_usage([
            {"pattern": "*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "*.js", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["glob_call_count"] == 1

    def test_glob_without_pattern_handled(self):
        """Verify glob calls without pattern are handled gracefully."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "turn_index": 0},
        ])

        assert result["glob_call_count"] == 1
        assert result["avg_pattern_specificity"] == 0.0
        assert result["common_patterns"] == []

    def test_optimal_glob_usage_pattern(self):
        """Verify optimal usage pattern has high metrics."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/**/*.py", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "tests/**/*.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/main.py", "turn_index": 2},
        ])

        # High glob usage rate, good specificity
        assert result["glob_usage_rate"] > 50.0
        assert result["avg_pattern_specificity"] >= 50.0
        assert result["missed_glob_opportunities"] == 0

    def test_anti_pattern_excessive_reads(self):
        """Verify anti-pattern of excessive reads without glob."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/file3.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "src/file4.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "src/file5.py", "turn_index": 4},
        ])

        # No glob usage, missed opportunity
        assert result["glob_call_count"] == 0
        assert result["glob_usage_rate"] == 0.0
        assert result["missed_glob_opportunities"] >= 1

    def test_anti_pattern_overly_broad_globs(self):
        """Verify anti-pattern of overly broad glob patterns."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "**/*", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "**", "turn_index": 1},
        ])

        # Low specificity, multiple overly broad patterns
        assert result["avg_pattern_specificity"] < 25.0
        assert result["overly_broad_pattern_count"] == 2

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "README.md", "turn_index": 2},
            {"tool_name": "Bash", "command": "ls", "turn_index": 3},
        ])

        assert result["total_tool_calls"] == 4
        assert result["glob_call_count"] == 1
        assert result["read_call_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "GLOB", "pattern": "*.py", "turn_index": 0},
            {"tool_name": "glob", "pattern": "*.js", "turn_index": 1},
            {"tool_name": "READ", "file_path": "main.py", "turn_index": 2},
        ])

        assert result["glob_call_count"] == 2
        assert result["read_call_count"] == 1

    def test_whitespace_handling_in_patterns(self):
        """Verify whitespace in patterns is stripped."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "  *.py  ", "turn_index": 0},
        ])

        assert result["common_patterns"][0]["pattern"] == "*.py"

    def test_empty_pattern_handled(self):
        """Verify empty patterns are handled gracefully."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "", "turn_index": 0},
            {"tool_name": "Glob", "pattern": "   ", "turn_index": 1},
        ])

        assert result["glob_call_count"] == 2
        assert result["avg_pattern_specificity"] == 0.0

    def test_pattern_with_multiple_extensions(self):
        """Verify patterns with multiple extensions."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/**/*.{py,js}", "turn_index": 0},
        ])

        # Should still detect extension and have good specificity
        assert result["avg_pattern_specificity"] > 50.0

    def test_question_mark_wildcard_counted(self):
        """Verify question mark wildcards are counted."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "file?.py", "turn_index": 0},
        ])

        # 1 question mark wildcard
        assert result["avg_wildcards_per_pattern"] == 1.0

    def test_glob_resets_sequential_read_tracking(self):
        """Verify glob resets sequential read tracking."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/file2.py", "turn_index": 1},
            {"tool_name": "Glob", "pattern": "*.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "src/file3.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "src/file4.py", "turn_index": 4},
        ])

        # Glob breaks the sequence, so no missed opportunity
        assert result["missed_glob_opportunities"] == 0

    def test_exactly_three_sequential_reads_triggers_detection(self):
        """Verify exactly 3 sequential reads triggers missed opportunity."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/b.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/c.py", "turn_index": 2},
        ])

        assert result["missed_glob_opportunities"] == 1

    def test_six_sequential_reads_triggers_twice(self):
        """Verify 6 sequential reads in same dir triggers detection twice."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Read", "file_path": "src/a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/b.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "src/c.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "src/d.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "src/e.py", "turn_index": 4},
            {"tool_name": "Read", "file_path": "src/f.py", "turn_index": 5},
        ])

        # First 3 trigger once, reset, next 3 trigger again
        assert result["missed_glob_opportunities"] == 2

    def test_zero_denominator_in_percentages(self):
        """Verify zero denominator in percentage calculations."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 0},
        ])

        # No glob or directory reads
        assert result["glob_usage_rate"] == 0.0
        assert result["pattern_reuse_rate"] == 0.0

    def test_specificity_normalized_to_range(self):
        """Verify specificity scores are normalized to 0-100 range."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "a/b/c/d/e/f/g/h/i.txt", "turn_index": 0},
        ])

        # Very specific pattern, but should be capped at 100
        assert 0.0 <= result["avg_pattern_specificity"] <= 100.0

    def test_base_path_extraction(self):
        """Verify base path extraction for false negative detection."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/synthesis/**/*.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/synthesis/analyzer.py", "turn_index": 1},
        ])

        # Read is in src/synthesis which was globbed
        assert result["false_negative_count"] == 1

    def test_no_false_negative_for_different_base_path(self):
        """Verify no false negative when base paths differ."""
        result = analyze_session_glob_tool_usage([
            {"tool_name": "Glob", "pattern": "src/**/*.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "tests/test_main.py", "turn_index": 1},
        ])

        # Different base path (src vs tests)
        assert result["false_negative_count"] == 0
