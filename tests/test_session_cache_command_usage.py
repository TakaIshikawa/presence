"""Tests for session cache command usage analyzer."""

import pytest

from synthesis.session_cache_command_usage import analyze_session_cache_command_usage


class TestAnalyzeSessionCacheCommandUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_cache_command_usage([])

        assert result["total_tool_calls"] == 0
        assert result["cache_command_count"] == 0
        assert result["cache_snapshot_count"] == 0
        assert result["cache_query_count"] == 0
        assert result["cache_clear_count"] == 0
        assert result["cache_query_before_read_count"] == 0
        assert result["cache_query_before_read_percentage"] == 0.0
        assert result["cache_hit_inferred_count"] == 0
        assert result["cache_miss_inferred_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_cache_command_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_cache_command_usage("not a list")

    def test_single_cache_snapshot_command(self):
        """Verify single cache snapshot command is tracked."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 0}
        ])

        assert result["cache_command_count"] == 1
        assert result["cache_snapshot_count"] == 1
        assert result["cache_query_count"] == 0
        assert result["cache_clear_count"] == 0

    def test_single_cache_query_command(self):
        """Verify single cache query command is tracked."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 0}
        ])

        assert result["cache_command_count"] == 1
        assert result["cache_snapshot_count"] == 0
        assert result["cache_query_count"] == 1
        assert result["cache_clear_count"] == 0

    def test_single_cache_clear_command(self):
        """Verify single cache clear command is tracked."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "clear", "turn_index": 0}
        ])

        assert result["cache_command_count"] == 1
        assert result["cache_snapshot_count"] == 0
        assert result["cache_query_count"] == 0
        assert result["cache_clear_count"] == 1

    def test_mixed_cache_commands(self):
        """Verify mix of cache command types."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 0},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file1.py", "turn_index": 1},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file2.py", "turn_index": 2},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "clear", "turn_index": 3},
        ])

        assert result["cache_command_count"] == 4
        assert result["cache_snapshot_count"] == 1
        assert result["cache_query_count"] == 2
        assert result["cache_clear_count"] == 1

    def test_command_type_distribution_percentages(self):
        """Verify command type distribution percentages."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 0},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 1},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file2.py", "turn_index": 2},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file3.py", "turn_index": 3},
        ])

        dist = result["command_type_distribution"]
        # 1 snapshot / 4 total = 25%
        # 3 query / 4 total = 75%
        assert dist["snapshot_percentage"] == 25.0
        assert dist["query_percentage"] == 75.0
        assert dist["clear_percentage"] == 0.0

    def test_cache_query_before_read_pattern_detected(self):
        """Verify cache-query-before-read pattern is detected."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
        ])

        assert result["cache_query_before_read_count"] == 1
        assert result["cache_query_before_read_percentage"] == 100.0

    def test_cache_query_before_read_within_two_tool_calls(self):
        """Verify pattern detected within 2 tool calls."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "other.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 2},
        ])

        # turn_index 2 - turn_index 0 = 2, which is <= 2
        assert result["cache_query_before_read_count"] == 1

    def test_cache_query_before_read_beyond_window_not_detected(self):
        """Verify pattern not detected beyond 2 tool calls."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file1.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 3},
        ])

        # turn_index 3 - turn_index 0 = 3, which is > 2
        assert result["cache_query_before_read_count"] == 0

    def test_cache_query_different_file_not_matched(self):
        """Verify cache query for different file doesn't match."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 1},
        ])

        assert result["cache_query_before_read_count"] == 0

    def test_multiple_reads_with_cache_queries(self):
        """Verify multiple reads with cache queries."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 1},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file2.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "file3.py", "turn_index": 4},  # No cache query
        ])

        # 2 reads with cache query before them, out of 3 total reads
        assert result["cache_query_before_read_count"] == 2
        assert result["cache_query_before_read_percentage"] == 66.67

    def test_cache_hit_inferred_from_tool_result(self):
        """Verify cache hit inference from tool result."""
        result = analyze_session_cache_command_usage([
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file.py",
                "tool_result": "Found cached summary for file.py",
                "turn_index": 0
            }
        ])

        assert result["cache_hit_inferred_count"] == 1
        assert result["cache_miss_inferred_count"] == 0

    def test_cache_miss_inferred_from_tool_result(self):
        """Verify cache miss inference from tool result."""
        result = analyze_session_cache_command_usage([
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file.py",
                "tool_result": "Cache miss: file not cached",
                "turn_index": 0
            }
        ])

        assert result["cache_hit_inferred_count"] == 0
        assert result["cache_miss_inferred_count"] == 1

    def test_cache_hit_various_messages(self):
        """Verify various cache hit message formats."""
        result = analyze_session_cache_command_usage([
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file1.py",
                "tool_result": "Using cached data",
                "turn_index": 0
            },
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file2.py",
                "tool_result": "Cache hit for file2.py",
                "turn_index": 1
            },
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file3.py",
                "tool_result": "Data found in cache",
                "turn_index": 2
            },
        ])

        assert result["cache_hit_inferred_count"] == 3

    def test_cache_miss_various_messages(self):
        """Verify various cache miss message formats."""
        result = analyze_session_cache_command_usage([
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file1.py",
                "tool_result": "Not cached",
                "turn_index": 0
            },
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file2.py",
                "tool_result": "File not in cache",
                "turn_index": 1
            },
            {
                "tool_name": "Skill",
                "skill": "cache",
                "skill_args": "query file3.py",
                "tool_result": "Cache empty",
                "turn_index": 2
            },
        ])

        assert result["cache_miss_inferred_count"] == 3

    def test_session_without_cache_usage(self):
        """Verify session without any cache usage."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Write", "file_path": "file3.py", "turn_index": 2},
        ])

        assert result["cache_command_count"] == 0
        assert result["cache_query_before_read_count"] == 0

    def test_effective_cache_usage_pattern(self):
        """Verify effective cache usage with high query-before-read rate."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "file1.py", "turn_index": 2},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 3},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file2.py", "turn_index": 4},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 5},
        ])

        # 2 reads with cache query, 100% coverage
        assert result["cache_query_before_read_percentage"] == 100.0
        assert result["cache_snapshot_count"] == 1

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Skill", "skill": "other", "skill_args": "args", "turn_index": 3},
        ])

        assert result["total_tool_calls"] == 4
        assert result["cache_command_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool and skill name matching is case-insensitive."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "SKILL", "skill": "CACHE", "skill_args": "SNAPSHOT", "turn_index": 0},
            {"tool_name": "skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 1},
        ])

        assert result["cache_command_count"] == 2
        assert result["cache_snapshot_count"] == 1
        assert result["cache_query_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_cache_command_usage([
            "not a dict",
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["cache_command_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_cache_command_usage([
            {"skill": "cache", "skill_args": "snapshot", "turn_index": 0},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query file.py", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["cache_command_count"] == 1

    def test_skill_without_skill_args_handled(self):
        """Verify Skill without skill_args is handled gracefully."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "turn_index": 0},
        ])

        # Should count as cache command but not categorized
        assert result["cache_command_count"] == 1
        assert result["cache_snapshot_count"] == 0
        assert result["cache_query_count"] == 0

    def test_non_cache_skill_ignored(self):
        """Verify non-cache skills are ignored."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "verify", "skill_args": "check", "turn_index": 0},
            {"tool_name": "Skill", "skill": "cache", "skill_args": "snapshot", "turn_index": 1},
        ])

        assert result["cache_command_count"] == 1

    def test_whitespace_handling(self):
        """Verify whitespace in fields is stripped."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "  Skill  ", "skill": "  cache  ", "skill_args": "  snapshot  ", "turn_index": 0},
        ])

        assert result["cache_command_count"] == 1
        assert result["cache_snapshot_count"] == 1

    def test_file_path_extraction_from_query(self):
        """Verify file path extraction from cache query."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query src/main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "src/main.py", "turn_index": 1},
        ])

        assert result["cache_query_before_read_count"] == 1

    def test_query_with_extra_whitespace(self):
        """Verify query parsing handles extra whitespace."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Skill", "skill": "cache", "skill_args": "query   file.py  ", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
        ])

        assert result["cache_query_before_read_count"] == 1

    def test_zero_cache_commands_percentage(self):
        """Verify zero denominator in percentage calculation."""
        result = analyze_session_cache_command_usage([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
        ])

        dist = result["command_type_distribution"]
        assert dist["snapshot_percentage"] == 0.0
        assert dist["query_percentage"] == 0.0
        assert dist["clear_percentage"] == 0.0
