"""Tests for session tool execution parallelization analyzer."""

import pytest

from synthesis.session_tool_execution_parallelization import (
    analyze_session_tool_execution_parallelization,
    _percentage,
    _average,
    _calculate_efficiency_score,
    _format_common_patterns,
)


class TestAnalyzeSessionToolExecutionParallelization:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_tool_execution_parallelization([])

        assert result["total_messages"] == 0
        assert result["messages_with_tools"] == 0
        assert result["total_tool_calls"] == 0
        assert result["multi_tool_messages"] == 0
        assert result["single_tool_messages"] == 0
        assert result["avg_tools_per_message"] == 0.0
        assert result["max_tools_per_message"] == 0
        assert result["parallel_tool_calls"] == 0
        assert result["sequential_tool_calls"] == 0
        assert result["parallelization_ratio"] == 0.0
        assert result["missed_opportunities"] == 0
        assert result["parallelizable_sequential_calls"] == 0
        assert result["parallel_efficiency_score"] == 0.0
        assert result["common_parallel_patterns"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_execution_parallelization(None)
        assert result["total_messages"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_execution_parallelization("not a list")

    def test_message_with_no_tool_calls(self):
        """Verify message without tool calls is counted but not analyzed."""
        result = analyze_session_tool_execution_parallelization([
            {"message_index": 0, "tool_calls": []}
        ])

        assert result["total_messages"] == 1
        assert result["messages_with_tools"] == 0
        assert result["total_tool_calls"] == 0

    def test_single_tool_call_counted_as_sequential(self):
        """Verify single tool call is counted as sequential."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            }
        ])

        assert result["messages_with_tools"] == 1
        assert result["total_tool_calls"] == 1
        assert result["single_tool_messages"] == 1
        assert result["multi_tool_messages"] == 0
        assert result["sequential_tool_calls"] == 1
        assert result["parallel_tool_calls"] == 0
        assert result["parallelization_ratio"] == 0.0

    def test_two_parallel_tool_calls(self):
        """Verify two parallel tool calls are detected."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            }
        ])

        assert result["multi_tool_messages"] == 1
        assert result["single_tool_messages"] == 0
        assert result["parallel_tool_calls"] == 2
        assert result["sequential_tool_calls"] == 0
        assert result["parallelization_ratio"] == 100.0
        assert result["avg_tools_per_message"] == 2.0
        assert result["max_tools_per_message"] == 2

    def test_large_parallel_batch(self):
        """Verify large parallel batches are tracked."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Grep", "pattern": "error"},
                    {"tool_name": "Glob", "pattern": "*.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            }
        ])

        assert result["parallel_tool_calls"] == 5
        assert result["max_tools_per_message"] == 5
        assert result["avg_tools_per_message"] == 5.0

    def test_mixed_parallel_and_sequential_messages(self):
        """Verify mixed parallel and sequential messages."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "test"},
                ]
            },
        ])

        assert result["total_tool_calls"] == 4
        assert result["parallel_tool_calls"] == 2
        assert result["sequential_tool_calls"] == 2
        assert result["parallelization_ratio"] == 50.0
        assert result["single_tool_messages"] == 2
        assert result["multi_tool_messages"] == 1

    def test_parallelization_ratio_calculation(self):
        """Verify parallelization ratio calculation."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "error"},
                ]
            },
        ])

        # 3 parallel + 1 sequential = 4 total, 75% parallel
        assert result["parallelization_ratio"] == 75.0

    def test_avg_tools_per_message_calculation(self):
        """Verify average tools per message calculation."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "c.py"},
                    {"tool_name": "Read", "file_path": "d.py"},
                    {"tool_name": "Read", "file_path": "e.py"},
                    {"tool_name": "Read", "file_path": "f.py"},
                ]
            },
        ])

        # (2 + 4) / 2 = 3.0
        assert result["avg_tools_per_message"] == 3.0

    def test_avg_tools_excludes_single_tool_messages(self):
        """Verify average tools calculation excludes single-tool messages."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            },
        ])

        # Average is 2.0 (only counts the multi-tool message)
        assert result["avg_tools_per_message"] == 2.0

    def test_missed_opportunities_detected(self):
        """Verify missed parallelization opportunities are detected."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "error"},
                ]
            },
            {
                "message_index": 3,
                "tool_calls": [
                    {"tool_name": "Glob", "pattern": "*.py"},
                ]
            },
        ])

        # 4 parallelizable sequential calls = 2 missed opportunities
        assert result["parallelizable_sequential_calls"] == 4
        assert result["missed_opportunities"] == 2

    def test_no_missed_opportunities_for_sequential_tools(self):
        """Verify sequential tools (Edit, Write) are not counted as missed opportunities."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "a.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Write", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Bash", "command": "pytest"},
                ]
            },
        ])

        # Edit, Write, Bash are not parallelizable, so no missed opportunities
        assert result["parallelizable_sequential_calls"] == 0
        assert result["missed_opportunities"] == 0

    def test_common_parallel_patterns_tracked(self):
        """Verify common parallel patterns are tracked."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "c.py"},
                    {"tool_name": "Grep", "pattern": "error"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "d.py"},
                    {"tool_name": "Read", "file_path": "e.py"},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) == 2

        # Find the read-read pattern
        read_read = [p for p in patterns if p["tools"] == ["read", "read"]][0]
        assert read_read["count"] == 2

        # Find the grep-read pattern
        grep_read = [p for p in patterns if sorted(p["tools"]) == ["grep", "read"]][0]
        assert grep_read["count"] == 1

    def test_common_patterns_sorted_by_frequency(self):
        """Verify common patterns are sorted by frequency."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "c.py"},
                    {"tool_name": "Read", "file_path": "d.py"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "error"},
                    {"tool_name": "Glob", "pattern": "*.py"},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        # Most frequent pattern should be first
        assert patterns[0]["count"] >= patterns[-1]["count"]

    def test_common_patterns_limited_to_top_five(self):
        """Verify common patterns limited to top 5."""
        messages = []
        for i in range(10):
            messages.append({
                "message_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}", "file_path": "a.py"},
                    {"tool_name": f"Tool{i}", "file_path": "b.py"},
                ]
            })

        result = analyze_session_tool_execution_parallelization(messages)
        assert len(result["common_parallel_patterns"]) <= 5

    def test_efficiency_score_for_optimal_parallelization(self):
        """Verify efficiency score for optimal parallelization."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "error"},
                    {"tool_name": "Glob", "pattern": "*.py"},
                    {"tool_name": "Read", "file_path": "d.py"},
                ]
            },
        ])

        # 100% parallel, avg batch 3.0, no missed opportunities
        # Should have high efficiency score
        assert result["parallel_efficiency_score"] > 90.0

    def test_efficiency_score_for_poor_parallelization(self):
        """Verify efficiency score for poor parallelization."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            },
        ])

        # 0% parallel, high missed opportunities
        # Should have low efficiency score
        assert result["parallel_efficiency_score"] < 20.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_execution_parallelization([
            "not a dict",
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            },
        ])

        assert result["total_messages"] == 1
        assert result["total_tool_calls"] == 1

    def test_record_with_non_list_tool_calls_skipped(self):
        """Verify records with non-list tool_calls are skipped."""
        result = analyze_session_tool_execution_parallelization([
            {"message_index": 0, "tool_calls": "not a list"},
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            },
        ])

        assert result["total_messages"] == 2
        assert result["messages_with_tools"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "READ", "file_path": "a.py"},
                    {"tool_name": "Grep", "pattern": "error"},
                ]
            }
        ])

        patterns = result["common_parallel_patterns"]
        # Should normalize to lowercase
        assert patterns[0]["tools"] == ["grep", "read"]

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "  Read  ", "file_path": "a.py"},
                    {"tool_name": " Grep ", "pattern": "error"},
                ]
            }
        ])

        patterns = result["common_parallel_patterns"]
        assert patterns[0]["tools"] == ["grep", "read"]

    def test_optimal_pattern_all_parallel(self):
        """Verify optimal usage pattern with all parallel calls."""
        result = analyze_session_tool_execution_parallelization([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "error"},
                    {"tool_name": "Glob", "pattern": "*.py"},
                ]
            },
        ])

        assert result["parallelization_ratio"] == 100.0
        assert result["missed_opportunities"] == 0
        assert result["parallel_efficiency_score"] >= 90.0

    def test_anti_pattern_all_sequential(self):
        """Verify anti-pattern of all sequential calls."""
        result = analyze_session_tool_execution_parallelization([
            {"message_index": 0, "tool_calls": [{"tool_name": "Read", "file_path": "a.py"}]},
            {"message_index": 1, "tool_calls": [{"tool_name": "Read", "file_path": "b.py"}]},
            {"message_index": 2, "tool_calls": [{"tool_name": "Read", "file_path": "c.py"}]},
            {"message_index": 3, "tool_calls": [{"tool_name": "Read", "file_path": "d.py"}]},
        ])

        assert result["parallelization_ratio"] == 0.0
        assert result["missed_opportunities"] > 0
        assert result["parallel_efficiency_score"] < 30.0

    def test_realistic_session_mixed_patterns(self):
        """Verify realistic session with mixed parallel and sequential."""
        result = analyze_session_tool_execution_parallelization([
            # Initial exploration - parallel reads
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                    {"tool_name": "Read", "file_path": "test.py"},
                    {"tool_name": "Grep", "pattern": "def.*test"},
                ]
            },
            # Edit (sequential, expected)
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "main.py"},
                ]
            },
            # Verification (sequential, expected)
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Bash", "command": "pytest"},
                ]
            },
            # More parallel reads
            {
                "message_index": 3,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "utils.py"},
                    {"tool_name": "Read", "file_path": "helpers.py"},
                ]
            },
        ])

        # 7 total calls: 5 parallel + 2 sequential = 71.43% parallel
        assert result["total_tool_calls"] == 7
        assert result["parallel_tool_calls"] == 5
        assert result["sequential_tool_calls"] == 2
        assert 70.0 < result["parallelization_ratio"] < 75.0

    def test_empty_tool_calls_list_handled(self):
        """Verify empty tool_calls list is handled correctly."""
        result = analyze_session_tool_execution_parallelization([
            {"message_index": 0, "tool_calls": []},
            {"message_index": 1, "tool_calls": []},
        ])

        assert result["total_messages"] == 2
        assert result["messages_with_tools"] == 0
        assert result["total_tool_calls"] == 0

    def test_tool_calls_missing_from_record(self):
        """Verify records without tool_calls key are handled."""
        result = analyze_session_tool_execution_parallelization([
            {"message_index": 0},
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            },
        ])

        assert result["total_messages"] == 2
        assert result["messages_with_tools"] == 1


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0

    def test_percentage_zero_denominator(self):
        """Verify percentage with zero denominator returns 0."""
        assert _percentage(10, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1, 2, 3, 4, 5]) == 3.0
        assert _average([10, 20]) == 15.0
        assert _average([7]) == 7.0

    def test_average_empty_list(self):
        """Verify average of empty list returns 0."""
        assert _average([]) == 0.0

    def test_efficiency_score_perfect(self):
        """Verify efficiency score for perfect parallelization."""
        # 100% parallel, ideal batch size 3, no missed opportunities
        score = _calculate_efficiency_score(100.0, 3.0, 0, 100)
        assert score == 100.0

    def test_efficiency_score_zero_parallelization(self):
        """Verify efficiency score for zero parallelization."""
        score = _calculate_efficiency_score(0.0, 0.0, 10, 10)
        assert score == 0.0

    def test_efficiency_score_partial_parallelization(self):
        """Verify efficiency score for partial parallelization."""
        # 50% parallel, batch size 2, some missed opportunities
        score = _calculate_efficiency_score(50.0, 2.0, 5, 20)
        # Should be moderate score
        assert 30.0 < score < 60.0

    def test_efficiency_score_large_batches_penalized(self):
        """Verify large batch sizes are slightly penalized."""
        # 100% parallel but very large batches may indicate inefficiency
        score_small = _calculate_efficiency_score(100.0, 3.0, 0, 100)
        score_large = _calculate_efficiency_score(100.0, 10.0, 0, 100)
        assert score_small > score_large

    def test_efficiency_score_missed_opportunities_penalty(self):
        """Verify missed opportunities reduce efficiency score."""
        score_no_missed = _calculate_efficiency_score(80.0, 3.0, 0, 100)
        score_with_missed = _calculate_efficiency_score(80.0, 3.0, 20, 100)
        assert score_no_missed > score_with_missed

    def test_efficiency_score_zero_total_calls(self):
        """Verify efficiency score returns 0 for zero total calls."""
        score = _calculate_efficiency_score(0.0, 0.0, 0, 0)
        assert score == 0.0

    def test_format_common_patterns_empty(self):
        """Verify formatting empty patterns returns empty list."""
        result = _format_common_patterns({})
        assert result == []

    def test_format_common_patterns_single_pattern(self):
        """Verify formatting single pattern."""
        patterns = {("read", "grep"): 5}
        result = _format_common_patterns(patterns)

        assert len(result) == 1
        assert result[0]["tools"] == ["read", "grep"]
        assert result[0]["count"] == 5

    def test_format_common_patterns_sorted_by_frequency(self):
        """Verify patterns are sorted by frequency."""
        patterns = {
            ("read", "read"): 10,
            ("grep", "glob"): 3,
            ("read", "grep"): 7,
        }
        result = _format_common_patterns(patterns)

        assert len(result) == 3
        assert result[0]["count"] == 10
        assert result[1]["count"] == 7
        assert result[2]["count"] == 3

    def test_format_common_patterns_limited_to_five(self):
        """Verify patterns are limited to top 5."""
        patterns = {
            (f"tool{i}",): i
            for i in range(10, 0, -1)
        }
        result = _format_common_patterns(patterns)

        assert len(result) == 5
        assert result[0]["count"] == 10
        assert result[4]["count"] == 6
