"""Tests for session parallel tool efficiency analyzer."""

import pytest

from synthesis.session_parallel_tool_efficiency import (
    analyze_session_parallel_tool_efficiency,
    _percentage,
    _average,
)


class TestAnalyzeSessionParallelToolEfficiency:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_parallel_tool_efficiency([])

        assert result["total_messages"] == 0
        assert result["messages_with_tools"] == 0
        assert result["messages_with_parallel_calls"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["total_parallel_groups"] == 0
        assert result["avg_parallel_group_size"] == 0.0
        assert result["max_parallel_group_size"] == 0
        assert result["common_parallel_patterns"] == []
        assert result["missed_opportunities"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_efficiency(None)
        assert result["total_messages"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_efficiency("not a list")

    def test_message_with_no_tool_calls(self):
        """Verify message without tool calls is counted but not analyzed."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 1, "tool_calls": []}
        ])

        assert result["total_messages"] == 1
        assert result["messages_with_tools"] == 0

    def test_single_tool_call_not_parallel(self):
        """Verify single tool call is not counted as parallel."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0}
                ]
            }
        ])

        assert result["messages_with_tools"] == 1
        assert result["messages_with_parallel_calls"] == 0
        assert result["parallelization_rate"] == 0.0

    def test_two_parallel_tool_calls(self):
        """Verify two parallel tool calls are detected."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        assert result["messages_with_parallel_calls"] == 1
        assert result["parallelization_rate"] == 100.0
        assert result["total_parallel_groups"] == 1
        assert result["avg_parallel_group_size"] == 2.0
        assert result["max_parallel_group_size"] == 2

    def test_large_parallel_group(self):
        """Verify large parallel groups are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                    {"tool_name": "Read", "call_index": 3},
                    {"tool_name": "Read", "call_index": 4},
                ]
            }
        ])

        assert result["avg_parallel_group_size"] == 5.0
        assert result["max_parallel_group_size"] == 5

    def test_multiple_messages_with_varying_parallelization(self):
        """Verify multiple messages with varying parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            },
            {
                "message_index": 3,
                "tool_calls": [
                    {"tool_name": "Bash", "call_index": 0},
                ]
            },
        ])

        assert result["total_messages"] == 3
        assert result["messages_with_tools"] == 3
        assert result["messages_with_parallel_calls"] == 1
        assert result["parallelization_rate"] == 33.33

    def test_common_parallel_patterns_detected(self):
        """Verify common parallel patterns are detected."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) > 0
        # Pattern should be sorted ["Read", "Read"]
        assert patterns[0]["tools"] == ["Read", "Read"]
        assert patterns[0]["count"] == 2

    def test_different_tool_patterns(self):
        """Verify different tool combinations create different patterns."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Bash", "call_index": 0},
                    {"tool_name": "Grep", "call_index": 1},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) == 2

    def test_patterns_limited_to_top_10(self):
        """Verify common patterns limited to 10."""
        # Create 15 different parallel patterns
        messages = []
        for i in range(15):
            messages.append({
                "message_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}A", "call_index": 0},
                    {"tool_name": f"Tool{i}B", "call_index": 1},
                ]
            })

        result = analyze_session_parallel_tool_efficiency(messages)
        assert len(result["common_parallel_patterns"]) == 10

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            "not a dict",
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            },
        ])

        assert result["total_messages"] == 1

    def test_message_with_invalid_tool_calls_field(self):
        """Verify message with invalid tool_calls field is skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 1, "tool_calls": "not a list"},
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                ]
            },
        ])

        assert result["messages_with_tools"] == 1

    def test_tool_call_without_tool_name_skipped(self):
        """Verify tool calls without tool_name are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"call_index": 0},  # Missing tool_name
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        # Only one valid tool call, so not parallel
        assert result["messages_with_parallel_calls"] == 0

    def test_empty_tool_name_skipped(self):
        """Verify empty tool names are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        # Only one valid tool call
        assert result["messages_with_parallel_calls"] == 0

    def test_whitespace_tool_name_stripped(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "  Read  ", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            }
        ])

        assert result["messages_with_parallel_calls"] == 1
        pattern = result["common_parallel_patterns"][0]
        assert "Read" in pattern["tools"]


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_simple_percentage(self):
        """Verify simple percentage calculation."""
        assert _percentage(1, 4) == 25.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestAverage:
    """Test average calculation helper."""

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0.0."""
        assert _average(10.0, 0) == 0.0

    def test_negative_count_returns_zero(self):
        """Verify negative count returns 0.0."""
        assert _average(10.0, -5) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(10.0, 4) == 2.5

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average(10.0, 3) == 3.33


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_highly_parallel_session(self):
        """Simulate session with frequent parallel calls."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Edit", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            },
        ])

        assert result["parallelization_rate"] == 100.0
        assert result["avg_parallel_group_size"] == 2.5
        assert result["max_parallel_group_size"] == 3

    def test_single_threaded_session(self):
        """Simulate session with no parallel calls."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 1, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"message_index": 2, "tool_calls": [{"tool_name": "Edit", "call_index": 0}]},
            {"message_index": 3, "tool_calls": [{"tool_name": "Write", "call_index": 0}]},
        ])

        assert result["parallelization_rate"] == 0.0
        assert result["total_parallel_groups"] == 0

    def test_mixed_parallelization_session(self):
        """Simulate session with mix of parallel and single calls."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 1, "tool_calls": [{"tool_name": "Grep", "call_index": 0}]},
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {"message_index": 3, "tool_calls": [{"tool_name": "Edit", "call_index": 0}]},
            {
                "message_index": 4,
                "tool_calls": [
                    {"tool_name": "Bash", "call_index": 0},
                    {"tool_name": "Bash", "call_index": 1},
                ]
            },
        ])

        assert result["messages_with_tools"] == 4
        assert result["messages_with_parallel_calls"] == 2
        assert result["parallelization_rate"] == 50.0

    def test_common_pattern_multiple_reads(self):
        """Simulate common pattern of parallel file reads."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        # Should detect 3x Read pattern occurring twice
        assert len(patterns) > 0
        top_pattern = patterns[0]
        assert top_pattern["count"] == 2
        assert len(top_pattern["tools"]) == 3
