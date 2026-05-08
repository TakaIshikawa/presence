"""Tests for session parallel tool call efficiency analyzer."""

import pytest

from synthesis.session_parallel_tool_efficiency import (
    analyze_session_parallel_tool_efficiency,
    _average,
    _classify_efficiency_pattern,
    _format_patterns,
    _percentage,
)


class TestAnalyzeSessionParallelToolEfficiency:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_parallel_tool_efficiency([])

        assert result["total_messages"] == 0
        assert result["parallel_messages"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["total_tool_calls"] == 0
        assert result["parallel_tool_calls"] == 0
        assert result["parallel_call_rate"] == 0.0
        assert result["average_parallel_group_size"] == 0.0
        assert result["max_parallel_group_size"] == 0
        assert result["common_parallel_patterns"] == []
        assert result["missed_opportunities"] == 0
        assert result["efficiency_pattern"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_efficiency(None)
        assert result["total_messages"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_efficiency("not a list")

    def test_single_message_single_tool(self):
        """Verify single message with single tool call."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": [{"tool_name": "Read"}]},
        ])

        assert result["total_messages"] == 1
        assert result["parallel_messages"] == 0
        assert result["total_tool_calls"] == 1
        assert result["efficiency_pattern"] == "simple"

    def test_single_message_parallel_tools(self):
        """Verify single message with parallel tool calls."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                    {"tool_name": "Glob"},
                ],
            },
        ])

        assert result["total_messages"] == 1
        assert result["parallel_messages"] == 1
        assert result["total_tool_calls"] == 3
        assert result["parallel_tool_calls"] == 3
        assert result["parallelization_rate"] == 100.0
        assert result["parallel_call_rate"] == 100.0
        assert result["average_parallel_group_size"] == 3.0
        assert result["max_parallel_group_size"] == 3

    def test_multiple_messages_all_sequential(self):
        """Verify multiple messages with all sequential calls."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": [{"tool_name": "Read"}]},
            {"message_index": 1, "tool_calls": [{"tool_name": "Edit"}]},
            {"message_index": 2, "tool_calls": [{"tool_name": "Bash"}]},
            {"message_index": 3, "tool_calls": [{"tool_name": "Read"}]},
            {"message_index": 4, "tool_calls": [{"tool_name": "Write"}]},
        ])

        assert result["total_messages"] == 5
        assert result["parallel_messages"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["efficiency_pattern"] == "sequential"

    def test_mixed_parallel_and_sequential(self):
        """Verify mixed parallel and sequential messages."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": [{"tool_name": "Read"}]},
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
            {"message_index": 2, "tool_calls": [{"tool_name": "Edit"}]},
            {
                "message_index": 3,
                "tool_calls": [
                    {"tool_name": "Bash"},
                    {"tool_name": "Grep"},
                ],
            },
        ])

        assert result["total_messages"] == 4
        assert result["parallel_messages"] == 2
        assert result["parallelization_rate"] == 50.0
        assert result["total_tool_calls"] == 6
        assert result["parallel_tool_calls"] == 4
        assert result["parallel_call_rate"] == pytest.approx(66.67, abs=0.01)

    def test_parallel_pattern_tracking(self):
        """Verify parallel tool patterns are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Edit"},
                    {"tool_name": "Write"},
                ],
            },
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) == 2
        # Most common should be Read+Grep (2 occurrences)
        assert patterns[0]["tools"] == ["Grep", "Read"]  # Sorted
        assert patterns[0]["count"] == 2

    def test_missed_opportunities_detection(self):
        """Verify missed parallelization opportunities are detected."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": [{"tool_name": "Read", "are_independent": True}]},
            {"message_index": 1, "tool_calls": [{"tool_name": "Read", "are_independent": True}]},
            {"message_index": 2, "tool_calls": [{"tool_name": "Grep", "are_independent": True}]},
            {"message_index": 3, "tool_calls": [{"tool_name": "Read", "are_independent": True}]},
            {"message_index": 4, "tool_calls": [{"tool_name": "Read", "are_independent": True}]},
            {"message_index": 5, "tool_calls": [{"tool_name": "Bash", "are_independent": True}]},
        ])

        assert result["missed_opportunities"] == 6
        assert result["efficiency_pattern"] == "underutilized"

    def test_optimal_efficiency_pattern(self):
        """Verify optimal pattern classification."""
        messages = []
        for i in range(10):
            if i % 2 == 0:
                # Parallel messages
                messages.append({
                    "message_index": i,
                    "tool_calls": [
                        {"tool_name": "Read"},
                        {"tool_name": "Read"},
                        {"tool_name": "Read"},
                    ],
                })
            else:
                # Sequential
                messages.append({
                    "message_index": i,
                    "tool_calls": [{"tool_name": "Edit"}],
                })

        result = analyze_session_parallel_tool_efficiency(messages)

        # 5 parallel out of 10 = 50% parallelization rate
        # 15 parallel calls out of 20 = 75% parallel call rate
        assert result["parallelization_rate"] == 50.0
        assert result["parallel_call_rate"] == 75.0
        assert result["efficiency_pattern"] == "optimal"

    def test_effective_efficiency_pattern(self):
        """Verify effective pattern classification."""
        messages = []
        for i in range(10):
            if i < 3:
                # 3 parallel messages
                messages.append({
                    "message_index": i,
                    "tool_calls": [
                        {"tool_name": "Read"},
                        {"tool_name": "Grep"},
                    ],
                })
            else:
                # 7 sequential
                messages.append({
                    "message_index": i,
                    "tool_calls": [{"tool_name": "Edit"}],
                })

        result = analyze_session_parallel_tool_efficiency(messages)

        # 3 parallel out of 10 = 30% parallelization rate
        assert result["parallelization_rate"] == 30.0
        assert result["efficiency_pattern"] == "effective"

    def test_average_parallel_group_size(self):
        """Verify average parallel group size calculation."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
        ])

        # Average of 2 and 4 = 3.0
        assert result["average_parallel_group_size"] == 3.0
        assert result["max_parallel_group_size"] == 4

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            "not a dict",
            {"message_index": 0, "tool_calls": [{"tool_name": "Read"}]},
        ])

        assert result["total_messages"] == 1

    def test_missing_tool_calls_skipped(self):
        """Verify records without tool_calls are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0},
            {"message_index": 1, "tool_calls": [{"tool_name": "Read"}]},
        ])

        assert result["total_messages"] == 1

    def test_empty_tool_calls_skipped(self):
        """Verify records with empty tool_calls are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": []},
            {"message_index": 1, "tool_calls": [{"tool_name": "Read"}]},
        ])

        assert result["total_messages"] == 1


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_normal(self):
        """Verify percentage calculation."""
        assert _percentage(3, 10) == 30.0
        assert _percentage(1, 4) == 25.0

    def test_percentage_zero_denominator(self):
        """Verify percentage returns 0.0 for zero denominator."""
        assert _percentage(5, 0) == 0.0

    def test_percentage_rounding(self):
        """Verify percentage is rounded to 2 decimals."""
        assert _percentage(1, 3) == 33.33

    def test_average_normal(self):
        """Verify average calculation."""
        assert _average([2, 4, 6]) == 4.0

    def test_average_empty_list(self):
        """Verify average returns 0.0 for empty list."""
        assert _average([]) == 0.0

    def test_average_rounding(self):
        """Verify average is rounded to 2 decimals."""
        assert _average([1, 2, 3]) == 2.0

    def test_format_patterns_basic(self):
        """Verify pattern formatting."""
        patterns = {
            ("Read", "Grep"): 3,
            ("Edit", "Write"): 1,
            ("Bash", "Read"): 2,
        }
        result = _format_patterns(patterns)

        assert len(result) == 3
        # Should be sorted by count descending
        assert result[0]["tools"] == ["Read", "Grep"]
        assert result[0]["count"] == 3

    def test_format_patterns_limits_to_five(self):
        """Verify pattern formatting limits to top 5."""
        patterns = {
            (f"Tool{i}",): i for i in range(10)
        }
        result = _format_patterns(patterns)
        assert len(result) == 5

    def test_classify_efficiency_pattern_optimal(self):
        """Verify optimal pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=50.0,
            parallel_call_rate=60.0,
            missed_opportunities=1,
            total_messages=10,
        )
        assert pattern == "optimal"

    def test_classify_efficiency_pattern_effective(self):
        """Verify effective pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=25.0,
            parallel_call_rate=35.0,
            missed_opportunities=2,
            total_messages=10,
        )
        assert pattern == "effective"

    def test_classify_efficiency_pattern_underutilized(self):
        """Verify underutilized pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=10.0,
            parallel_call_rate=15.0,
            missed_opportunities=5,
            total_messages=10,
        )
        assert pattern == "underutilized"

    def test_classify_efficiency_pattern_sequential(self):
        """Verify sequential pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=5.0,
            parallel_call_rate=8.0,
            missed_opportunities=0,
            total_messages=10,
        )
        assert pattern == "sequential"

    def test_classify_efficiency_pattern_simple(self):
        """Verify simple pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=50.0,
            parallel_call_rate=50.0,
            missed_opportunities=0,
            total_messages=3,
        )
        assert pattern == "simple"

    def test_classify_efficiency_pattern_empty(self):
        """Verify empty pattern classification."""
        pattern = _classify_efficiency_pattern(
            parallelization_rate=0.0,
            parallel_call_rate=0.0,
            missed_opportunities=0,
            total_messages=0,
        )
        assert pattern == "empty"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_highly_parallel_workflow(self):
        """Simulate workflow with heavy parallel usage."""
        messages = []
        for i in range(8):
            messages.append({
                "message_index": i,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            })

        result = analyze_session_parallel_tool_efficiency(messages)

        assert result["parallelization_rate"] == 100.0
        assert result["parallel_call_rate"] == 100.0
        assert result["average_parallel_group_size"] == 3.0
        assert result["efficiency_pattern"] == "optimal"

    def test_mostly_sequential_workflow(self):
        """Simulate workflow with minimal parallelization."""
        messages = []
        for i in range(10):
            messages.append({
                "message_index": i,
                "tool_calls": [{"tool_name": "Read"}],
            })

        result = analyze_session_parallel_tool_efficiency(messages)

        assert result["parallelization_rate"] == 0.0
        assert result["efficiency_pattern"] == "sequential"

    def test_balanced_workflow(self):
        """Simulate balanced workflow with some parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": 0, "tool_calls": [{"tool_name": "Grep"}]},
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
            {"message_index": 2, "tool_calls": [{"tool_name": "Edit"}]},
            {
                "message_index": 3,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Bash"},
                ],
            },
            {"message_index": 4, "tool_calls": [{"tool_name": "Write"}]},
            {"message_index": 5, "tool_calls": [{"tool_name": "Bash"}]},
        ])

        assert result["total_messages"] == 6
        assert result["parallel_messages"] == 2
        assert result["parallelization_rate"] == pytest.approx(33.33, abs=0.01)
        assert result["efficiency_pattern"] in ("effective", "moderate")

    def test_large_parallel_groups(self):
        """Simulate workflow with large parallel groups."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 0,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
            {"message_index": 1, "tool_calls": [{"tool_name": "Edit"}]},
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Bash"},
                    {"tool_name": "Bash"},
                    {"tool_name": "Bash"},
                ],
            },
        ])

        assert result["max_parallel_group_size"] == 5
        assert result["average_parallel_group_size"] == 4.0
