"""Tests for session parallel tool usage analyzer."""

import pytest

from synthesis.session_parallel_tool_usage import (
    analyze_session_parallel_tool_usage,
    _percentage,
    _average,
)


class TestAnalyzeSessionParallelToolUsage:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_parallel_tool_usage([])

        assert result["total_turns"] == 0
        assert result["turns_with_tools"] == 0
        assert result["total_tool_calls"] == 0
        assert result["parallel_turns"] == 0
        assert result["parallel_usage_rate"] == 0.0
        assert result["total_parallel_batches"] == 0
        assert result["avg_parallel_batch_size"] == 0.0
        assert result["max_parallel_batch_size"] == 0
        assert result["missed_opportunities"] == 0
        assert result["tool_parallelization"] == {}
        assert result["mode_comparison"]["baseline"]["parallel_usage_rate"] == 0.0
        assert result["mode_comparison"]["optimized"]["parallel_usage_rate"] == 0.0
        assert result["examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_usage(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_usage("not a list")

    def test_single_tool_call_no_parallelization(self):
        """Verify single tool call registers no parallelization."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            }
        ])

        assert result["total_turns"] == 1
        assert result["turns_with_tools"] == 1
        assert result["total_tool_calls"] == 1
        assert result["parallel_turns"] == 0
        assert result["parallel_usage_rate"] == 0.0

    def test_parallel_tool_calls_detected(self):
        """Verify parallel tool calls are detected."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
            }
        ])

        assert result["parallel_turns"] == 1
        assert result["parallel_usage_rate"] == 100.0
        assert result["total_parallel_batches"] == 1
        assert result["avg_parallel_batch_size"] == 3.0
        assert result["max_parallel_batch_size"] == 3

    def test_tool_parallelization_tracking(self):
        """Verify tool parallelization counts are tracked."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
            },
        ])

        # Read appears in 2 parallel batches, Grep in 1
        assert result["tool_parallelization"]["Read"] == 2
        assert result["tool_parallelization"]["Grep"] == 1

    def test_missed_opportunities_detected(self):
        """Verify missed parallelization opportunities are detected."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Grep"}],  # Could be parallel with previous Read
            },
        ])

        assert result["missed_opportunities"] == 1

    def test_no_missed_opportunity_for_same_tool(self):
        """Verify no missed opportunity when same tool used sequentially."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}],  # Same tool, might be dependent
            },
        ])

        assert result["missed_opportunities"] == 0

    def test_mode_comparison_baseline(self):
        """Verify baseline mode tracking."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
                "optimization_mode": "baseline",
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "optimization_mode": "baseline",
            },
        ])

        baseline = result["mode_comparison"]["baseline"]
        assert baseline["turns_with_tools"] == 2
        assert baseline["parallel_turns"] == 1
        assert baseline["parallel_usage_rate"] == 50.0
        assert baseline["total_tool_calls"] == 3

    def test_mode_comparison_optimized(self):
        """Verify optimized mode tracking."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "optimization_mode": "optimized",
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit"}],
                "optimization_mode": "optimized",
            },
        ])

        optimized = result["mode_comparison"]["optimized"]
        assert optimized["turns_with_tools"] == 2
        assert optimized["parallel_turns"] == 1
        assert optimized["parallel_usage_rate"] == 50.0

    def test_mode_comparison_mixed_session(self):
        """Verify mode comparison in mixed session."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
                "optimization_mode": "baseline",
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "optimization_mode": "optimized",
            },
        ])

        assert result["mode_comparison"]["baseline"]["parallel_usage_rate"] == 0.0
        assert result["mode_comparison"]["optimized"]["parallel_usage_rate"] == 100.0

    def test_examples_collected(self):
        """Verify examples are collected."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "optimization_mode": "optimized",
            }
        ])

        assert len(result["examples"]) == 1
        example = result["examples"][0]
        assert example["turn_index"] == 1
        assert example["tools"] == ["Read", "Grep"]
        assert example["batch_size"] == 2
        assert example["optimization_mode"] == "optimized"

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        turns = [
            {
                "turn_index": i,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
            }
            for i in range(10)
        ]

        result = analyze_session_parallel_tool_usage(turns)
        assert len(result["examples"]) == 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_usage([
            "not a dict",
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
        ])

        assert result["total_turns"] == 1

    def test_turn_with_no_tool_calls(self):
        """Verify turns with no tool calls are handled."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
            },
        ])

        assert result["total_turns"] == 2
        assert result["turns_with_tools"] == 1

    def test_turn_with_missing_tool_calls(self):
        """Verify turns with missing tool_calls key are handled."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
            },
        ])

        assert result["total_turns"] == 2
        assert result["turns_with_tools"] == 1

    def test_malformed_tool_call_skipped(self):
        """Verify non-dict tool calls are skipped."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    "not a dict",
                    {"tool_name": "Read"},
                ],
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_tool_call_with_missing_tool_name(self):
        """Verify tool calls with missing tool_name are skipped."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"timestamp": "2024-01-01"},
                    {"tool_name": "Read"},
                ],
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_average_batch_size_calculation(self):
        """Verify average parallel batch size calculation."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],  # Size 2
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                    {"tool_name": "Edit"},
                    {"tool_name": "Write"},
                ],  # Size 4
            },
        ])

        # Average: (2 + 4) / 2 = 3
        assert result["avg_parallel_batch_size"] == 3.0
        assert result["max_parallel_batch_size"] == 4

    def test_tool_parallelization_limited_to_ten(self):
        """Verify tool parallelization is limited to top 10."""
        # Create more than 10 different tools
        tool_calls = []
        for i in range(15):
            tool_calls.append({
                "turn_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}"},
                    {"tool_name": "Read"},
                ],
            })

        result = analyze_session_parallel_tool_usage(tool_calls)
        assert len(result["tool_parallelization"]) <= 10


class TestPercentage:
    """Test percentage calculation helper."""

    def test_perfect_percentage(self):
        """Verify perfect percentage returns 100."""
        assert _percentage(5, 5) == 100.0

    def test_partial_percentage(self):
        """Verify partial percentage calculation."""
        assert _percentage(1, 4) == 25.0
        assert _percentage(3, 4) == 75.0

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0."""
        assert _percentage(5, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0."""
        assert _percentage(0, 5) == 0.0


class TestAverage:
    """Test average calculation helper."""

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(100, 4) == 25.0

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0."""
        assert _average(100, 0) == 0.0

    def test_rounding(self):
        """Verify rounding to 2 decimal places."""
        assert _average(100, 3) == 33.33


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_high_parallelization_session(self):
        """Simulate session with high parallelization."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Edit"},
                    {"tool_name": "Edit"},
                ],
            },
            {
                "turn_index": 3,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Write"},
                ],
            },
        ])

        assert result["parallel_usage_rate"] == 100.0
        assert result["avg_parallel_batch_size"] == 2.33
        assert result["missed_opportunities"] == 0

    def test_sequential_execution_pattern(self):
        """Simulate session with sequential execution."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Grep"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Edit"}],
            },
        ])

        assert result["parallel_usage_rate"] == 0.0
        assert result["missed_opportunities"] == 2  # Turn 1→2 and 2→3

    def test_optimization_mode_impact(self):
        """Simulate improved parallelization in optimized mode."""
        result = analyze_session_parallel_tool_usage([
            # Baseline: mostly sequential
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
                "optimization_mode": "baseline",
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Grep"}],
                "optimization_mode": "baseline",
            },
            # Optimized: more parallel
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "optimization_mode": "optimized",
            },
            {
                "turn_index": 4,
                "tool_calls": [{"tool_name": "Edit"}, {"tool_name": "Write"}],
                "optimization_mode": "optimized",
            },
        ])

        assert result["mode_comparison"]["baseline"]["parallel_usage_rate"] == 0.0
        assert result["mode_comparison"]["optimized"]["parallel_usage_rate"] == 100.0

    def test_mixed_parallel_and_sequential(self):
        """Simulate realistic session with mixed patterns."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Write"}],
            },
            {
                "turn_index": 4,
                "tool_calls": [{"tool_name": "Bash"}, {"tool_name": "Read"}],
            },
        ])

        assert result["turns_with_tools"] == 4
        assert result["parallel_turns"] == 2
        assert result["parallel_usage_rate"] == 50.0
        assert result["missed_opportunities"] == 1  # Turn 2→3
