"""Tests for session parallel tool usage analyzer."""

import pytest

from synthesis.session_parallel_tool_usage import (
    analyze_session_parallel_tool_usage,
    _percentage,
    _average,
<<<<<<< HEAD
    _calculate_efficiency_score,
=======
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
)


class TestAnalyzeSessionParallelToolUsage:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_parallel_tool_usage([])

        assert result["total_turns"] == 0
        assert result["turns_with_tools"] == 0
        assert result["total_tool_calls"] == 0
<<<<<<< HEAD
        assert result["parallel_tool_calls"] == 0
        assert result["sequential_tool_calls"] == 0
        assert result["parallel_usage_percentage"] == 0.0
        assert result["turns_with_parallel"] == 0
        assert result["avg_parallel_batch_size"] == 0.0
        assert result["max_parallel_batch_size"] == 0
        assert result["missed_opportunities"] == 0
        assert result["parallelization_by_tool"] == []
        assert result["common_parallel_patterns"] == []
        assert result["optimization_mode_comparison"] == []
        assert result["parallel_efficiency_score"] == 0.0
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_usage(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_usage("not a list")

<<<<<<< HEAD
    def test_turn_with_no_tool_calls(self):
        """Verify turn without tool calls is counted but not analyzed."""
        result = analyze_session_parallel_tool_usage([
            {"turn_index": 1, "tool_calls": []}
        ])

        assert result["total_turns"] == 1
        assert result["turns_with_tools"] == 0

    def test_single_tool_call_counted_as_sequential(self):
        """Verify single tool call is counted as sequential."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0}
                ]
            }
        ])

        assert result["turns_with_tools"] == 1
        assert result["total_tool_calls"] == 1
        assert result["sequential_tool_calls"] == 1
        assert result["parallel_tool_calls"] == 0
        assert result["parallel_usage_percentage"] == 0.0

    def test_two_parallel_tool_calls(self):
        """Verify two parallel tool calls are detected."""
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        assert result["turns_with_parallel"] == 1
        assert result["parallel_tool_calls"] == 2
        assert result["sequential_tool_calls"] == 0
        assert result["parallel_usage_percentage"] == 100.0
        assert result["avg_parallel_batch_size"] == 2.0
        assert result["max_parallel_batch_size"] == 2

    def test_large_parallel_batch(self):
        """Verify large parallel batches are tracked."""
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                    {"tool_name": "Grep", "call_index": 3},
                    {"tool_name": "Glob", "call_index": 4},
                ]
            }
        ])

        assert result["avg_parallel_batch_size"] == 5.0
        assert result["max_parallel_batch_size"] == 5
        assert result["parallel_tool_calls"] == 5

    def test_mixed_parallel_and_sequential_turns(self):
        """Verify mixed parallelization patterns."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Grep", "call_index": 0}]
=======
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ],
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
            },
            {
                "turn_index": 2,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Edit", "call_index": 0}]
            },
        ])

        assert result["total_turns"] == 3
        assert result["turns_with_tools"] == 3
        assert result["total_tool_calls"] == 4
        assert result["parallel_tool_calls"] == 2
        assert result["sequential_tool_calls"] == 2
        assert result["parallel_usage_percentage"] == 50.0
        assert result["turns_with_parallel"] == 1

    def test_missed_parallelization_opportunity_detected(self):
        """Verify detection of missed parallelization opportunities."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            },
        ])

        # Two sequential Read calls = missed opportunity
        assert result["missed_opportunities"] == 1

    def test_missed_opportunity_with_different_parallelizable_tools(self):
        """Verify missed opportunity with different but parallelizable tools."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Grep", "call_index": 0}]
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            },
        ])

        # Grep then Read sequentially = missed opportunity
        assert result["missed_opportunities"] == 1

    def test_no_missed_opportunity_with_sequential_tools(self):
        """Verify no missed opportunity for inherently sequential tools."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Edit", "call_index": 0}]
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit", "call_index": 0}]
            },
        ])

        # Edit tools often have dependencies, not counted as missed opportunity
        assert result["missed_opportunities"] == 0

    def test_parallelization_by_tool_breakdown(self):
        """Verify tool-specific parallelization breakdown."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Grep", "call_index": 0}]
            },
        ])

        by_tool = result["parallelization_by_tool"]
        read_stats = next(t for t in by_tool if t["tool"] == "Read")
        grep_stats = next(t for t in by_tool if t["tool"] == "Grep")

        assert read_stats["parallel_count"] == 2
        assert read_stats["sequential_count"] == 1
        assert read_stats["total_count"] == 3
        assert read_stats["parallel_percentage"] == 66.67

        assert grep_stats["parallel_count"] == 0
        assert grep_stats["sequential_count"] == 1
        assert grep_stats["parallel_percentage"] == 0.0

    def test_common_parallel_patterns_detected(self):
        """Verify common parallel patterns are tracked."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            {
                "turn_index": 3,
                "tool_calls": [
                    {"tool_name": "Grep", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        assert len(patterns) == 2

        # Most common pattern should be [Read, Read]
        top_pattern = patterns[0]
        assert top_pattern["count"] == 2
        assert sorted(top_pattern["tools"]) == ["Read", "Read"]

    def test_optimization_mode_comparison(self):
        """Verify optimization mode comparison metrics."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}],
                "optimization_mode": "baseline",
                "turn_duration": 10.0,
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}],
                "optimization_mode": "baseline",
                "turn_duration": 12.0,
            },
            {
                "turn_index": 3,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ],
                "optimization_mode": "optimized",
                "turn_duration": 8.0,
            },
            {
                "turn_index": 4,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Grep", "call_index": 2},
                ],
                "optimization_mode": "optimized",
                "turn_duration": 9.0,
            },
        ])

        comparison = result["optimization_mode_comparison"]
        baseline = next(m for m in comparison if m["mode"] == "baseline")
        optimized = next(m for m in comparison if m["mode"] == "optimized")

        assert baseline["turns"] == 2
        assert baseline["parallel_calls"] == 0
        assert baseline["sequential_calls"] == 2
        assert baseline["parallel_percentage"] == 0.0
        assert baseline["avg_turn_duration"] == 11.0

        assert optimized["turns"] == 2
        assert optimized["parallel_calls"] == 5
        assert optimized["sequential_calls"] == 0
        assert optimized["parallel_percentage"] == 100.0
        assert optimized["avg_turn_duration"] == 8.5

    def test_efficiency_score_high_for_optimal_usage(self):
        """Verify high efficiency score for optimal parallelization."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                    {"tool_name": "Grep", "call_index": 3},
                    {"tool_name": "Glob", "call_index": 4},
                ]
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Grep", "call_index": 1},
                ]
            },
        ])

        # 100% parallel usage, good batch sizes, no missed opportunities
        assert result["parallel_efficiency_score"] >= 90.0

    def test_efficiency_score_low_for_poor_usage(self):
        """Verify low efficiency score for poor parallelization."""
        result = analyze_session_parallel_tool_usage([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Grep", "call_index": 0}]},
            {"turn_index": 4, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
        ])

        # All sequential, multiple missed opportunities
        assert result["parallel_efficiency_score"] <= 20.0
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_usage([
            "not a dict",
            {
                "turn_index": 1,
<<<<<<< HEAD
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
=======
                "tool_calls": [{"tool_name": "Read"}],
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
            },
        ])

        assert result["total_turns"] == 1

<<<<<<< HEAD
    def test_turn_with_invalid_tool_calls_field(self):
        """Verify turn with invalid tool_calls field is skipped."""
        result = analyze_session_parallel_tool_usage([
            {"turn_index": 1, "tool_calls": "not a list"},
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            },
        ])

        assert result["turns_with_tools"] == 1

    def test_tool_call_without_tool_name_skipped(self):
        """Verify tool calls without tool_name are skipped."""
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
<<<<<<< HEAD
                    {"call_index": 0},  # Missing tool_name
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        # Only one valid tool call, so sequential
        assert result["sequential_tool_calls"] == 1
        assert result["parallel_tool_calls"] == 0

    def test_empty_tool_name_skipped(self):
        """Verify empty tool names are skipped."""
=======
                    "not a dict",
                    {"tool_name": "Read"},
                ],
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_tool_call_with_missing_tool_name(self):
        """Verify tool calls with missing tool_name are skipped."""
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            }
        ])

        assert result["sequential_tool_calls"] == 1

    def test_whitespace_tool_name_stripped(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "  Read  ", "call_index": 0},
                    {"tool_name": "Edit", "call_index": 1},
                ]
            }
        ])

        assert result["parallel_tool_calls"] == 2
        by_tool = result["parallelization_by_tool"]
        read_stats = next(t for t in by_tool if t["tool"] == "Read")
        assert read_stats["parallel_count"] == 1

    def test_patterns_limited_to_top_10(self):
        """Verify common patterns limited to 10."""
        turns = []
        for i in range(15):
            turns.append({
                "turn_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}A", "call_index": 0},
                    {"tool_name": f"Tool{i}B", "call_index": 1},
                ]
            })

        result = analyze_session_parallel_tool_usage(turns)
        assert len(result["common_parallel_patterns"]) == 10

    def test_turn_without_tool_calls_resets_opportunity_detection(self):
        """Verify turn without tools resets opportunity tracking."""
        result = analyze_session_parallel_tool_usage([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"turn_index": 2, "tool_calls": []},  # No tools
            {"turn_index": 3, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
        ])

        # No missed opportunity because turn 2 has no tools (breaks sequence)
        assert result["missed_opportunities"] == 0

    def test_missing_optimization_mode_uses_unknown(self):
        """Verify missing optimization mode is tracked as unknown."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            }
        ])

        comparison = result["optimization_mode_comparison"]
        assert len(comparison) == 1
        assert comparison[0]["mode"] == "unknown"

    def test_missing_duration_defaults_to_zero(self):
        """Verify missing duration defaults to 0.0."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}]
            }
        ])

        comparison = result["optimization_mode_comparison"]
        assert comparison[0]["avg_turn_duration"] == 0.0
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY


class TestPercentage:
    """Test percentage calculation helper."""

<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY


class TestAverage:
    """Test average calculation helper."""

<<<<<<< HEAD
    def test_empty_list_returns_zero(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average([2, 4, 6, 8]) == 5.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average([1, 2, 3]) == 2.0


class TestCalculateEfficiencyScore:
    """Test efficiency score calculation."""

    def test_perfect_score(self):
        """Verify perfect efficiency yields score near 100."""
        # 100% parallel, batch size 5, no missed opportunities
        score = _calculate_efficiency_score(100.0, 5.0, 0, 100)
        assert score == 100.0

    def test_zero_score(self):
        """Verify poor efficiency yields low score."""
        # 0% parallel, batch size 0, many missed opportunities
        # With 100% missed opportunities (all 100 calls), penalty maxes at 20
        # So score will be 0 (parallel) + 0 (batch) + 0 (opportunity) = 0
        score = _calculate_efficiency_score(0.0, 0.0, 100, 100)
        assert score == 0.0

    def test_moderate_score(self):
        """Verify moderate efficiency yields moderate score."""
        # 50% parallel, batch size 2.5, some missed opportunities
        score = _calculate_efficiency_score(50.0, 2.5, 10, 100)
        assert 30.0 <= score <= 60.0

    def test_high_parallel_usage_increases_score(self):
        """Verify high parallel usage increases score."""
        score_low = _calculate_efficiency_score(20.0, 2.0, 5, 100)
        score_high = _calculate_efficiency_score(80.0, 2.0, 5, 100)
        assert score_high > score_low

    def test_large_batch_size_increases_score(self):
        """Verify larger batch sizes increase score."""
        score_small = _calculate_efficiency_score(50.0, 1.0, 5, 100)
        score_large = _calculate_efficiency_score(50.0, 5.0, 5, 100)
        assert score_large > score_small

    def test_missed_opportunities_decrease_score(self):
        """Verify missed opportunities decrease score."""
        score_few_missed = _calculate_efficiency_score(50.0, 3.0, 1, 100)
        score_many_missed = _calculate_efficiency_score(50.0, 3.0, 30, 100)
        assert score_few_missed > score_many_missed
=======
    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(100, 4) == 25.0

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0."""
        assert _average(100, 0) == 0.0

    def test_rounding(self):
        """Verify rounding to 2 decimal places."""
        assert _average(100, 3) == 33.33
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

<<<<<<< HEAD
    def test_highly_parallel_optimized_session(self):
        """Simulate highly parallelized optimized session."""
=======
    def test_high_parallelization_session(self):
        """Simulate session with high parallelization."""
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                    {"tool_name": "Read", "call_index": 2},
                ],
                "optimization_mode": "optimized",
=======
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
            },
            {
                "turn_index": 2,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Grep", "call_index": 0},
                    {"tool_name": "Grep", "call_index": 1},
                ],
                "optimization_mode": "optimized",
            },
        ])

        assert result["parallel_usage_percentage"] == 100.0
        assert result["missed_opportunities"] == 0
        assert result["parallel_efficiency_score"] >= 85.0

        comparison = result["optimization_mode_comparison"]
        opt = next(m for m in comparison if m["mode"] == "optimized")
        assert opt["parallel_percentage"] == 100.0

    def test_sequential_baseline_session(self):
        """Simulate sequential baseline session."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "call_index": 0}],
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
                "optimization_mode": "baseline",
            },
            {
                "turn_index": 2,
<<<<<<< HEAD
                "tool_calls": [{"tool_name": "Read", "call_index": 0}],
                "optimization_mode": "baseline",
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Grep", "call_index": 0}],
                "optimization_mode": "baseline",
            },
        ])

        assert result["parallel_usage_percentage"] == 0.0
        assert result["missed_opportunities"] >= 1
        assert result["parallel_efficiency_score"] <= 30.0

    def test_baseline_vs_optimized_comparison(self):
        """Simulate session with both baseline and optimized modes."""
        result = analyze_session_parallel_tool_usage([
            # Baseline: sequential
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read", "call_index": 0}], "optimization_mode": "baseline"},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Read", "call_index": 0}], "optimization_mode": "baseline"},
            # Optimized: parallel
            {
                "turn_index": 3,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ],
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
                "optimization_mode": "optimized",
            },
        ])

<<<<<<< HEAD
        comparison = result["optimization_mode_comparison"]
        baseline = next(m for m in comparison if m["mode"] == "baseline")
        optimized = next(m for m in comparison if m["mode"] == "optimized")

        assert baseline["parallel_percentage"] == 0.0
        assert optimized["parallel_percentage"] == 100.0
        assert optimized["parallel_percentage"] > baseline["parallel_percentage"]

    def test_common_read_grep_pattern(self):
        """Simulate common pattern of parallel Read and Grep calls."""
        result = analyze_session_parallel_tool_usage([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Grep", "call_index": 1},
                ]
            },
            {
                "turn_index": 2,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Grep", "call_index": 1},
                ]
            },
        ])

        patterns = result["common_parallel_patterns"]
        top_pattern = patterns[0]
        assert sorted(top_pattern["tools"]) == ["Grep", "Read"]
        assert top_pattern["count"] == 2

    def test_tool_specific_parallelization_insights(self):
        """Verify tool-specific parallelization insights."""
        result = analyze_session_parallel_tool_usage([
            # Read: often parallel
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "call_index": 0},
                    {"tool_name": "Read", "call_index": 1},
                ]
            },
            # Edit: often sequential
            {"turn_index": 2, "tool_calls": [{"tool_name": "Edit", "call_index": 0}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Edit", "call_index": 0}]},
        ])

        by_tool = result["parallelization_by_tool"]
        read_stats = next(t for t in by_tool if t["tool"] == "Read")
        edit_stats = next(t for t in by_tool if t["tool"] == "Edit")

        assert read_stats["parallel_percentage"] == 100.0
        assert edit_stats["parallel_percentage"] == 0.0

    def test_multiple_missed_opportunities(self):
        """Verify detection of multiple missed opportunities."""
        result = analyze_session_parallel_tool_usage([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Read", "call_index": 0}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Grep", "call_index": 0}]},
            {"turn_index": 4, "tool_calls": [{"tool_name": "Glob", "call_index": 0}]},
        ])

        # Read→Read, Grep→Glob = 2 missed opportunities
        assert result["missed_opportunities"] >= 2
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
