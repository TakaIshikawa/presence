"""Tests for session parallel tool call efficiency analyzer."""

import pytest

from synthesis.session_parallel_tool_efficiency import (
    analyze_session_parallel_tool_efficiency,
<<<<<<< HEAD
    _detect_missed_opportunities,
=======
    _average,
    _classify_efficiency_pattern,
    _format_patterns,
    _percentage,
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
)


class TestAnalyzeSessionParallelToolEfficiency:
    """Test main analyzer function."""

<<<<<<< HEAD
    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_parallel_tool_efficiency([])

        assert result["total_messages"] == 0
        assert result["messages_with_parallel_calls"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["avg_parallel_group_size"] == 0.0
        assert result["total_parallel_calls"] == 0
        assert result["parallel_patterns"] == []
        assert result["missed_opportunities"] == []
        assert result["parallel_success_rate"] == 100.0  # Edge case
        assert result["efficiency_score"] == 0.0
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_efficiency(None)
        assert result["total_messages"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_efficiency("not a list")

<<<<<<< HEAD
    def test_single_sequential_call_no_parallelization(self):
        """Verify single sequential call shows no parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read", "success": True}]
            }
        ])

        assert result["total_messages"] == 1
        assert result["messages_with_parallel_calls"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["total_parallel_calls"] == 0

    def test_parallel_calls_detected(self):
        """Verify parallel calls are detected."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        assert result["messages_with_parallel_calls"] == 1
        assert result["total_parallel_calls"] == 2
        assert result["parallelization_rate"] == 100.0
        assert result["avg_parallel_group_size"] == 2.0

    def test_mixed_sequential_and_parallel(self):
        """Verify mixed sequential and parallel calls calculated correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read", "success": True}]
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
            },
            {
                "message_index": 2,
                "tool_calls": [
<<<<<<< HEAD
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            },
        ])

        # 3 total calls, 2 in parallel -> 66.67% parallelization rate
        assert result["total_messages"] == 2
        assert result["messages_with_parallel_calls"] == 1
        assert result["total_parallel_calls"] == 2
        assert result["parallelization_rate"] == 66.67

    def test_parallel_patterns_tracked(self):
        """Verify parallel patterns are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ]
            }
        ])

        patterns = result["parallel_patterns"]
        assert len(patterns) == 1
        assert patterns[0]["tools"] == ["Read", "Read"]
        assert patterns[0]["count"] == 1

    def test_parallel_patterns_sorted(self):
        """Verify parallel patterns are sorted by tools (alphabetically)."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Edit"},
                    {"tool_name": "Bash"},
                    {"tool_name": "Read"},
                ]
            }
        ])

        patterns = result["parallel_patterns"]
        # Tools should be sorted alphabetically
        assert patterns[0]["tools"] == ["Bash", "Edit", "Read"]

    def test_parallel_patterns_counted(self):
        """Verify parallel patterns are counted correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ]
            },
        ])

        patterns = result["parallel_patterns"]
        assert patterns[0]["count"] == 2

    def test_parallel_success_rate_calculated(self):
        """Verify parallel success rate is calculated correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": False},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        # 3 out of 4 succeeded = 75%
        assert result["parallel_success_rate"] == 75.0

    def test_parallel_success_rate_default_true(self):
        """Verify success defaults to True when not specified."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ]
            }
        ])

        # Both should count as successful
        assert result["parallel_success_rate"] == 100.0

    def test_avg_parallel_group_size_calculated(self):
        """Verify average parallel group size is calculated correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                ]
            },
            {
                "message_index": 2,
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
                    {"tool_name": "Read"},
<<<<<<< HEAD
                ]
=======
                ],
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
            },
        ])

        # Average of 2 and 4 = 3.0
<<<<<<< HEAD
        assert result["avg_parallel_group_size"] == 3.0

    def test_efficiency_score_calculated(self):
        """Verify efficiency score is calculated."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        # Should have non-zero efficiency score
        assert result["efficiency_score"] > 0.0
        assert result["efficiency_score"] <= 100.0

    def test_efficiency_score_perfect_session(self):
        """Verify efficiency score for perfect parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        # Perfect: 100% parallelization, group size 5, 100% success
        # Score = 100 * 0.5 + min(5/5, 1) * 30 + 100 * 0.2 = 50 + 30 + 20 = 100
        assert result["efficiency_score"] == 100.0

    def test_missed_opportunity_consecutive_reads(self):
        """Verify missed opportunity for consecutive reads."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read"}]
            },
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Read"}]
            },
        ])

        opportunities = result["missed_opportunities"]
        assert len(opportunities) > 0
        assert opportunities[0]["type"] == "consecutive_read"
        assert opportunities[0]["count"] == 2

    def test_missed_opportunity_consecutive_greps(self):
        """Verify missed opportunity for consecutive greps."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Grep"}]
            },
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Grep"}]
            },
        ])

        opportunities = result["missed_opportunities"]
        assert any(opp["type"] == "consecutive_grep" for opp in opportunities)

    def test_no_missed_opportunity_non_adjacent_messages(self):
        """Verify non-adjacent messages don't trigger missed opportunities."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read"}]
            },
            {
                "message_index": 3,  # Skipped message 2
                "tool_calls": [{"tool_name": "Read"}]
            },
        ])

        # Should not detect opportunity since messages aren't adjacent
        opportunities = result["missed_opportunities"]
        assert len(opportunities) == 0

    def test_no_missed_opportunity_different_tools(self):
        """Verify different tools don't trigger missed opportunities."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read"}]
            },
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Edit"}]
            },
        ])

        # Different tools shouldn't trigger missed opportunity
        opportunities = result["missed_opportunities"]
        assert len(opportunities) == 0

    def test_no_missed_opportunity_for_non_parallelizable_tools(self):
        """Verify non-parallelizable tools don't trigger missed opportunities."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Edit"}]
            },
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Edit"}]
            },
        ])

        # Edit calls typically depend on each other
        opportunities = result["missed_opportunities"]
        assert len(opportunities) == 0

    def test_missed_opportunities_limited_to_ten(self):
        """Verify missed opportunities are limited to 10."""
        # Create many consecutive reads
        records = [
            {
                "message_index": i,
                "tool_calls": [{"tool_name": "Read"}]
            }
            for i in range(30)
        ]

        result = analyze_session_parallel_tool_efficiency(records)
        assert len(result["missed_opportunities"]) <= 10

    def test_parallel_patterns_limited_to_ten(self):
        """Verify parallel patterns are limited to 10."""
        # Create many different parallel patterns
        records = []
        for i in range(20):
            records.append({
                "message_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}A"},
                    {"tool_name": f"Tool{i}B"},
                ]
            })

        result = analyze_session_parallel_tool_efficiency(records)
        assert len(result["parallel_patterns"]) <= 10
=======
        assert result["average_parallel_group_size"] == 3.0
        assert result["max_parallel_group_size"] == 4
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            "not a dict",
<<<<<<< HEAD
            {
                "message_index": 1,
                "tool_calls": [{"tool_name": "Read"}]
            },
=======
            {"message_index": 0, "tool_calls": [{"tool_name": "Read"}]},
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
        ])

        assert result["total_messages"] == 1

    def test_missing_tool_calls_skipped(self):
        """Verify records without tool_calls are skipped."""
        result = analyze_session_parallel_tool_efficiency([
<<<<<<< HEAD
            {"message_index": 1},
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Read"}]
            },
        ])

        assert result["total_messages"] == 2  # Counted but no tool calls

    def test_empty_tool_calls_list(self):
        """Verify records with empty tool_calls list."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": []
            }
        ])

        assert result["total_messages"] == 1
        assert result["messages_with_parallel_calls"] == 0

    def test_non_list_tool_calls_skipped(self):
        """Verify non-list tool_calls are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": "not a list"
            }
        ])

        assert result["total_messages"] == 0  # Skipped


class TestDetectMissedOpportunities:
    """Test missed opportunity detection helper."""

    def test_empty_input_returns_empty(self):
        """Verify empty input returns no opportunities."""
        opportunities = _detect_missed_opportunities([])
        assert opportunities == []

    def test_consecutive_reads_detected(self):
        """Verify consecutive reads are detected."""
        calls = [
            {"message_index": 1, "tool_name": "Read"},
            {"message_index": 2, "tool_name": "Read"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 1
        assert opportunities[0]["type"] == "consecutive_read"
        assert opportunities[0]["count"] == 2

    def test_consecutive_greps_detected(self):
        """Verify consecutive greps are detected."""
        calls = [
            {"message_index": 1, "tool_name": "Grep"},
            {"message_index": 2, "tool_name": "Grep"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 1
        assert opportunities[0]["type"] == "consecutive_grep"

    def test_consecutive_globs_detected(self):
        """Verify consecutive globs are detected."""
        calls = [
            {"message_index": 1, "tool_name": "Glob"},
            {"message_index": 2, "tool_name": "Glob"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 1
        assert opportunities[0]["type"] == "consecutive_glob"

    def test_three_consecutive_reads_detected(self):
        """Verify three consecutive reads are detected."""
        calls = [
            {"message_index": 1, "tool_name": "Read"},
            {"message_index": 2, "tool_name": "Read"},
            {"message_index": 3, "tool_name": "Read"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 1
        assert opportunities[0]["count"] == 3

    def test_non_adjacent_messages_not_detected(self):
        """Verify non-adjacent messages don't trigger detection."""
        calls = [
            {"message_index": 1, "tool_name": "Read"},
            {"message_index": 3, "tool_name": "Read"},  # Skipped 2
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 0

    def test_different_tools_not_detected(self):
        """Verify different tools don't trigger detection."""
        calls = [
            {"message_index": 1, "tool_name": "Read"},
            {"message_index": 2, "tool_name": "Edit"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 0

    def test_non_parallelizable_tools_not_detected(self):
        """Verify non-parallelizable tools don't trigger detection."""
        calls = [
            {"message_index": 1, "tool_name": "Edit"},
            {"message_index": 2, "tool_name": "Edit"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 0

    def test_single_call_not_detected(self):
        """Verify single call doesn't trigger detection."""
        calls = [
            {"message_index": 1, "tool_name": "Read"},
        ]
        opportunities = _detect_missed_opportunities(calls)

        assert len(opportunities) == 0
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

<<<<<<< HEAD
    def test_fully_sequential_session(self):
        """Simulate fully sequential session with no parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": i, "tool_calls": [{"tool_name": "Read"}]}
            for i in range(10)
        ])

        assert result["parallelization_rate"] == 0.0
        assert result["messages_with_parallel_calls"] == 0
        # Should detect missed opportunities
        assert len(result["missed_opportunities"]) > 0

    def test_fully_parallel_session(self):
        """Simulate fully parallel session."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        assert result["parallelization_rate"] == 100.0
        assert result["messages_with_parallel_calls"] == 1
        assert result["missed_opportunities"] == []

    def test_mixed_session_with_opportunities(self):
        """Simulate mixed session with both parallel and missed opportunities."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [{"tool_name": "Edit"}]
            },
            {
                "message_index": 3,
                "tool_calls": [{"tool_name": "Read"}]
            },
            {
                "message_index": 4,
                "tool_calls": [{"tool_name": "Read"}]
            },
        ])

        # Should have parallelization
        assert result["messages_with_parallel_calls"] == 1
        # Should detect missed opportunity for messages 3-4
        assert len(result["missed_opportunities"]) > 0

    def test_high_efficiency_session(self):
        """Simulate high efficiency session with good parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": True},
                ]
            },
            {
                "message_index": 2,
                "tool_calls": [
                    {"tool_name": "Edit", "success": True},
                    {"tool_name": "Write", "success": True},
                ]
            },
        ])

        # High parallelization rate and success
        assert result["parallelization_rate"] == 100.0
        assert result["parallel_success_rate"] == 100.0
        assert result["efficiency_score"] > 80.0

    def test_low_efficiency_session(self):
        """Simulate low efficiency session with poor parallelization."""
        result = analyze_session_parallel_tool_efficiency([
            {"message_index": i, "tool_calls": [{"tool_name": "Read"}]}
            for i in range(10)
        ])

        # No parallelization
        assert result["parallelization_rate"] == 0.0
        assert result["efficiency_score"] < 20.0

    def test_empty_session(self):
        """Simulate empty session."""
        result = analyze_session_parallel_tool_efficiency([])

        assert result["total_messages"] == 0
        assert result["efficiency_score"] == 0.0

    def test_session_with_failures(self):
        """Simulate session with some parallel call failures."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "message_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "success": True},
                    {"tool_name": "Read", "success": False},
                    {"tool_name": "Read", "success": False},
                    {"tool_name": "Read", "success": True},
                ]
            }
        ])

        # 50% success rate
        assert result["parallel_success_rate"] == 50.0
        # Efficiency should be impacted
        assert result["efficiency_score"] < 100.0
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
