"""Tests for session tool call batching efficiency analyzer."""

import pytest

from synthesis.session_tool_call_batching import analyze_session_tool_call_batching


class TestAnalyzeSessionToolCallBatching:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_tool_call_batching([])

        assert result["total_turns"] == 0
        assert result["turns_with_tool_calls"] == 0
        assert result["single_call_turns"] == 0
        assert result["batched_turns"] == 0
        assert result["batching_rate"] == 0.0
        assert result["total_tool_calls"] == 0
        assert result["total_batchable_calls"] == 0
        assert result["total_batched_calls"] == 0
        assert result["batching_efficiency_score"] == 0.0
        assert result["avg_batch_size"] == 0.0
        assert result["max_batch_size"] == 0
        assert result["missed_batching_opportunities"] == 0
        assert result["batching_improvement_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_call_batching(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_call_batching("not a list")

    def test_single_call_turn(self):
        """Verify turn with single tool call."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            }
        ])

        assert result["total_turns"] == 1
        assert result["turns_with_tool_calls"] == 1
        assert result["single_call_turns"] == 1
        assert result["batched_turns"] == 0
        assert result["batching_rate"] == 0.0
        assert result["total_tool_calls"] == 1
        assert result["total_batchable_calls"] == 1
        assert result["total_batched_calls"] == 0

    def test_batched_turn_two_calls(self):
        """Verify turn with two tool calls batched together."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 2,
                "batchable_calls": 2,
                "batched_calls": 2,
            }
        ])

        assert result["turns_with_tool_calls"] == 1
        assert result["single_call_turns"] == 0
        assert result["batched_turns"] == 1
        assert result["batching_rate"] == 100.0
        assert result["total_tool_calls"] == 2
        assert result["avg_batch_size"] == 2.0
        assert result["max_batch_size"] == 2

    def test_batched_turn_multiple_calls(self):
        """Verify turn with multiple tool calls batched together."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 5,
                "batchable_calls": 5,
                "batched_calls": 5,
            }
        ])

        assert result["batched_turns"] == 1
        assert result["total_tool_calls"] == 5
        assert result["avg_batch_size"] == 5.0
        assert result["max_batch_size"] == 5

    def test_all_sequential_pattern(self):
        """Verify session with all sequential (single-call) turns."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            },
            {
                "turn_index": 3,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            },
        ])

        assert result["turns_with_tool_calls"] == 3
        assert result["single_call_turns"] == 3
        assert result["batched_turns"] == 0
        assert result["batching_rate"] == 0.0
        assert result["batching_efficiency_score"] == 0.0
        assert result["avg_batch_size"] == 0.0

    def test_optimal_batching_pattern(self):
        """Verify session with optimal batching (all multi-call turns)."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 3,
                "batchable_calls": 3,
                "batched_calls": 3,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 2,
                "batchable_calls": 2,
                "batched_calls": 2,
            },
            {
                "turn_index": 3,
                "tool_calls_count": 4,
                "batchable_calls": 4,
                "batched_calls": 4,
            },
        ])

        assert result["turns_with_tool_calls"] == 3
        assert result["single_call_turns"] == 0
        assert result["batched_turns"] == 3
        assert result["batching_rate"] == 100.0
        assert result["total_tool_calls"] == 9
        assert result["total_batchable_calls"] == 9
        assert result["total_batched_calls"] == 9
        assert result["batching_efficiency_score"] == 100.0
        # (3 + 2 + 4) / 3 = 3
        assert result["avg_batch_size"] == 3.0
        assert result["max_batch_size"] == 4

    def test_mixed_batching_pattern(self):
        """Verify session with mixed single and batched turns."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 3,
                "batchable_calls": 3,
                "batched_calls": 3,
            },
            {
                "turn_index": 3,
                "tool_calls_count": 1,
                "batchable_calls": 1,
                "batched_calls": 0,
            },
            {
                "turn_index": 4,
                "tool_calls_count": 2,
                "batchable_calls": 2,
                "batched_calls": 2,
            },
        ])

        assert result["turns_with_tool_calls"] == 4
        assert result["single_call_turns"] == 2
        assert result["batched_turns"] == 2
        assert result["batching_rate"] == 50.0
        assert result["total_tool_calls"] == 7
        assert result["total_batchable_calls"] == 7
        assert result["total_batched_calls"] == 5
        # 5/7 * 100 = 71.43
        assert result["batching_efficiency_score"] == 71.43

    def test_missed_batching_opportunities(self):
        """Verify tracking of missed batching opportunities."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 1,
                "is_sequential_independent": True,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 1,
                "is_sequential_independent": True,
            },
            {
                "turn_index": 3,
                "tool_calls_count": 2,
                "is_sequential_independent": False,
            },
        ])

        assert result["missed_batching_opportunities"] == 2

    def test_no_missed_opportunities(self):
        """Verify sessions with no missed opportunities."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 3,
                "is_sequential_independent": False,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 2,
                "is_sequential_independent": False,
            },
        ])

        assert result["missed_batching_opportunities"] == 0

    def test_batching_improvement_score_positive(self):
        """Verify positive batching improvement over session."""
        result = analyze_session_tool_call_batching([
            # Early session: all sequential
            {"turn_index": 1, "tool_calls_count": 1},
            {"turn_index": 2, "tool_calls_count": 1},
            # Late session: all batched
            {"turn_index": 3, "tool_calls_count": 3},
            {"turn_index": 4, "tool_calls_count": 2},
        ])

        # Early: 0% batched (0/2)
        # Late: 100% batched (2/2)
        # Improvement: 100 - 0 = 100
        assert result["batching_improvement_score"] == 100.0

    def test_batching_improvement_score_negative(self):
        """Verify negative batching improvement (degradation)."""
        result = analyze_session_tool_call_batching([
            # Early session: all batched
            {"turn_index": 1, "tool_calls_count": 3},
            {"turn_index": 2, "tool_calls_count": 2},
            # Late session: all sequential
            {"turn_index": 3, "tool_calls_count": 1},
            {"turn_index": 4, "tool_calls_count": 1},
        ])

        # Early: 100% batched (2/2)
        # Late: 0% batched (0/2)
        # Improvement: 0 - 100 = -100
        assert result["batching_improvement_score"] == -100.0

    def test_batching_improvement_score_stable(self):
        """Verify stable batching rate (no improvement)."""
        result = analyze_session_tool_call_batching([
            # Early session: 50% batched
            {"turn_index": 1, "tool_calls_count": 1},
            {"turn_index": 2, "tool_calls_count": 2},
            # Late session: 50% batched
            {"turn_index": 3, "tool_calls_count": 1},
            {"turn_index": 4, "tool_calls_count": 2},
        ])

        # Both early and late: 50% batched
        assert result["batching_improvement_score"] == 0.0

    def test_turns_without_tool_calls_ignored(self):
        """Verify turns without tool calls are ignored in batching metrics."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 0,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 2,
                "batchable_calls": 2,
                "batched_calls": 2,
            },
            {
                "turn_index": 3,
                "tool_calls_count": None,
            },
        ])

        assert result["total_turns"] == 3
        assert result["turns_with_tool_calls"] == 1
        assert result["batched_turns"] == 1
        assert result["batching_rate"] == 100.0

    def test_efficiency_score_calculation(self):
        """Verify batching efficiency score calculation."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 8,
                "batchable_calls": 10,
                "batched_calls": 8,
            },
        ])

        # 8/10 * 100 = 80.0
        assert result["batching_efficiency_score"] == 80.0

    def test_efficiency_score_with_zero_batchable(self):
        """Verify efficiency score is 0 when no batchable calls."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "batchable_calls": 0,
                "batched_calls": 0,
            },
        ])

        assert result["batching_efficiency_score"] == 0.0

    def test_average_batch_size_calculation(self):
        """Verify average batch size calculation."""
        result = analyze_session_tool_call_batching([
            {"turn_index": 1, "tool_calls_count": 2},
            {"turn_index": 2, "tool_calls_count": 4},
            {"turn_index": 3, "tool_calls_count": 3},
        ])

        # (2 + 4 + 3) / 3 = 3.0
        assert result["avg_batch_size"] == 3.0

    def test_max_batch_size_tracking(self):
        """Verify maximum batch size tracking."""
        result = analyze_session_tool_call_batching([
            {"turn_index": 1, "tool_calls_count": 2},
            {"turn_index": 2, "tool_calls_count": 5},
            {"turn_index": 3, "tool_calls_count": 3},
        ])

        assert result["max_batch_size"] == 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_call_batching([
            "not a dict",
            {
                "turn_index": 1,
                "tool_calls_count": 2,
            },
        ])

        assert result["total_turns"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": True,
                "batchable_calls": False,
            },
        ])

        assert result["total_tool_calls"] == 0
        assert result["total_batchable_calls"] == 0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 2.0,
                "batchable_calls": 2.0,
                "batched_calls": 2.0,
            },
        ])

        assert result["total_tool_calls"] == 2
        assert result["total_batchable_calls"] == 2
        assert result["total_batched_calls"] == 2

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 2,
                # Missing batchable_calls, batched_calls, etc.
            },
        ])

        assert result["turns_with_tool_calls"] == 1
        assert result["total_tool_calls"] == 2
        assert result["total_batchable_calls"] == 0
        assert result["total_batched_calls"] == 0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 3,
                "tools_called": ["Read", "Read", "Grep"],
                "batchable_calls": 3,
                "batched_calls": 3,
                "is_sequential_independent": False,
            },
            {
                "turn_index": 2,
                "tool_calls_count": 1,
                "tools_called": ["Edit"],
                "batchable_calls": 1,
                "batched_calls": 0,
                "is_sequential_independent": False,
            },
        ])

        assert result["total_turns"] == 2
        assert result["turns_with_tool_calls"] == 2
        assert result["single_call_turns"] == 1
        assert result["batched_turns"] == 1
        assert result["batching_rate"] == 50.0
        assert result["total_tool_calls"] == 4
        assert result["total_batchable_calls"] == 4
        assert result["total_batched_calls"] == 3
        assert result["batching_efficiency_score"] == 75.0
        assert result["avg_batch_size"] == 3.0
        assert result["max_batch_size"] == 3
        assert result["missed_batching_opportunities"] == 0

    def test_edge_case_single_turn_batching_improvement(self):
        """Verify batching improvement with single turn."""
        result = analyze_session_tool_call_batching([
            {"turn_index": 1, "tool_calls_count": 3},
        ])

        # Single turn: goes to early session (before midpoint)
        # Early: 100% batched (1/1), Late: 0% batched (0/0) = 0.0
        # Improvement: 0.0 - 100.0 = -100.0
        # Actually with 1 turn, midpoint is 0, so it goes to late
        # Late: 100% batched (1/1), Early: 0% batched (0/0) = 0.0
        # Improvement: 100.0 - 0.0 = 100.0
        assert result["batching_improvement_score"] == 100.0

    def test_large_batch_size(self):
        """Verify handling of large batch sizes."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 10,
                "batchable_calls": 10,
                "batched_calls": 10,
            },
        ])

        assert result["avg_batch_size"] == 10.0
        assert result["max_batch_size"] == 10

    def test_zero_tool_calls_in_turn(self):
        """Verify handling of explicit zero tool calls."""
        result = analyze_session_tool_call_batching([
            {
                "turn_index": 1,
                "tool_calls_count": 0,
            },
        ])

        assert result["turns_with_tool_calls"] == 0
        assert result["total_tool_calls"] == 0
