"""Tests for session context window usage analyzer."""

import pytest

from synthesis.session_context_window_usage import (
    analyze_session_context_window_usage,
)


class TestAnalyzeSessionContextWindowUsage:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_context_window_usage([])

        assert result["total_turns"] == 0
        assert result["total_input_tokens"] == 0
        assert result["total_output_tokens"] == 0
        assert result["avg_tokens_per_turn"] == 0.0
        assert result["max_tokens_per_turn"] == 0
        assert result["context_window_pressure_score"] == 0.0
        assert result["turns_exceeding_50k"] == 0
        assert result["high_token_turn_rate"] == 0.0
        assert result["efficiency_ratio"] == 0.0
        assert result["final_cumulative_tokens"] == 0
        assert result["budget_remaining"] == 200000
        assert result["is_approaching_limit"] is False

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_context_window_usage(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_context_window_usage("not a list")

    def test_low_token_session(self):
        """Verify session with low token consumption."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 1000,
                "output_tokens": 500,
            },
            {
                "turn_index": 2,
                "input_tokens": 1500,
                "output_tokens": 750,
            },
        ])

        assert result["total_turns"] == 2
        assert result["total_input_tokens"] == 2500
        assert result["total_output_tokens"] == 1250
        assert result["avg_tokens_per_turn"] == 1250.0
        assert result["max_tokens_per_turn"] == 1500
        # 2500 / 200000 * 100 = 1.25
        assert result["context_window_pressure_score"] == 1.25
        assert result["turns_exceeding_50k"] == 0
        assert result["is_approaching_limit"] is False

    def test_high_pressure_session(self):
        """Verify session with high context window pressure."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 100000,
                "output_tokens": 30000,
                "cumulative_input_tokens": 100000,
            },
            {
                "turn_index": 2,
                "input_tokens": 80000,
                "output_tokens": 25000,
                "cumulative_input_tokens": 180000,
            },
        ])

        assert result["total_input_tokens"] == 180000
        assert result["final_cumulative_tokens"] == 180000
        # 180000 / 200000 * 100 = 90.0
        assert result["context_window_pressure_score"] == 90.0
        assert result["budget_remaining"] == 20000
        assert result["is_approaching_limit"] is True

    def test_turns_exceeding_50k_threshold(self):
        """Verify detection of turns exceeding 50k token threshold."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 60000,
            },
            {
                "turn_index": 2,
                "input_tokens": 30000,
            },
            {
                "turn_index": 3,
                "input_tokens": 75000,
            },
        ])

        assert result["turns_exceeding_50k"] == 2
        # 2/3 * 100 = 66.67
        assert result["high_token_turn_rate"] == 66.67

    def test_no_high_token_turns(self):
        """Verify sessions with no high-token turns."""
        result = analyze_session_context_window_usage([
            {"turn_index": 1, "input_tokens": 10000},
            {"turn_index": 2, "input_tokens": 20000},
            {"turn_index": 3, "input_tokens": 15000},
        ])

        assert result["turns_exceeding_50k"] == 0
        assert result["high_token_turn_rate"] == 0.0

    def test_efficiency_ratio_calculation(self):
        """Verify efficiency ratio (output/input) calculation."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": 10000,
                "output_tokens": 3000,
            },
            {
                "input_tokens": 15000,
                "output_tokens": 6000,
            },
        ])

        # Total: 9000 output / 25000 input = 0.36
        assert result["efficiency_ratio"] == 0.36

    def test_zero_efficiency_ratio(self):
        """Verify efficiency ratio is 0 with no input tokens."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": 0,
                "output_tokens": 100,
            },
        ])

        assert result["efficiency_ratio"] == 0.0

    def test_cumulative_tokens_tracking(self):
        """Verify cumulative token tracking."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 5000,
                "cumulative_input_tokens": 5000,
            },
            {
                "turn_index": 2,
                "input_tokens": 7000,
                "cumulative_input_tokens": 12000,
            },
            {
                "turn_index": 3,
                "input_tokens": 3000,
                "cumulative_input_tokens": 15000,
            },
        ])

        assert result["final_cumulative_tokens"] == 15000
        assert result["budget_remaining"] == 185000

    def test_cumulative_fallback_to_total(self):
        """Verify cumulative falls back to total when not provided."""
        result = analyze_session_context_window_usage([
            {"turn_index": 1, "input_tokens": 5000},
            {"turn_index": 2, "input_tokens": 7000},
        ])

        # No cumulative provided, should use total
        assert result["final_cumulative_tokens"] == 12000

    def test_average_tokens_per_turn(self):
        """Verify average tokens per turn calculation."""
        result = analyze_session_context_window_usage([
            {"input_tokens": 10000},
            {"input_tokens": 20000},
            {"input_tokens": 30000},
        ])

        # (10000 + 20000 + 30000) / 3 = 20000
        assert result["avg_tokens_per_turn"] == 20000.0

    def test_max_tokens_per_turn(self):
        """Verify maximum tokens per turn tracking."""
        result = analyze_session_context_window_usage([
            {"input_tokens": 10000},
            {"input_tokens": 50000},
            {"input_tokens": 25000},
        ])

        assert result["max_tokens_per_turn"] == 50000

    def test_approaching_limit_threshold(self):
        """Verify approaching limit detection at 80% threshold."""
        # Exactly 80% - should not trigger
        result1 = analyze_session_context_window_usage([
            {
                "input_tokens": 160000,
                "cumulative_input_tokens": 160000,
            },
        ])
        assert result1["is_approaching_limit"] is False

        # Above 80% - should trigger
        result2 = analyze_session_context_window_usage([
            {
                "input_tokens": 161000,
                "cumulative_input_tokens": 161000,
            },
        ])
        assert result2["is_approaching_limit"] is True

    def test_exceeds_threshold_flag(self):
        """Verify explicit exceeds_threshold flag."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 30000,
                "exceeds_threshold": True,
            },
        ])

        # Input is 30k but flag says it exceeds
        assert result["turns_exceeding_50k"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_context_window_usage([
            "not a dict",
            {
                "turn_index": 1,
                "input_tokens": 1000,
            },
        ])

        assert result["total_turns"] == 1

    def test_boolean_values_not_extracted_as_numbers(self):
        """Verify boolean values are not extracted as numbers."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": True,
                "output_tokens": False,
            },
        ])

        assert result["total_input_tokens"] == 0
        assert result["total_output_tokens"] == 0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": 1000.5,
                "output_tokens": 500.75,
            },
        ])

        assert result["total_input_tokens"] == 1000
        assert result["total_output_tokens"] == 500

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                # Missing tokens fields
            },
        ])

        assert result["total_turns"] == 1
        assert result["total_input_tokens"] == 0
        assert result["total_output_tokens"] == 0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_context_window_usage([
            {
                "turn_index": 1,
                "input_tokens": 25000,
                "output_tokens": 8000,
                "cumulative_input_tokens": 25000,
                "exceeds_threshold": False,
            },
            {
                "turn_index": 2,
                "input_tokens": 60000,
                "output_tokens": 20000,
                "cumulative_input_tokens": 85000,
                "exceeds_threshold": True,
            },
        ])

        assert result["total_turns"] == 2
        assert result["total_input_tokens"] == 85000
        assert result["total_output_tokens"] == 28000
        assert result["final_cumulative_tokens"] == 85000
        assert result["turns_exceeding_50k"] == 1
        assert result["high_token_turn_rate"] == 50.0
        # 28000 / 85000 = 0.329
        assert result["efficiency_ratio"] == 0.329
        # 85000 / 200000 = 42.5
        assert result["context_window_pressure_score"] == 42.5
        assert result["is_approaching_limit"] is False

    def test_budget_remaining_calculation(self):
        """Verify budget remaining calculation."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": 50000,
                "cumulative_input_tokens": 50000,
            },
        ])

        assert result["budget_remaining"] == 150000

    def test_budget_remaining_at_zero(self):
        """Verify budget remaining doesn't go negative."""
        result = analyze_session_context_window_usage([
            {
                "input_tokens": 250000,
                "cumulative_input_tokens": 250000,
            },
        ])

        assert result["budget_remaining"] == 0
        assert result["is_approaching_limit"] is True

    def test_edge_case_exactly_50k(self):
        """Verify edge case of exactly 50k tokens."""
        result = analyze_session_context_window_usage([
            {"input_tokens": 50000},
        ])

        # Exactly 50k should not exceed threshold (>50k)
        assert result["turns_exceeding_50k"] == 0

    def test_edge_case_50001_tokens(self):
        """Verify edge case of 50001 tokens exceeds threshold."""
        result = analyze_session_context_window_usage([
            {"input_tokens": 50001},
        ])

        assert result["turns_exceeding_50k"] == 1

    def test_multiple_turns_mixed_scenarios(self):
        """Verify multiple turns with varied token usage."""
        result = analyze_session_context_window_usage([
            # Low token turn
            {
                "turn_index": 1,
                "input_tokens": 5000,
                "output_tokens": 2000,
            },
            # High token turn
            {
                "turn_index": 2,
                "input_tokens": 75000,
                "output_tokens": 25000,
            },
            # Medium token turn
            {
                "turn_index": 3,
                "input_tokens": 30000,
                "output_tokens": 10000,
            },
        ])

        assert result["total_turns"] == 3
        assert result["total_input_tokens"] == 110000
        assert result["avg_tokens_per_turn"] == 36666.67
        assert result["max_tokens_per_turn"] == 75000
        assert result["turns_exceeding_50k"] == 1
