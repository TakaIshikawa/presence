"""Tests for session grep targeting precision analyzer."""

import pytest

from synthesis.session_grep_targeting_precision import analyze_session_grep_targeting_precision


class TestAnalyzeSessionGrepTargetingPrecision:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_grep_targeting_precision([])

        assert result["total_tool_calls"] == 0
        assert result["grep_call_count"] == 0
        assert result["avg_results_per_grep"] == 0.0
        assert result["precision_score"] == 0.0
        assert result["pattern_refinement_chains"] == 0
        assert result["avg_chain_length"] == 0.0
        assert result["context_flag_usage_rate"] == 0.0
        assert result["context_a_usage"] == 0
        assert result["context_b_usage"] == 0
        assert result["context_c_usage"] == 0
        assert result["high_precision_searches"] == 0
        assert result["low_precision_searches"] == 0
        assert result["common_patterns"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_grep_targeting_precision(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_grep_targeting_precision("not a list")

    def test_single_grep_call_tracked(self):
        """Verify single grep call is tracked correctly."""
        result = analyze_session_grep_targeting_precision([
            {
                "tool_name": "Grep",
                "pattern": "error",
                "result_count": 10,
                "turn_index": 0,
            }
        ])

        assert result["grep_call_count"] == 1
        assert result["total_tool_calls"] == 1
        assert len(result["common_patterns"]) == 1
        assert result["common_patterns"][0]["pattern"] == "error"

    def test_high_precision_search_detected(self):
        """Verify high precision searches (< 20 results) are detected."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "result_count": 5, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "result_count": 15, "turn_index": 1},
        ])

        assert result["high_precision_searches"] == 2
        assert result["precision_score"] > 90.0

    def test_low_precision_search_detected(self):
        """Verify low precision searches (> 100 results) are detected."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": ".*", "result_count": 500, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "a", "result_count": 200, "turn_index": 1},
        ])

        assert result["low_precision_searches"] == 2
        assert result["precision_score"] < 35.0

    def test_avg_results_per_grep_calculation(self):
        """Verify average results per grep is calculated correctly."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "result_count": 10, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "result_count": 20, "turn_index": 1},
            {"tool_name": "Grep", "pattern": "info", "result_count": 30, "turn_index": 2},
        ])

        # (10 + 20 + 30) / 3 = 20.0
        assert result["avg_results_per_grep"] == 20.0

    def test_precision_score_calculation(self):
        """Verify precision score is calculated based on result counts."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "specific", "result_count": 5, "turn_index": 0},
        ])

        # 5 results should have very high precision (> 95)
        assert result["precision_score"] > 95.0

    def test_pattern_refinement_chain_detected(self):
        """Verify pattern refinement chains are detected."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error.*auth", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "error.*auth.*failed", "turn_index": 2},
        ])

        # 3 sequential refinements = 1 chain of length 3
        assert result["pattern_refinement_chains"] == 1
        assert result["avg_chain_length"] == 3.0

    def test_pattern_refinement_not_detected_for_different_patterns(self):
        """Verify no refinement chain for unrelated patterns."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "info", "turn_index": 2},
        ])

        # No refinement chains
        assert result["pattern_refinement_chains"] == 0

    def test_multiple_refinement_chains(self):
        """Verify multiple refinement chains are tracked."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error.*auth", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 2},
            {"tool_name": "Grep", "pattern": "warning.*timeout", "turn_index": 3},
        ])

        # 2 chains: error->error.*auth (length 2), warning->warning.*timeout (length 2)
        assert result["pattern_refinement_chains"] == 2
        assert result["avg_chain_length"] == 2.0

    def test_context_flag_a_usage(self):
        """Verify -A flag usage is tracked."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": {"A": 5}, "turn_index": 0},
        ])

        assert result["context_a_usage"] == 1
        assert result["context_flag_usage_rate"] == 100.0

    def test_context_flag_b_usage(self):
        """Verify -B flag usage is tracked."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": {"B": 3}, "turn_index": 0},
        ])

        assert result["context_b_usage"] == 1

    def test_context_flag_c_usage(self):
        """Verify -C flag usage is tracked."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": {"C": 2}, "turn_index": 0},
        ])

        assert result["context_c_usage"] == 1

    def test_multiple_context_flags_in_single_call(self):
        """Verify multiple context flags in one grep call."""
        result = analyze_session_grep_targeting_precision([
            {
                "tool_name": "Grep",
                "pattern": "error",
                "context_flags": {"A": 3, "B": 2},
                "turn_index": 0,
            },
        ])

        assert result["context_a_usage"] == 1
        assert result["context_b_usage"] == 1
        assert result["context_flag_usage_rate"] == 200.0  # 2 flags / 1 grep

    def test_context_flag_usage_rate_calculation(self):
        """Verify context flag usage rate calculation."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": {"A": 3}, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "info", "turn_index": 2},
        ])

        # 1 flag usage / 3 greps = 33.33%
        assert result["context_flag_usage_rate"] == 33.33

    def test_common_patterns_sorted_by_frequency(self):
        """Verify common patterns are sorted by usage count."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "error", "turn_index": 2},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 3},
        ])

        assert len(result["common_patterns"]) == 2
        assert result["common_patterns"][0]["pattern"] == "error"
        assert result["common_patterns"][0]["count"] == 3
        assert result["common_patterns"][1]["pattern"] == "warning"
        assert result["common_patterns"][1]["count"] == 1

    def test_common_patterns_limited_to_five(self):
        """Verify common patterns list is capped at 5."""
        records = [
            {"tool_name": "Grep", "pattern": f"pattern{i}", "turn_index": i}
            for i in range(10)
        ]

        result = analyze_session_grep_targeting_precision(records)

        assert len(result["common_patterns"]) == 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_grep_targeting_precision([
            "not a dict",
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["grep_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_grep_targeting_precision([
            {"pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["grep_call_count"] == 1

    def test_grep_without_pattern_handled(self):
        """Verify grep calls without pattern are handled gracefully."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "turn_index": 0},
        ])

        assert result["grep_call_count"] == 1
        assert result["common_patterns"] == []

    def test_grep_without_result_count_handled(self):
        """Verify grep calls without result_count are handled gracefully."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
        ])

        assert result["grep_call_count"] == 1
        assert result["avg_results_per_grep"] == 0.0
        assert result["precision_score"] == 0.0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 2},
        ])

        assert result["total_tool_calls"] == 3
        assert result["grep_call_count"] == 2

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "GREP", "pattern": "error", "turn_index": 0},
            {"tool_name": "grep", "pattern": "warning", "turn_index": 1},
        ])

        assert result["grep_call_count"] == 2

    def test_whitespace_handling_in_patterns(self):
        """Verify whitespace in patterns is stripped."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "  error  ", "turn_index": 0},
        ])

        assert result["common_patterns"][0]["pattern"] == "error"

    def test_zero_results_high_precision(self):
        """Verify zero results is considered high precision."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "nonexistent", "result_count": 0, "turn_index": 0},
        ])

        assert result["precision_score"] == 100.0

    def test_very_broad_search_low_precision(self):
        """Verify very broad searches have low precision."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": ".*", "result_count": 10000, "turn_index": 0},
        ])

        assert result["precision_score"] < 10.0

    def test_optimal_grep_usage_pattern(self):
        """Verify optimal usage pattern has high metrics."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "result_count": 8, "context_flags": {"C": 3}, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error.*auth", "result_count": 3, "context_flags": {"C": 3}, "turn_index": 1},
        ])

        # High precision, refinement chain, context usage
        assert result["precision_score"] > 95.0
        assert result["pattern_refinement_chains"] == 1
        assert result["context_flag_usage_rate"] == 100.0

    def test_anti_pattern_broad_unfocused_search(self):
        """Verify anti-pattern of broad unfocused searches."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": ".", "result_count": 500, "turn_index": 0},
            {"tool_name": "Grep", "pattern": ".*", "result_count": 1000, "turn_index": 1},
        ])

        # Low precision, no refinement, no context usage
        assert result["precision_score"] < 20.0
        assert result["low_precision_searches"] == 2
        assert result["context_flag_usage_rate"] == 0.0

    def test_pattern_refinement_substring_containment(self):
        """Verify refinement detected via substring containment."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "function", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "function\\s+test", "turn_index": 1},
        ])

        assert result["pattern_refinement_chains"] == 1

    def test_pattern_refinement_reverse_containment(self):
        """Verify refinement detected via reverse containment (narrowing)."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error.*authentication", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error", "turn_index": 1},
        ])

        assert result["pattern_refinement_chains"] == 1

    def test_pattern_refinement_common_substring(self):
        """Verify refinement detected via common substring."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "authentication_error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "authentication_failed", "turn_index": 1},
        ])

        # Both share "authentication_" (> 50% overlap)
        assert result["pattern_refinement_chains"] == 1

    def test_context_flags_alternative_format(self):
        """Verify context flags with dash prefix are recognized."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": {"-A": 3}, "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "context_flags": {"-B": 2}, "turn_index": 1},
            {"tool_name": "Grep", "pattern": "info", "context_flags": {"-C": 1}, "turn_index": 2},
        ])

        assert result["context_a_usage"] == 1
        assert result["context_b_usage"] == 1
        assert result["context_c_usage"] == 1

    def test_context_flags_non_dict_ignored(self):
        """Verify non-dict context_flags are handled gracefully."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "context_flags": "not a dict", "turn_index": 0},
        ])

        assert result["context_flag_usage_rate"] == 0.0

    def test_precision_score_ranges(self):
        """Verify precision scores for different result count ranges."""
        test_cases = [
            (5, 95.0, 100.0),      # 1-10 results
            (15, 85.0, 95.0),      # 11-20 results
            (30, 70.0, 85.0),      # 21-50 results
            (75, 50.0, 70.0),      # 51-100 results
            (200, 20.0, 50.0),     # 101-500 results
            (1000, 0.0, 20.0),     # 500+ results
        ]

        for count, min_prec, max_prec in test_cases:
            result = analyze_session_grep_targeting_precision([
                {"tool_name": "Grep", "pattern": "test", "result_count": count, "turn_index": 0},
            ])
            assert min_prec <= result["precision_score"] <= max_prec, \
                f"Count {count} should have precision between {min_prec} and {max_prec}, got {result['precision_score']}"

    def test_zero_denominator_in_percentages(self):
        """Verify zero denominator in percentage calculations."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 0},
        ])

        # No grep calls
        assert result["context_flag_usage_rate"] == 0.0

    def test_single_pattern_no_refinement_chain(self):
        """Verify single pattern does not create refinement chain."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
        ])

        assert result["pattern_refinement_chains"] == 0

    def test_empty_pattern_handled(self):
        """Verify empty patterns are handled gracefully."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "   ", "turn_index": 1},
        ])

        assert result["grep_call_count"] == 2
        assert result["common_patterns"] == []

    def test_refinement_chain_reset_on_unrelated_pattern(self):
        """Verify refinement chain resets on unrelated pattern."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "error.*auth", "turn_index": 1},
            {"tool_name": "Grep", "pattern": "warning", "turn_index": 2},
            {"tool_name": "Grep", "pattern": "warning.*timeout", "turn_index": 3},
        ])

        # Two chains: error->error.*auth, warning->warning.*timeout
        assert result["pattern_refinement_chains"] == 2

    def test_result_count_must_be_integer(self):
        """Verify non-integer result_count is ignored."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "result_count": "not a number", "turn_index": 0},
            {"tool_name": "Grep", "pattern": "warning", "result_count": 10, "turn_index": 1},
        ])

        # Only second grep has valid result count
        assert result["avg_results_per_grep"] == 10.0

    def test_boolean_result_count_ignored(self):
        """Verify boolean result_count is ignored."""
        result = analyze_session_grep_targeting_precision([
            {"tool_name": "Grep", "pattern": "error", "result_count": True, "turn_index": 0},
        ])

        assert result["avg_results_per_grep"] == 0.0
