"""Tests for session parallel tool call efficiency analyzer."""

import pytest

from synthesis.session_parallel_tool_efficiency import analyze_session_parallel_tool_efficiency


class TestAnalyzeSessionParallelToolEfficiency:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_parallel_tool_efficiency([])

        assert result["total_turns"] == 0
        assert result["turns_with_tool_calls"] == 0
        assert result["parallel_turns"] == 0
        assert result["sequential_turns"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["total_parallel_groups"] == 0
        assert result["average_group_size"] == 0.0
        assert result["max_group_size"] == 0
        assert result["common_parallel_patterns"] == []
        assert result["missed_opportunities"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_parallel_tool_efficiency(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_parallel_tool_efficiency("not a list")

    def test_single_tool_call_is_sequential(self):
        """Verify single tool call is counted as sequential."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None}
                ]
            }
        ])

        assert result["total_turns"] == 1
        assert result["turns_with_tool_calls"] == 1
        assert result["parallel_turns"] == 0
        assert result["sequential_turns"] == 1
        assert result["parallelization_rate"] == 0.0

    def test_parallel_tool_calls_detected(self):
        """Verify parallel tool calls are detected correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            }
        ])

        assert result["parallel_turns"] == 1
        assert result["sequential_turns"] == 0
        assert result["parallelization_rate"] == 100.0
        assert result["total_parallel_groups"] == 1
        assert result["average_group_size"] == 2.0
        assert result["max_group_size"] == 2

    def test_multiple_parallel_groups_in_single_turn(self):
        """Verify multiple parallel groups in one turn."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Edit", "parallel_group_id": 1},
                    {"tool_name": "Edit", "parallel_group_id": 1},
                ]
            }
        ])

        assert result["parallel_turns"] == 1
        assert result["total_parallel_groups"] == 2
        assert result["average_group_size"] == 2.0

    def test_sequential_tool_calls_no_parallel(self):
        """Verify sequential tool calls without parallel groups."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None},
                    {"tool_name": "Edit", "parallel_group_id": None},
                ]
            }
        ])

        assert result["parallel_turns"] == 0
        assert result["sequential_turns"] == 1
        assert result["total_parallel_groups"] == 0

    def test_missed_opportunity_detected(self):
        """Verify missed parallelization opportunity is detected."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None},
                    {"tool_name": "Read", "parallel_group_id": None},
                ]
            }
        ])

        assert result["missed_opportunities"] == 1
        assert result["sequential_turns"] == 1

    def test_common_parallel_patterns_tracked(self):
        """Verify common parallel patterns are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                ]
            }
        ])

        assert len(result["common_parallel_patterns"]) == 1
        pattern = result["common_parallel_patterns"][0]
        assert set(pattern["tools"]) == {"Read", "Grep"}
        assert pattern["count"] == 2

    def test_mixed_parallel_and_sequential_turns(self):
        """Verify sessions with mixed parallel and sequential turns."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Edit", "parallel_group_id": None},
                ]
            }
        ])

        assert result["parallel_turns"] == 1
        assert result["sequential_turns"] == 1
        assert result["parallelization_rate"] == 50.0

    def test_large_parallel_group(self):
        """Verify large parallel groups are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            }
        ])

        assert result["max_group_size"] == 5
        assert result["average_group_size"] == 5.0

    def test_turns_without_tool_calls_ignored(self):
        """Verify turns without tool calls are excluded from metrics."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": []
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None}
                ]
            }
        ])

        assert result["total_turns"] == 2
        assert result["turns_with_tool_calls"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_parallel_tool_efficiency([
            "not a dict",
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None}
                ]
            }
        ])

        assert result["total_turns"] == 2
        assert result["turns_with_tool_calls"] == 1

    def test_backward_compat_tool_call_count(self):
        """Verify backward compatibility with tool_call_count format."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_call_count": 3,
                "is_parallel": True,
            }
        ])

        assert result["parallel_turns"] == 1
        assert result["total_parallel_groups"] == 1
        assert result["average_group_size"] == 3.0

    def test_backward_compat_sequential_calls(self):
        """Verify backward compatibility for sequential calls."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_call_count": 3,
                "is_parallel": False,
            }
        ])

        assert result["parallel_turns"] == 0
        assert result["sequential_turns"] == 1

    def test_parallelizable_tools_detected(self):
        """Verify parallelizable tool patterns are detected."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Grep", "parallel_group_id": None},
                    {"tool_name": "Glob", "parallel_group_id": None},
                ]
            }
        ])

        assert result["missed_opportunities"] == 1

    def test_non_parallelizable_tools_not_flagged(self):
        """Verify non-parallelizable tool sequences aren't flagged."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None},
                    {"tool_name": "Edit", "parallel_group_id": None},
                ]
            }
        ])

        assert result["missed_opportunities"] == 0

    def test_multiple_distinct_parallel_patterns(self):
        """Verify multiple different parallel patterns are tracked."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Glob", "parallel_group_id": 0},
                ]
            }
        ])

        assert len(result["common_parallel_patterns"]) == 2

    def test_pattern_normalization(self):
        """Verify parallel patterns are normalized (sorted)."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                ]
            }
        ])

        # Both should be counted as same pattern
        assert len(result["common_parallel_patterns"]) == 1
        assert result["common_parallel_patterns"][0]["count"] == 2

    def test_fully_parallel_session(self):
        """Verify session with all parallel tool calls."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                ]
            }
        ])

        assert result["parallelization_rate"] == 100.0
        assert result["sequential_turns"] == 0

    def test_fully_sequential_session(self):
        """Verify session with only sequential tool calls."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": None},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Edit", "parallel_group_id": None},
                ]
            }
        ])

        assert result["parallelization_rate"] == 0.0
        assert result["parallel_turns"] == 0

    def test_average_group_size_calculation(self):
        """Verify average group size is calculated correctly."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "Read", "parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            },
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                    {"tool_name": "Grep", "parallel_group_id": 0},
                ]
            }
        ])

        # (2 + 4) / 2 = 3.0
        assert result["average_group_size"] == 3.0

    def test_common_patterns_limited_to_five(self):
        """Verify common patterns list is capped at 5."""
        records = []
        for i in range(10):
            records.append({
                "turn_index": i,
                "tool_calls": [
                    {"tool_name": f"Tool{i}", "parallel_group_id": 0},
                    {"tool_name": f"Helper{i}", "parallel_group_id": 0},
                ]
            })

        result = analyze_session_parallel_tool_efficiency(records)

        assert len(result["common_parallel_patterns"]) <= 5

    def test_webfetch_parallelizable(self):
        """Verify WebFetch is recognized as parallelizable."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "WebFetch", "parallel_group_id": None},
                    {"tool_name": "WebFetch", "parallel_group_id": None},
                ]
            }
        ])

        assert result["missed_opportunities"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"tool_name": "READ", "parallel_group_id": None},
                    {"tool_name": "read", "parallel_group_id": None},
                ]
            }
        ])

        assert result["missed_opportunities"] == 1

    def test_tool_calls_without_tool_name_ignored(self):
        """Verify tool calls without tool_name are handled gracefully."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": [
                    {"parallel_group_id": 0},
                    {"tool_name": "Read", "parallel_group_id": 0},
                ]
            }
        ])

        # Should still process the valid tool call
        assert result["turns_with_tool_calls"] == 1

    def test_zero_denominator_handled_gracefully(self):
        """Verify zero denominator in percentage calculation."""
        result = analyze_session_parallel_tool_efficiency([
            {
                "turn_index": 0,
                "tool_calls": []
            }
        ])

        assert result["parallelization_rate"] == 0.0
