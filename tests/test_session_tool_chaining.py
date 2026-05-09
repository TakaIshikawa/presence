"""Tests for session tool chaining depth analyzer."""

import pytest

from synthesis.session_tool_chaining import analyze_session_tool_chaining


class TestAnalyzeSessionToolChaining:
    """Test main analyzer function."""

    def test_empty_turns_returns_zeroed_metrics(self):
        """Verify empty turn list returns zero metrics."""
        result = analyze_session_tool_chaining([])

        assert result["total_turns"] == 0
        assert result["total_tool_calls"] == 0
        assert result["max_chain_depth"] == 0
        assert result["avg_chain_depth"] == 0.0
        assert result["common_chain_patterns"] == []
        assert result["parallel_vs_sequential_ratio"] == 0.0
        assert result["chain_efficiency_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_chaining(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_chaining("not a list")

    def test_single_tool_call_simple_chain(self):
        """Verify single tool call creates chain of depth 1."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            }
        ])

        assert result["total_tool_calls"] == 1
        assert result["max_chain_depth"] == 1
        assert result["sequential_call_count"] == 1

    def test_sequential_tool_calls_chain_depth(self):
        """Verify sequential tool calls create deeper chains."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Grep"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Edit"}],
            }
        ])

        # Three sequential calls = chain depth of 3
        assert result["max_chain_depth"] == 3
        assert result["avg_chain_depth"] == 3.0
        assert result["sequential_call_count"] == 3

    def test_parallel_tool_calls_counted(self):
        """Verify parallel tool calls are counted correctly."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                ],
                "parallel": True,
            }
        ])

        assert result["parallel_call_count"] == 2
        assert result["sequential_call_count"] == 0

    def test_parallel_inferred_from_multiple_tools(self):
        """Verify parallel is inferred when multiple tools in one turn."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [
                    {"tool_name": "Read"},
                    {"tool_name": "Grep"},
                    {"tool_name": "Glob"},
                ],
            }
        ])

        # Multiple tools in one turn implies parallel
        assert result["parallel_call_count"] == 3

    def test_common_chain_pattern_identified(self):
        """Verify common tool chain patterns are identified."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Grep"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [{"tool_name": "Edit"}],
            }
        ])

        # Should capture Grep->Read->Edit pattern
        assert len(result["common_chain_patterns"]) > 0
        pattern = result["common_chain_patterns"][0]["pattern"]
        assert "Grep" in pattern
        assert "Read" in pattern
        assert "Edit" in pattern

    def test_chain_broken_by_no_tools(self):
        """Verify chain is broken by turn with no tool calls."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [],
            },
            {
                "turn_index": 4,
                "tool_calls": [{"tool_name": "Bash"}],
            }
        ])

        # Two separate chains: [Read, Edit] and [Bash]
        assert result["max_chain_depth"] == 2

    def test_chain_efficiency_all_successful(self):
        """Verify chain efficiency score for all successful chains."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "success": True}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit", "success": True}],
            }
        ])

        assert result["chain_efficiency_score"] == 1.0

    def test_chain_efficiency_with_failures(self):
        """Verify chain efficiency score with some failures."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read", "success": False}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit", "success": True}],
            }
        ])

        # One failed chain
        assert result["chain_efficiency_score"] < 1.0

    def test_parallel_vs_sequential_ratio(self):
        """Verify parallel vs sequential ratio calculation."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit"}],
            },
            {
                "turn_index": 3,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Glob"},
                ],
                "parallel": True,
            }
        ])

        # 2 sequential + 2 parallel = 50% parallel
        assert result["parallel_vs_sequential_ratio"] == 50.0

    def test_string_tool_calls_handled(self):
        """Verify tool calls can be strings instead of dicts."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": ["Read", "Edit"],
            }
        ])

        assert result["total_tool_calls"] == 2

    def test_malformed_turn_skipped(self):
        """Verify non-dict turns are skipped."""
        result = analyze_session_tool_chaining([
            "not a dict",
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}],
            }
        ])

        assert result["total_turns"] == 1

    def test_missing_tool_calls_handled(self):
        """Verify turns without tool_calls are handled."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
            }
        ])

        assert result["total_turns"] == 1
        assert result["total_tool_calls"] == 0

    def test_non_list_tool_calls_skipped(self):
        """Verify non-list tool_calls values are skipped."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": "not a list",
            }
        ])

        assert result["total_tool_calls"] == 0

    def test_empty_tool_name_skipped(self):
        """Verify tool calls with empty names are skipped."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": ""}],
            }
        ])

        assert result["total_tool_calls"] == 0

    def test_max_chain_depth_multiple_chains(self):
        """Verify max chain depth across multiple chains."""
        result = analyze_session_tool_chaining([
            # Chain 1: depth 2
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Edit"}]},
            {"turn_index": 3, "tool_calls": []},
            # Chain 2: depth 4
            {"turn_index": 4, "tool_calls": [{"tool_name": "Grep"}]},
            {"turn_index": 5, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 6, "tool_calls": [{"tool_name": "Edit"}]},
            {"turn_index": 7, "tool_calls": [{"tool_name": "Bash"}]},
        ])

        assert result["max_chain_depth"] == 4

    def test_avg_chain_depth_calculation(self):
        """Verify average chain depth calculation."""
        result = analyze_session_tool_chaining([
            # Chain 1: depth 2
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Edit"}]},
            {"turn_index": 3, "tool_calls": []},
            # Chain 2: depth 3
            {"turn_index": 4, "tool_calls": [{"tool_name": "Grep"}]},
            {"turn_index": 5, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 6, "tool_calls": [{"tool_name": "Bash"}]},
        ])

        # Average of [2, 3] = 2.5
        assert result["avg_chain_depth"] == 2.5

    def test_pattern_limited_to_10(self):
        """Verify common patterns limited to top 10."""
        # Create many different patterns
        turns = []
        for i in range(20):
            turns.extend([
                {"turn_index": i * 2, "tool_calls": [{"tool_name": f"Tool{i}"}]},
                {"turn_index": i * 2 + 1, "tool_calls": [{"tool_name": "End"}]},
                {"turn_index": i * 2 + 2, "tool_calls": []},
            ])

        result = analyze_session_tool_chaining(turns)

        assert len(result["common_chain_patterns"]) <= 10

    def test_long_chain_pattern_truncated(self):
        """Verify long chain patterns are truncated to 5 tools."""
        result = analyze_session_tool_chaining([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Tool1"}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Tool2"}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Tool3"}]},
            {"turn_index": 4, "tool_calls": [{"tool_name": "Tool4"}]},
            {"turn_index": 5, "tool_calls": [{"tool_name": "Tool5"}]},
            {"turn_index": 6, "tool_calls": [{"tool_name": "Tool6"}]},
            {"turn_index": 7, "tool_calls": [{"tool_name": "Tool7"}]},
        ])

        # Pattern should be truncated to last 5 tools
        pattern = result["common_chain_patterns"][0]["pattern"]
        assert len(pattern) <= 5

    def test_all_parallel_calls(self):
        """Verify handling when all calls are parallel."""
        result = analyze_session_tool_chaining([
            {
                "turn_index": 1,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Grep"}],
                "parallel": True,
            },
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Edit"}, {"tool_name": "Write"}],
                "parallel": True,
            }
        ])

        assert result["parallel_call_count"] == 4
        assert result["sequential_call_count"] == 0
        assert result["parallel_vs_sequential_ratio"] == 100.0

    def test_all_sequential_calls(self):
        """Verify handling when all calls are sequential."""
        result = analyze_session_tool_chaining([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Edit"}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Bash"}]},
        ])

        assert result["parallel_call_count"] == 0
        assert result["sequential_call_count"] == 3
        assert result["parallel_vs_sequential_ratio"] == 0.0

    def test_mixed_parallel_and_sequential(self):
        """Verify handling of mixed parallel and sequential calls."""
        result = analyze_session_tool_chaining([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Grep"}]},
            {
                "turn_index": 2,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Edit"}],
            },
            {"turn_index": 3, "tool_calls": [{"tool_name": "Bash"}]},
        ])

        # Turn 2 has 2 tools = parallel
        assert result["parallel_call_count"] == 2
        assert result["sequential_call_count"] == 2

    def test_chain_efficiency_no_chains(self):
        """Verify chain efficiency when no chains exist."""
        result = analyze_session_tool_chaining([
            {"turn_index": 1, "tool_calls": []},
        ])

        assert result["chain_efficiency_score"] == 0.0

    def test_realistic_session_pattern(self):
        """Verify realistic session with mixed patterns."""
        result = analyze_session_tool_chaining([
            {"turn_index": 1, "tool_calls": [{"tool_name": "Grep"}]},
            {"turn_index": 2, "tool_calls": [{"tool_name": "Read"}]},
            {"turn_index": 3, "tool_calls": [{"tool_name": "Edit"}]},
            {"turn_index": 4, "tool_calls": [{"tool_name": "Bash", "success": True}]},
            {
                "turn_index": 5,
                "tool_calls": [{"tool_name": "Read"}, {"tool_name": "Read"}],
                "parallel": True,
            },
            {"turn_index": 6, "tool_calls": [{"tool_name": "Edit"}]},
        ])

        assert result["total_tool_calls"] == 7
        assert result["max_chain_depth"] >= 2
        assert len(result["common_chain_patterns"]) > 0
