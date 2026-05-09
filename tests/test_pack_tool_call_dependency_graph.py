"""Tests for pack tool call dependency graph analyzer."""

import pytest

from synthesis.pack_tool_call_dependency_graph import (
    analyze_pack_tool_call_dependency_graph,
    _percentage,
    _average,
    _summarize_chain_lengths,
)


class TestAnalyzePackToolCallDependencyGraph:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_tool_call_dependency_graph([])

        assert result["total_sessions"] == 0
        assert result["total_tool_calls"] == 0
        assert result["independent_call_count"] == 0
        assert result["dependent_call_count"] == 0
        assert result["independent_call_ratio"] == 0.0
        assert result["max_dependency_depth"] == 0
        assert result["avg_dependency_depth"] == 0.0
        assert result["circular_dependency_count"] == 0
        assert result["redundant_read_count"] == 0
        assert result["redundant_read_files"] == []
        assert result["parallelization_efficiency"] == 0.0
        assert result["cross_session_file_overlap"] == 0
        assert result["dependency_chain_lengths"]["short_chains"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_tool_call_dependency_graph(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_tool_call_dependency_graph("not a list")

    def test_single_session_independent_reads(self):
        """Verify single session with independent reads."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Grep", "pattern": "error"},
                ]
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_tool_calls"] == 3
        assert result["independent_call_count"] == 3
        assert result["dependent_call_count"] == 0
        assert result["independent_call_ratio"] == 100.0

    def test_single_session_with_dependencies(self):
        """Verify single session with dependencies."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Edit", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "a.py"},
                ]
            }
        ])

        assert result["total_tool_calls"] == 3
        assert result["independent_call_count"] == 1  # First read
        assert result["dependent_call_count"] == 2  # Edit and second read
        assert result["dependency_chain_lengths"]["short_chains"] == 1

    def test_multiple_sessions_independent(self):
        """Verify multiple sessions with independent calls."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Grep", "pattern": "test"},
                    {"tool_name": "Glob", "pattern": "*.py"},
                ]
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_tool_calls"] == 4
        assert result["independent_call_count"] == 4
        assert result["independent_call_ratio"] == 100.0

    def test_redundant_read_detection(self):
        """Verify redundant reads across sessions are detected."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
        ])

        assert result["redundant_read_count"] == 1
        assert "main.py" in result["redundant_read_files"]

    def test_no_redundant_read_same_session(self):
        """Verify redundant reads within same session not counted."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
        ])

        # Both reads in same session, not redundant across sessions
        assert result["redundant_read_count"] == 0

    def test_cross_session_file_overlap(self):
        """Verify cross-session file overlap detection."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Edit", "file_path": "b.py"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Write", "file_path": "c.py"},
                ]
            },
        ])

        # a.py accessed by both sessions
        assert result["cross_session_file_overlap"] >= 1

    def test_dependency_depth_calculation(self):
        """Verify dependency depth calculation."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Edit", "file_path": "a.py"},
                    {"tool_name": "Edit", "file_path": "b.py"},
                    {"tool_name": "Bash", "command": "pytest"},
                ]
            }
        ])

        # 3 dependent calls (Edit, Edit, Bash)
        assert result["max_dependency_depth"] == 3
        assert result["avg_dependency_depth"] == 3.0

    def test_circular_dependency_detection(self):
        """Verify circular dependency detection."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Write", "file_path": "main.py"},
                ]
            },
            {
                "session_id": "session3",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
        ])

        # Read-Write-Read pattern on main.py
        assert result["circular_dependency_count"] >= 1

    def test_parallelization_efficiency_independent_calls(self):
        """Verify parallelization efficiency for independent calls."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "a.py"},
                    {"tool_name": "Read", "file_path": "b.py"},
                    {"tool_name": "Read", "file_path": "c.py"},
                ]
            }
        ])

        # Has independent calls, should have non-zero efficiency
        assert result["parallelization_efficiency"] > 0

    def test_dependency_chain_lengths_distribution(self):
        """Verify dependency chain length distribution."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "a.py"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "b.py"},
                    {"tool_name": "Edit", "file_path": "c.py"},
                    {"tool_name": "Edit", "file_path": "d.py"},
                    {"tool_name": "Edit", "file_path": "e.py"},
                ]
            },
            {
                "session_id": "session3",
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "f.py"},
                    {"tool_name": "Edit", "file_path": "g.py"},
                    {"tool_name": "Edit", "file_path": "h.py"},
                    {"tool_name": "Edit", "file_path": "i.py"},
                    {"tool_name": "Edit", "file_path": "j.py"},
                    {"tool_name": "Edit", "file_path": "k.py"},
                    {"tool_name": "Edit", "file_path": "l.py"},
                ]
            },
        ])

        chains = result["dependency_chain_lengths"]
        assert chains["short_chains"] == 1  # 1 edit
        assert chains["medium_chains"] == 1  # 4 edits
        assert chains["long_chains"] == 1  # 7 edits

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_tool_call_dependency_graph([
            "not a dict",
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            },
        ])

        assert result["total_sessions"] == 1
        assert result["total_tool_calls"] == 1

    def test_record_with_non_list_tool_calls_skipped(self):
        """Verify records with non-list tool_calls are skipped."""
        result = analyze_pack_tool_call_dependency_graph([
            {"session_id": "session1", "tool_calls": "not a list"},
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"}
                ]
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_tool_calls"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "READ", "file_path": "a.py"},
                    {"tool_name": "Edit", "file_path": "b.py"},
                ]
            }
        ])

        assert result["independent_call_count"] == 1
        assert result["dependent_call_count"] == 1

    def test_empty_tool_calls_list(self):
        """Verify empty tool_calls list is handled."""
        result = analyze_pack_tool_call_dependency_graph([
            {"session_id": "session1", "tool_calls": []},
        ])

        assert result["total_sessions"] == 1
        assert result["total_tool_calls"] == 0

    def test_realistic_pack_pattern(self):
        """Verify realistic pack with mixed patterns."""
        result = analyze_pack_tool_call_dependency_graph([
            {
                "session_id": "session1",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                    {"tool_name": "Read", "file_path": "test.py"},
                    {"tool_name": "Grep", "pattern": "def.*test"},
                ]
            },
            {
                "session_id": "session2",
                "tool_calls": [
                    {"tool_name": "Edit", "file_path": "main.py"},
                    {"tool_name": "Bash", "command": "pytest tests/"},
                ]
            },
            {
                "session_id": "session3",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": "main.py"},
                ]
            },
        ])

        # First session: 3 independent
        # Second session: 2 dependent
        # Third session: 1 independent (different session from edit)
        assert result["total_sessions"] == 3
        assert result["total_tool_calls"] == 6
        assert result["independent_call_count"] == 4
        assert result["dependent_call_count"] == 2

    def test_redundant_read_files_limited_to_ten(self):
        """Verify redundant_read_files limited to top 10."""
        sessions = []
        for i in range(15):
            sessions.append({
                "session_id": f"session1_{i}",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": f"file{i}.py"}
                ]
            })
            sessions.append({
                "session_id": f"session2_{i}",
                "tool_calls": [
                    {"tool_name": "Read", "file_path": f"file{i}.py"}
                ]
            })

        result = analyze_pack_tool_call_dependency_graph(sessions)
        assert result["redundant_read_count"] == 15
        assert len(result["redundant_read_files"]) == 10


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33

    def test_percentage_zero_denominator(self):
        """Verify percentage with zero denominator returns 0."""
        assert _percentage(10, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1, 2, 3, 4, 5]) == 3.0
        assert _average([10, 20]) == 15.0

    def test_average_empty_list(self):
        """Verify average of empty list returns 0."""
        assert _average([]) == 0.0

    def test_summarize_chain_lengths_empty(self):
        """Verify summarizing empty chain lengths."""
        result = _summarize_chain_lengths([])
        assert result["short_chains"] == 0
        assert result["medium_chains"] == 0
        assert result["long_chains"] == 0

    def test_summarize_chain_lengths_short(self):
        """Verify summarizing short chains (1-2)."""
        result = _summarize_chain_lengths([1, 2, 1, 2])
        assert result["short_chains"] == 4
        assert result["medium_chains"] == 0
        assert result["long_chains"] == 0

    def test_summarize_chain_lengths_medium(self):
        """Verify summarizing medium chains (3-5)."""
        result = _summarize_chain_lengths([3, 4, 5])
        assert result["short_chains"] == 0
        assert result["medium_chains"] == 3
        assert result["long_chains"] == 0

    def test_summarize_chain_lengths_long(self):
        """Verify summarizing long chains (6+)."""
        result = _summarize_chain_lengths([6, 7, 10])
        assert result["short_chains"] == 0
        assert result["medium_chains"] == 0
        assert result["long_chains"] == 3

    def test_summarize_chain_lengths_mixed(self):
        """Verify summarizing mixed chain lengths."""
        result = _summarize_chain_lengths([1, 2, 3, 4, 5, 6, 7, 8])
        assert result["short_chains"] == 2  # 1, 2
        assert result["medium_chains"] == 3  # 3, 4, 5
        assert result["long_chains"] == 3  # 6, 7, 8
