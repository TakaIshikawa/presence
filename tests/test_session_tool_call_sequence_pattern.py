"""Tests for session tool call sequence pattern analyzer."""

import pytest

from synthesis.session_tool_call_sequence_pattern import (
    analyze_session_tool_call_sequence_pattern,
    _count_circular_reads,
    _count_patterns,
    _find_sequence_patterns,
    _find_transitions,
    _max_consecutive_same,
)


class TestAnalyzeSessionToolCallSequencePattern:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns empty metrics."""
        result = analyze_session_tool_call_sequence_pattern([])

        assert result["total_tool_calls"] == 0
        assert result["unique_tools"] == 0
        assert result["sequence_patterns"] == []
        assert result["tool_transitions"] == []
        assert result["consecutive_same_tool"] == 0
        assert result["efficient_pattern_count"] == 0
        assert result["inefficient_pattern_count"] == 0
        assert result["circular_reads"] == 0
        assert result["workflow_efficiency"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_call_sequence_pattern(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_call_sequence_pattern("not a list")

    def test_single_tool_call(self):
        """Verify single tool call is handled."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["unique_tools"] == 1
        assert result["workflow_efficiency"] == "simple"

    def test_simple_sequence(self):
        """Verify simple tool sequence."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
        ])

        assert result["total_tool_calls"] == 3
        assert result["unique_tools"] == 2
        assert result["consecutive_same_tool"] == 1

    def test_efficient_pattern_detection(self):
        """Verify efficient patterns are detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Grep", "turn_index": 3},
            {"tool_name": "Read", "turn_index": 4},
            {"tool_name": "Edit", "turn_index": 5},
        ])

        assert result["efficient_pattern_count"] >= 1
        assert result["workflow_efficiency"] in ("efficient", "optimal")

    def test_inefficient_pattern_detection(self):
        """Verify inefficient patterns are detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Edit", "turn_index": 5},
        ])

        assert result["inefficient_pattern_count"] >= 1
        assert result["workflow_efficiency"] == "inefficient"

    def test_sequence_pattern_extraction(self):
        """Verify sequence patterns are extracted."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Bash"},
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Bash"},
        ])

        assert len(result["sequence_patterns"]) > 0
        # Should detect Read→Edit→Bash pattern twice
        pattern = result["sequence_patterns"][0]
        assert pattern["pattern"] == ["Read", "Edit", "Bash"]
        assert pattern["count"] == 2

    def test_tool_transitions_extraction(self):
        """Verify tool transitions are extracted."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
        ])

        transitions = result["tool_transitions"]
        assert len(transitions) > 0
        # Should have Read→Edit and Edit→Read transitions
        transition_pairs = [(t["from_tool"], t["to_tool"]) for t in transitions]
        assert ("Read", "Edit") in transition_pairs
        assert ("Edit", "Read") in transition_pairs

    def test_consecutive_same_tool_tracking(self):
        """Verify consecutive same tool tracking."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read"},
            {"tool_name": "Read"},
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Read"},
        ])

        assert result["consecutive_same_tool"] == 3

    def test_circular_reads_detection(self):
        """Verify circular reads are detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "file_path": "foo.py"},
            {"tool_name": "Edit", "file_path": "bar.py"},
            {"tool_name": "Read", "file_path": "foo.py"},
            {"tool_name": "Bash"},
            {"tool_name": "Read", "file_path": "foo.py"},
        ])

        assert result["circular_reads"] > 0

    def test_optimal_workflow_classification(self):
        """Verify optimal workflow classification."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep"},
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Read"},
            {"tool_name": "Bash"},
        ])

        assert result["efficient_pattern_count"] >= 2
        assert result["inefficient_pattern_count"] == 0
        assert result["workflow_efficiency"] == "optimal"

    def test_mixed_workflow_classification(self):
        """Verify mixed workflow classification."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
            {"tool_name": "Write"},
            {"tool_name": "Bash"},
            {"tool_name": "Glob"},
        ])

        # No strong patterns either way
        assert result["workflow_efficiency"] == "mixed"

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            "not a dict",
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
        ])

        assert result["total_tool_calls"] == 2

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            {"turn_index": 0},
            {"tool_name": "Read"},
        ])

        assert result["total_tool_calls"] == 1


class TestHelperFunctions:
    """Test helper functions."""

    def test_find_sequence_patterns_basic(self):
        """Verify basic sequence pattern finding."""
        tools = ["Read", "Edit", "Read", "Edit", "Read"]
        patterns = _find_sequence_patterns(tools, 2)

        assert len(patterns) > 0
        # Should find Read→Edit pattern
        pattern_dict = {tuple(p["pattern"]): p["count"] for p in patterns}
        assert pattern_dict.get(("Read", "Edit")) == 2

    def test_find_sequence_patterns_length_three(self):
        """Verify 3-tool sequence pattern finding."""
        tools = ["Read", "Edit", "Bash", "Read", "Edit", "Bash"]
        patterns = _find_sequence_patterns(tools, 3)

        assert len(patterns) > 0
        pattern_dict = {tuple(p["pattern"]): p["count"] for p in patterns}
        assert pattern_dict.get(("Read", "Edit", "Bash")) == 2

    def test_find_sequence_patterns_too_short(self):
        """Verify empty result for too short sequence."""
        tools = ["Read", "Edit"]
        patterns = _find_sequence_patterns(tools, 3)
        assert patterns == []

    def test_find_transitions_basic(self):
        """Verify basic transition finding."""
        tools = ["Read", "Edit", "Read", "Edit"]
        transitions = _find_transitions(tools)

        assert len(transitions) == 2
        trans_dict = {(t["from_tool"], t["to_tool"]): t["count"] for t in transitions}
        assert trans_dict.get(("Read", "Edit")) == 2
        assert trans_dict.get(("Edit", "Read")) == 1

    def test_find_transitions_single_tool(self):
        """Verify empty result for single tool."""
        tools = ["Read"]
        transitions = _find_transitions(tools)
        assert transitions == []

    def test_max_consecutive_same_basic(self):
        """Verify max consecutive calculation."""
        tools = ["Read", "Read", "Read", "Edit", "Read", "Read"]
        assert _max_consecutive_same(tools) == 3

    def test_max_consecutive_same_all_different(self):
        """Verify max consecutive with no repeats."""
        tools = ["Read", "Edit", "Bash", "Grep"]
        assert _max_consecutive_same(tools) == 1

    def test_max_consecutive_same_empty(self):
        """Verify max consecutive with empty list."""
        assert _max_consecutive_same([]) == 0

    def test_count_patterns_found(self):
        """Verify pattern counting."""
        tools = ["Read", "Edit", "Read", "Grep", "Read", "Edit"]
        patterns = [("Read", "Edit", "Read")]
        assert _count_patterns(tools, patterns) == 1

    def test_count_patterns_multiple(self):
        """Verify multiple pattern occurrences."""
        tools = ["Read", "Read", "Read", "Edit", "Read", "Read", "Read"]
        patterns = [("Read", "Read", "Read")]
        # Should find 2 occurrences (overlapping allowed)
        assert _count_patterns(tools, patterns) == 2

    def test_count_patterns_not_found(self):
        """Verify zero count when pattern not found."""
        tools = ["Read", "Edit", "Bash"]
        patterns = [("Grep", "Read", "Edit")]
        assert _count_patterns(tools, patterns) == 0

    def test_count_circular_reads_detected(self):
        """Verify circular reads detection."""
        tools = ["Read", "Edit", "Read", "Bash"]
        paths = ["foo.py", "bar.py", "foo.py", ""]
        assert _count_circular_reads(tools, paths) == 1

    def test_count_circular_reads_no_circles(self):
        """Verify no false positives."""
        tools = ["Read", "Edit", "Read"]
        paths = ["foo.py", "bar.py", "baz.py"]
        assert _count_circular_reads(tools, paths) == 0

    def test_count_circular_reads_within_window(self):
        """Verify circular detection within window size."""
        tools = ["Read", "Edit", "Edit", "Edit", "Edit", "Read"]
        paths = ["foo.py", "a", "b", "c", "d", "foo.py"]
        # foo.py read at index 0 and 5, within window of 5
        assert _count_circular_reads(tools, paths) == 1

    def test_count_circular_reads_outside_window(self):
        """Verify no detection outside window."""
        tools = ["Read", "Edit", "Edit", "Edit", "Edit", "Edit", "Read"]
        paths = ["foo.py", "a", "b", "c", "d", "e", "foo.py"]
        # foo.py read at index 0 and 6, outside window of 5
        assert _count_circular_reads(tools, paths) == 0

    def test_count_circular_reads_mismatched_lengths(self):
        """Verify handling of mismatched list lengths."""
        tools = ["Read", "Edit"]
        paths = ["foo.py"]
        assert _count_circular_reads(tools, paths) == 0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_efficient_grep_read_edit_workflow(self):
        """Simulate efficient search-read-edit workflow."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "file_path": ""},
            {"tool_name": "Read", "file_path": "src/main.py"},
            {"tool_name": "Edit", "file_path": "src/main.py"},
            {"tool_name": "Grep", "file_path": ""},
            {"tool_name": "Read", "file_path": "src/utils.py"},
            {"tool_name": "Edit", "file_path": "src/utils.py"},
            {"tool_name": "Bash", "file_path": ""},
        ])

        assert result["efficient_pattern_count"] >= 2
        assert result["inefficient_pattern_count"] == 0
        assert result["workflow_efficiency"] in ("optimal", "efficient")

    def test_inefficient_excessive_reads_workflow(self):
        """Simulate inefficient workflow with excessive re-reads."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "file_path": "foo.py"},
            {"tool_name": "Read", "file_path": "foo.py"},
            {"tool_name": "Read", "file_path": "foo.py"},
            {"tool_name": "Read", "file_path": "bar.py"},
            {"tool_name": "Read", "file_path": "bar.py"},
            {"tool_name": "Edit", "file_path": "foo.py"},
        ])

        assert result["inefficient_pattern_count"] > 0
        assert result["circular_reads"] > 0
        assert result["workflow_efficiency"] == "inefficient"

    def test_balanced_workflow_with_verification(self):
        """Simulate balanced workflow with read-edit-verify cycles."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "file_path": "src/main.py"},
            {"tool_name": "Edit", "file_path": "src/main.py"},
            {"tool_name": "Read", "file_path": "src/main.py"},
            {"tool_name": "Bash"},
            {"tool_name": "Read", "file_path": "tests/test_main.py"},
            {"tool_name": "Edit", "file_path": "tests/test_main.py"},
            {"tool_name": "Bash"},
        ])

        assert result["efficient_pattern_count"] > 0
        assert result["workflow_efficiency"] in ("efficient", "optimal", "mixed")

    def test_complex_multi_file_workflow(self):
        """Simulate complex workflow across multiple files."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Glob"},
            {"tool_name": "Read", "file_path": "a.py"},
            {"tool_name": "Read", "file_path": "b.py"},
            {"tool_name": "Edit", "file_path": "a.py"},
            {"tool_name": "Edit", "file_path": "b.py"},
            {"tool_name": "Bash"},
            {"tool_name": "Read", "file_path": "c.py"},
            {"tool_name": "Edit", "file_path": "c.py"},
            {"tool_name": "Bash"},
        ])

        assert result["total_tool_calls"] == 9
        assert result["unique_tools"] >= 3
        # Should have multiple Read→Edit transitions
        transitions = result["tool_transitions"]
        read_edit = [t for t in transitions if t["from_tool"] == "Read" and t["to_tool"] == "Edit"]
        assert len(read_edit) > 0
