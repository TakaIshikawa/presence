"""Tests for session tool call sequence pattern analyzer."""

import pytest

from synthesis.session_tool_call_sequence_pattern import (
    analyze_session_tool_call_sequence_pattern,
    _calculate_transitions,
    _find_common_patterns,
    _count_efficient_patterns,
    _detect_inefficient_patterns,
    _count_circular_reads,
)


class TestAnalyzeSessionToolCallSequencePattern:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_tool_call_sequence_pattern([])

        assert result["total_tool_calls"] == 0
        assert result["sequence_length_stats"]["min"] == 0
        assert result["sequence_length_stats"]["max"] == 0
        assert result["sequence_length_stats"]["avg"] == 0.0
        assert result["tool_transitions"] == {}
        assert result["common_patterns"] == []
        assert result["efficient_pattern_count"] == 0
        assert result["inefficient_patterns"] == []
        assert result["circular_reads"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_call_sequence_pattern(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_call_sequence_pattern("not a list")

    def test_single_tool_call(self):
        """Verify single tool call is processed."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1}
        ])

        assert result["total_tool_calls"] == 1
        assert result["sequence_length_stats"]["min"] == 1
        assert result["tool_transitions"] == {}  # No transitions with single call

    def test_simple_sequence_with_transitions(self):
        """Verify simple tool sequence tracks transitions."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
        ])

        assert result["total_tool_calls"] == 3
        assert result["tool_transitions"]["Read->Edit"] == 1
        assert result["tool_transitions"]["Edit->Read"] == 1

    def test_common_pattern_detection(self):
        """Verify common patterns are detected."""
        records = [
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
        ]

        result = analyze_session_tool_call_sequence_pattern(records)

        # Should detect "Read, Edit" pattern occurring twice
        patterns = result["common_patterns"]
        read_edit_pattern = next((p for p in patterns if p["pattern"] == ["Read", "Edit"]), None)
        assert read_edit_pattern is not None
        assert read_edit_pattern["count"] == 2

    def test_efficient_pattern_count(self):
        """Verify efficient patterns are counted."""
        records = [
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
        ]

        result = analyze_session_tool_call_sequence_pattern(records)

        # "Read->Edit" is an efficient pattern, occurs twice
        assert result["efficient_pattern_count"] >= 2

    def test_circular_reads_detection(self):
        """Verify circular reads are detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "src/foo.py"},
            {"tool_name": "Bash", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3, "file_path": "src/foo.py"},
        ])

        # Same file read twice without edit -> circular read
        assert result["circular_reads"] == 1

    def test_no_circular_reads_when_file_edited(self):
        """Verify no circular reads when file is edited between reads."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "src/foo.py"},
            {"tool_name": "Edit", "turn_index": 2, "file_path": "src/foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "src/foo.py"},
        ])

        # File edited between reads -> not circular
        assert result["circular_reads"] == 0

    def test_inefficient_pattern_excessive_reads(self):
        """Verify detection of excessive consecutive reads."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
        ])

        assert "excessive_consecutive_reads" in result["inefficient_patterns"]

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            "not a dict",
            {"tool_name": "Read", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            {"turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
        ])

        assert result["total_tool_calls"] == 1

    def test_grep_read_edit_workflow(self):
        """Verify Grep->Read->Edit workflow is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
            {"tool_name": "Grep", "turn_index": 4},
            {"tool_name": "Read", "turn_index": 5},
            {"tool_name": "Edit", "turn_index": 6},
        ])

        # Should detect the 3-tool pattern
        patterns = result["common_patterns"]
        grep_read_edit = next(
            (p for p in patterns if p["pattern"] == ["Grep", "Read", "Edit"]),
            None
        )
        assert grep_read_edit is not None
        assert grep_read_edit["count"] == 2


class TestCalculateTransitions:
    """Test transition calculation helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty dict."""
        assert _calculate_transitions([]) == {}

    def test_single_tool_returns_empty(self):
        """Verify single tool returns empty dict."""
        assert _calculate_transitions(["Read"]) == {}

    def test_simple_transition(self):
        """Verify simple transition is tracked."""
        result = _calculate_transitions(["Read", "Edit"])
        assert result["Read->Edit"] == 1

    def test_multiple_same_transitions(self):
        """Verify repeated transitions are counted."""
        result = _calculate_transitions(["Read", "Edit", "Read", "Edit"])
        assert result["Read->Edit"] == 2
        assert result["Edit->Read"] == 1

    def test_different_transitions(self):
        """Verify different transitions are tracked separately."""
        result = _calculate_transitions(["Read", "Edit", "Write", "Bash"])
        assert result["Read->Edit"] == 1
        assert result["Edit->Write"] == 1
        assert result["Write->Bash"] == 1


class TestFindCommonPatterns:
    """Test common pattern detection helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty list."""
        assert _find_common_patterns([]) == []

    def test_single_tool_returns_empty(self):
        """Verify single tool returns empty list."""
        assert _find_common_patterns(["Read"]) == []

    def test_pattern_occurs_once_not_common(self):
        """Verify patterns occurring once are not returned."""
        result = _find_common_patterns(["Read", "Edit", "Write"])
        # No pattern repeats twice, so should be empty
        assert len(result) == 0

    def test_pattern_occurs_twice_is_common(self):
        """Verify patterns occurring twice are detected."""
        result = _find_common_patterns(["Read", "Edit", "Read", "Edit"])
        # "Read, Edit" occurs twice
        assert len(result) > 0
        assert any(p["pattern"] == ["Read", "Edit"] and p["count"] == 2 for p in result)

    def test_multiple_pattern_lengths(self):
        """Verify patterns of different lengths are detected."""
        sequence = ["A", "B", "C", "A", "B", "C"]
        result = _find_common_patterns(sequence)
        # Should detect both 2-length and 3-length patterns
        assert len(result) > 0

    def test_limited_to_top_10(self):
        """Verify result is limited to 10 patterns."""
        # Create a sequence with many different patterns
        sequence = list(range(50)) * 3  # Many repeated patterns
        result = _find_common_patterns([str(x) for x in sequence])
        assert len(result) <= 10


class TestCountEfficientPatterns:
    """Test efficient pattern counting helper."""

    def test_empty_patterns_returns_zero(self):
        """Verify empty patterns list returns 0."""
        assert _count_efficient_patterns([]) == 0

    def test_no_efficient_patterns_returns_zero(self):
        """Verify no efficient patterns returns 0."""
        patterns = [
            {"pattern": ["Bash", "Bash"], "count": 5}
        ]
        assert _count_efficient_patterns(patterns) == 0

    def test_efficient_pattern_counted(self):
        """Verify efficient patterns are counted."""
        patterns = [
            {"pattern": ["Read", "Edit"], "count": 3}
        ]
        assert _count_efficient_patterns(patterns) == 3

    def test_multiple_efficient_patterns(self):
        """Verify multiple efficient patterns are summed."""
        patterns = [
            {"pattern": ["Read", "Edit"], "count": 2},
            {"pattern": ["Grep", "Read"], "count": 3},
        ]
        assert _count_efficient_patterns(patterns) == 5


class TestDetectInefficientPatterns:
    """Test inefficient pattern detection helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty list."""
        assert _detect_inefficient_patterns([]) == []

    def test_no_inefficient_patterns(self):
        """Verify well-structured sequence has no inefficiencies."""
        result = _detect_inefficient_patterns(["Read", "Edit", "Write"])
        assert len(result) == 0

    def test_excessive_consecutive_reads(self):
        """Verify excessive consecutive reads are detected."""
        result = _detect_inefficient_patterns(["Read", "Read", "Read"])
        assert "excessive_consecutive_reads" in result

    def test_triple_read_pattern(self):
        """Verify triple read pattern is detected."""
        result = _detect_inefficient_patterns(["Read", "Read", "Read", "Edit"])
        assert "triple_read_pattern" in result

    def test_reads_interrupted_by_other_tools(self):
        """Verify reads interrupted by other tools don't trigger false positive."""
        result = _detect_inefficient_patterns(["Read", "Edit", "Read", "Edit", "Read"])
        assert "excessive_consecutive_reads" not in result


class TestCountCircularReads:
    """Test circular read counting helper."""

    def test_empty_inputs_returns_zero(self):
        """Verify empty inputs return 0."""
        assert _count_circular_reads({}, set()) == 0

    def test_file_read_once_not_circular(self):
        """Verify file read once is not circular."""
        file_reads = {"src/foo.py": [1]}
        file_edits = set()
        assert _count_circular_reads(file_reads, file_edits) == 0

    def test_file_read_twice_without_edit_is_circular(self):
        """Verify file read twice without edit is circular."""
        file_reads = {"src/foo.py": [1, 3]}
        file_edits = set()
        assert _count_circular_reads(file_reads, file_edits) == 1

    def test_file_read_twice_with_edit_not_circular(self):
        """Verify file read twice with edit is not circular."""
        file_reads = {"src/foo.py": [1, 3]}
        file_edits = {"src/foo.py"}
        assert _count_circular_reads(file_reads, file_edits) == 0

    def test_multiple_files_with_circular_reads(self):
        """Verify multiple files with circular reads are counted."""
        file_reads = {
            "src/foo.py": [1, 3],
            "src/bar.py": [2, 4, 6],
        }
        file_edits = set()
        assert _count_circular_reads(file_reads, file_edits) == 2


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_standard_modification_workflow(self):
        """Simulate standard Read->Edit->Read workflow."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "src/foo.py"},
            {"tool_name": "Edit", "turn_index": 2, "file_path": "src/foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "src/foo.py"},
        ])

        assert result["total_tool_calls"] == 3
        assert "Read->Edit" in result["tool_transitions"]
        assert "Edit->Read" in result["tool_transitions"]
        assert result["circular_reads"] == 0  # File was edited

    def test_search_and_modify_workflow(self):
        """Simulate Grep->Read->Edit workflow."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2, "file_path": "src/target.py"},
            {"tool_name": "Edit", "turn_index": 3, "file_path": "src/target.py"},
        ])

        assert result["tool_transitions"]["Grep->Read"] == 1
        assert result["tool_transitions"]["Read->Edit"] == 1
        # Verify efficient patterns are detected in common_patterns
        assert len(result["common_patterns"]) == 0  # Pattern only occurs once

    def test_inefficient_repeated_reads(self):
        """Simulate inefficient repeated reads without modifications."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "src/file.py"},
            {"tool_name": "Read", "turn_index": 2, "file_path": "src/file.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "src/file.py"},
            {"tool_name": "Read", "turn_index": 4, "file_path": "src/file.py"},
        ])

        assert result["circular_reads"] == 1
        assert "excessive_consecutive_reads" in result["inefficient_patterns"]

    def test_complex_multi_file_session(self):
        """Simulate complex session with multiple files and tools."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2, "file_path": "src/a.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "src/b.py"},
            {"tool_name": "Edit", "turn_index": 4, "file_path": "src/a.py"},
            {"tool_name": "Bash", "turn_index": 5},
            {"tool_name": "Read", "turn_index": 6, "file_path": "src/a.py"},
        ])

        assert result["total_tool_calls"] == 6
        assert result["circular_reads"] == 0  # src/a.py was edited
