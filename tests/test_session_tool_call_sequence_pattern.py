"""Tests for session tool call sequence pattern analyzer."""

import pytest

from synthesis.session_tool_call_sequence_pattern import (
    analyze_session_tool_call_sequence_pattern,
<<<<<<< HEAD
    _extract_bigrams,
    _extract_trigrams,
    _calculate_transitions,
    _calculate_sequence_lengths,
    _detect_inefficient_patterns,
=======
    _count_circular_reads,
    _count_patterns,
    _find_sequence_patterns,
    _find_transitions,
    _max_consecutive_same,
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
)


class TestAnalyzeSessionToolCallSequencePattern:
    """Test main analyzer function."""

<<<<<<< HEAD
    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_tool_call_sequence_pattern([])

        assert result["total_sequences"] == 0
        assert result["common_patterns"] == []
        assert result["sequence_length_distribution"] == {}
        assert result["tool_transitions"] == {}
        assert result["inefficient_patterns"] == []
        assert result["avg_sequence_length"] == 0.0
        assert result["most_common_workflow"] is None
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_tool_call_sequence_pattern(None)
<<<<<<< HEAD
        assert result["total_sequences"] == 0
=======
        assert result["total_tool_calls"] == 0
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_tool_call_sequence_pattern("not a list")

    def test_single_tool_call(self):
<<<<<<< HEAD
        """Verify single tool call returns minimal metrics."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1}
        ])

        assert result["total_sequences"] == 1
        # No patterns possible with single tool
        assert len(result["common_patterns"]) == 0

    def test_simple_read_edit_pattern(self):
        """Verify simple Read → Edit pattern is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
        ])

        assert result["total_sequences"] == 2
        # Should have bigram pattern
        patterns = [p["pattern"] for p in result["common_patterns"]]
        assert "Read → Edit" in patterns

    def test_read_edit_read_workflow(self):
        """Verify Read → Edit → Read workflow is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "foo.py"},
            {"tool_name": "Edit", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "foo.py"},
        ])

        patterns = [p["pattern"] for p in result["common_patterns"]]
        assert "Read → Edit → Read" in patterns
        assert result["most_common_workflow"] == "Read → Edit → Read"

    def test_common_patterns_ordered_by_frequency(self):
        """Verify common patterns are ordered by frequency."""
        records = [
            {"tool_name": "Read", "turn_index": i * 3 + 1}
            for i in range(5)
        ] + [
            {"tool_name": "Edit", "turn_index": i * 3 + 2}
            for i in range(5)
        ] + [
            {"tool_name": "Bash", "turn_index": i * 3 + 3}
            for i in range(5)
        ]

        # Create sequence: Read, Edit, Bash, Read, Edit, Bash, ...
        sequence_records = []
        for i in range(5):
            sequence_records.extend([
                {"tool_name": "Read", "turn_index": i * 3 + 1},
                {"tool_name": "Edit", "turn_index": i * 3 + 2},
                {"tool_name": "Bash", "turn_index": i * 3 + 3},
            ])

        result = analyze_session_tool_call_sequence_pattern(sequence_records)

        # Most common should be Read → Edit → Bash (appears 4 times)
        assert result["most_common_workflow"] == "Read → Edit → Bash"

    def test_tool_transitions_calculated(self):
        """Verify tool transition matrix is calculated."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
            {"tool_name": "Bash", "turn_index": 4},
        ])

        transitions = result["tool_transitions"]
        assert "Read" in transitions
        assert transitions["Read"]["Edit"] == 1
        assert transitions["Read"]["Bash"] == 1
        assert transitions["Edit"]["Read"] == 1

    def test_sequence_length_distribution(self):
        """Verify sequence length distribution is calculated."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Read", "turn_index": 3},
            {"tool_name": "Edit", "turn_index": 4},
            {"tool_name": "Bash", "turn_index": 5},
        ])

        # Should have: 1 sequence of length 3 (Read), 1 of length 1 (Edit), 1 of length 1 (Bash)
        dist = result["sequence_length_distribution"]
        assert dist[3] == 1  # Three consecutive Reads
        assert dist[1] == 2  # One Edit and one Bash

    def test_avg_sequence_length_calculated(self):
        """Verify average sequence length is calculated correctly."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
            {"tool_name": "Edit", "turn_index": 3},
        ])

        # Sequences: [2 Reads], [1 Edit] -> avg = (2 + 1) / 2 = 1.5
        assert result["avg_sequence_length"] == 1.5

    def test_excessive_re_reads_detected(self):
        """Verify excessive re-reads pattern is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "foo.py"},
        ])

        inefficient = result["inefficient_patterns"]
        assert any(p["type"] == "excessive_re_reads" for p in inefficient)
        pattern = next(p for p in inefficient if p["type"] == "excessive_re_reads")
        assert pattern["file"] == "foo.py"

    def test_circular_read_edit_read_detected(self):
        """Verify circular Read → Edit → Read pattern is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "foo.py"},
            {"tool_name": "Edit", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "foo.py"},
        ])

        inefficient = result["inefficient_patterns"]
        assert any(p["type"] == "circular_read_edit_read" for p in inefficient)

    def test_excessive_read_chain_detected(self):
        """Verify excessive read chain (5+ consecutive reads) is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": i + 1}
            for i in range(6)
        ])

        inefficient = result["inefficient_patterns"]
        assert any(p["type"] == "excessive_read_chain" for p in inefficient)
        pattern = next(p for p in inefficient if p["type"] == "excessive_read_chain")
        assert pattern["count"] == 6

    def test_edit_without_read_detected(self):
        """Verify edit without prior read is detected."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Edit", "turn_index": 1, "file_path": "foo.py"},
        ])

        inefficient = result["inefficient_patterns"]
        assert any(p["type"] == "edit_without_read" for p in inefficient)
        pattern = next(p for p in inefficient if p["type"] == "edit_without_read")
        assert pattern["file"] == "foo.py"

    def test_edit_after_read_not_flagged(self):
        """Verify edit after read is not flagged as inefficient."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "foo.py"},
            {"tool_name": "Edit", "turn_index": 2, "file_path": "foo.py"},
        ])

        inefficient = result["inefficient_patterns"]
        assert not any(p["type"] == "edit_without_read" for p in inefficient)
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            "not a dict",
<<<<<<< HEAD
            {"tool_name": "Read", "turn_index": 1},
            {"tool_name": "Edit", "turn_index": 2},
        ])

        assert result["total_sequences"] == 2
=======
            {"tool_name": "Read"},
            {"tool_name": "Edit"},
        ])

        assert result["total_tool_calls"] == 2
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
<<<<<<< HEAD
            {"turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
        ])

        assert result["total_sequences"] == 1

    def test_empty_tool_name_skipped(self):
        """Verify records with empty tool_name are skipped."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2},
        ])

        assert result["total_sequences"] == 1

    def test_common_patterns_limited_to_ten(self):
        """Verify common patterns are limited to 10."""
        # Create many different patterns
        records = []
        for i in range(20):
            records.extend([
                {"tool_name": f"Tool{i}A", "turn_index": i * 3 + 1},
                {"tool_name": f"Tool{i}B", "turn_index": i * 3 + 2},
                {"tool_name": f"Tool{i}C", "turn_index": i * 3 + 3},
            ])

        result = analyze_session_tool_call_sequence_pattern(records)
        assert len(result["common_patterns"]) <= 10

    def test_inefficient_patterns_limited_to_ten(self):
        """Verify inefficient patterns are limited to 10."""
        # Create many edit without read patterns
        records = [
            {"tool_name": "Edit", "turn_index": i, "file_path": f"file{i}.py"}
            for i in range(20)
        ]

        result = analyze_session_tool_call_sequence_pattern(records)
        assert len(result["inefficient_patterns"]) <= 10


class TestExtractBigrams:
    """Test bigram extraction helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty counter."""
        bigrams = _extract_bigrams([])
        assert len(bigrams) == 0

    def test_single_item_returns_empty(self):
        """Verify single item sequence returns empty counter."""
        bigrams = _extract_bigrams(["Read"])
        assert len(bigrams) == 0

    def test_two_items_returns_one_bigram(self):
        """Verify two item sequence returns one bigram."""
        bigrams = _extract_bigrams(["Read", "Edit"])
        assert bigrams[("Read", "Edit")] == 1

    def test_multiple_items_returns_correct_bigrams(self):
        """Verify multiple items return correct bigrams."""
        bigrams = _extract_bigrams(["Read", "Edit", "Bash", "Read"])
        assert bigrams[("Read", "Edit")] == 1
        assert bigrams[("Edit", "Bash")] == 1
        assert bigrams[("Bash", "Read")] == 1

    def test_repeated_bigrams_counted(self):
        """Verify repeated bigrams are counted."""
        bigrams = _extract_bigrams(["Read", "Edit", "Read", "Edit"])
        assert bigrams[("Read", "Edit")] == 2
        assert bigrams[("Edit", "Read")] == 1


class TestExtractTrigrams:
    """Test trigram extraction helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty counter."""
        trigrams = _extract_trigrams([])
        assert len(trigrams) == 0

    def test_two_items_returns_empty(self):
        """Verify two item sequence returns empty counter."""
        trigrams = _extract_trigrams(["Read", "Edit"])
        assert len(trigrams) == 0

    def test_three_items_returns_one_trigram(self):
        """Verify three item sequence returns one trigram."""
        trigrams = _extract_trigrams(["Read", "Edit", "Bash"])
        assert trigrams[("Read", "Edit", "Bash")] == 1

    def test_multiple_items_returns_correct_trigrams(self):
        """Verify multiple items return correct trigrams."""
        trigrams = _extract_trigrams(["Read", "Edit", "Bash", "Read"])
        assert trigrams[("Read", "Edit", "Bash")] == 1
        assert trigrams[("Edit", "Bash", "Read")] == 1

    def test_repeated_trigrams_counted(self):
        """Verify repeated trigrams are counted."""
        trigrams = _extract_trigrams(["Read", "Edit", "Bash", "Read", "Edit", "Bash"])
        assert trigrams[("Read", "Edit", "Bash")] == 2


class TestCalculateTransitions:
    """Test transition calculation helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty dict."""
        transitions = _calculate_transitions([])
        assert transitions == {}

    def test_single_item_returns_empty(self):
        """Verify single item sequence returns empty dict."""
        transitions = _calculate_transitions(["Read"])
        assert transitions == {}

    def test_two_items_returns_one_transition(self):
        """Verify two item sequence returns one transition."""
        transitions = _calculate_transitions(["Read", "Edit"])
        assert transitions["Read"]["Edit"] == 1

    def test_multiple_transitions_calculated(self):
        """Verify multiple transitions are calculated correctly."""
        transitions = _calculate_transitions(["Read", "Edit", "Bash", "Read"])
        assert transitions["Read"]["Edit"] == 1
        assert transitions["Edit"]["Bash"] == 1
        assert transitions["Bash"]["Read"] == 1

    def test_repeated_transitions_counted(self):
        """Verify repeated transitions are counted."""
        transitions = _calculate_transitions(["Read", "Edit", "Read", "Edit"])
        assert transitions["Read"]["Edit"] == 2
        assert transitions["Edit"]["Read"] == 1


class TestCalculateSequenceLengths:
    """Test sequence length calculation helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns empty counter."""
        lengths = _calculate_sequence_lengths([])
        assert len(lengths) == 0

    def test_single_item_returns_length_one(self):
        """Verify single item sequence returns length 1."""
        lengths = _calculate_sequence_lengths(["Read"])
        assert lengths[1] == 1

    def test_consecutive_same_tool_counted(self):
        """Verify consecutive same tool calls are counted correctly."""
        lengths = _calculate_sequence_lengths(["Read", "Read", "Read"])
        assert lengths[3] == 1

    def test_mixed_sequences_counted(self):
        """Verify mixed sequences are counted correctly."""
        lengths = _calculate_sequence_lengths(["Read", "Read", "Edit", "Bash", "Bash"])
        assert lengths[2] == 2  # Two Reads and two Bashes
        assert lengths[1] == 1  # One Edit


class TestDetectInefficientPatterns:
    """Test inefficient pattern detection helper."""

    def test_empty_sequence_returns_empty(self):
        """Verify empty sequence returns no patterns."""
        patterns = _detect_inefficient_patterns([], [])
        assert patterns == []

    def test_excessive_re_reads_detected(self):
        """Verify excessive re-reads are detected."""
        sequence = ["Read", "Read", "Read"]
        file_paths = ["foo.py", "foo.py", "foo.py"]
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert any(p["type"] == "excessive_re_reads" for p in patterns)

    def test_different_files_not_flagged(self):
        """Verify different files are not flagged as excessive re-reads."""
        sequence = ["Read", "Read", "Read"]
        file_paths = ["foo.py", "bar.py", "baz.py"]
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert not any(p["type"] == "excessive_re_reads" for p in patterns)

    def test_circular_read_edit_read_detected(self):
        """Verify circular Read → Edit → Read is detected."""
        sequence = ["Read", "Edit", "Read"]
        file_paths = ["foo.py", "foo.py", "foo.py"]
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert any(p["type"] == "circular_read_edit_read" for p in patterns)

    def test_excessive_read_chain_detected(self):
        """Verify excessive read chain is detected."""
        sequence = ["Read"] * 6
        file_paths = [None] * 6
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert any(p["type"] == "excessive_read_chain" for p in patterns)
        pattern = next(p for p in patterns if p["type"] == "excessive_read_chain")
        assert pattern["count"] == 6

    def test_short_read_chain_not_flagged(self):
        """Verify short read chains are not flagged."""
        sequence = ["Read"] * 4
        file_paths = [None] * 4
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert not any(p["type"] == "excessive_read_chain" for p in patterns)

    def test_edit_without_read_detected(self):
        """Verify edit without read is detected."""
        sequence = ["Edit"]
        file_paths = ["foo.py"]
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert any(p["type"] == "edit_without_read" for p in patterns)

    def test_edit_after_read_not_flagged(self):
        """Verify edit after read is not flagged."""
        sequence = ["Read", "Edit"]
        file_paths = ["foo.py", "foo.py"]
        patterns = _detect_inefficient_patterns(sequence, file_paths)

        assert not any(p["type"] == "edit_without_read" for p in patterns)
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

<<<<<<< HEAD
    def test_typical_workflow_pattern(self):
        """Simulate typical Read → Edit → Bash workflow."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Edit", "turn_index": 3, "file_path": "foo.py"},
            {"tool_name": "Bash", "turn_index": 4},
        ])

        patterns = [p["pattern"] for p in result["common_patterns"]]
        assert "Read → Edit → Bash" in patterns
        assert result["total_sequences"] == 4

    def test_inefficient_workflow_detected(self):
        """Simulate inefficient workflow with excessive re-reads."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": 1, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "foo.py"},
            {"tool_name": "Edit", "turn_index": 4, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 5, "file_path": "foo.py"},
        ])

        inefficient = result["inefficient_patterns"]
        assert len(inefficient) >= 2  # Should detect excessive re-reads and circular

    def test_empty_session(self):
        """Simulate empty session with no tool calls."""
        result = analyze_session_tool_call_sequence_pattern([])

        assert result["total_sequences"] == 0
        assert result["most_common_workflow"] is None

    def test_single_tool_session(self):
        """Simulate session with only one type of tool."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Read", "turn_index": i}
            for i in range(10)
        ])

        assert result["total_sequences"] == 10
        # Should detect excessive read chain
        assert any(
            p["type"] == "excessive_read_chain"
            for p in result["inefficient_patterns"]
        )

    def test_complex_multi_tool_workflow(self):
        """Simulate complex workflow with multiple tool types."""
        result = analyze_session_tool_call_sequence_pattern([
            {"tool_name": "Grep", "turn_index": 1},
            {"tool_name": "Read", "turn_index": 2, "file_path": "foo.py"},
            {"tool_name": "Read", "turn_index": 3, "file_path": "bar.py"},
            {"tool_name": "Edit", "turn_index": 4, "file_path": "foo.py"},
            {"tool_name": "Bash", "turn_index": 5},
            {"tool_name": "Read", "turn_index": 6, "file_path": "foo.py"},
            {"tool_name": "Write", "turn_index": 7, "file_path": "baz.py"},
        ])

        assert result["total_sequences"] == 7
        assert len(result["tool_transitions"]) > 0
        # Should detect Write without Read for baz.py
        assert any(
            p["type"] == "edit_without_read" and p["file"] == "baz.py"
            for p in result["inefficient_patterns"]
        )
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
