"""Tests for session write tool frequency analyzer."""

import pytest

from synthesis.session_write_tool_frequency import analyze_session_write_tool_frequency


class TestAnalyzeSessionWriteToolFrequency:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_write_tool_frequency([])

        assert result["total_tool_calls"] == 0
        assert result["write_call_count"] == 0
        assert result["edit_call_count"] == 0
        assert result["write_to_edit_ratio"] == 0.0
        assert result["avg_file_size_written"] == 0.0
        assert result["overwrites_without_prior_read"] == 0
        assert result["write_then_immediate_read_count"] == 0
        assert result["unique_files_written"] == 0
        assert result["duplicate_writes_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_write_tool_frequency(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_write_tool_frequency("not a list")

    def test_single_write_call(self):
        """Verify single Write call is tracked."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "new_file.py", "turn_index": 0}
        ])

        assert result["write_call_count"] == 1
        assert result["unique_files_written"] == 1

    def test_write_to_edit_ratio_calculation(self):
        """Verify write-to-edit ratio calculation."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "file3.py", "turn_index": 2},
        ])

        # 1 Write / (1 Write + 2 Edit) = 33.33%
        assert result["write_to_edit_ratio"] == 33.33

    def test_all_writes_ratio(self):
        """Verify 100% writes results in 100% ratio."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "turn_index": 1},
        ])

        assert result["write_to_edit_ratio"] == 100.0

    def test_all_edits_ratio(self):
        """Verify all edits results in 0% write ratio."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Edit", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
        ])

        assert result["write_to_edit_ratio"] == 0.0

    def test_file_size_averaging(self):
        """Verify average file size calculation."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "file_size": 100, "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "file_size": 200, "turn_index": 1},
            {"tool_name": "Write", "file_path": "file3.py", "file_size": 300, "turn_index": 2},
        ])

        # (100 + 200 + 300) / 3 = 200
        assert result["avg_file_size_written"] == 200.0

    def test_overwrite_without_prior_read_detected(self):
        """Verify overwrite without read is detected."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "existing_file.py", "turn_index": 0}
        ])

        # Write without prior Read = potential overwrite
        assert result["overwrites_without_prior_read"] == 1

    def test_write_after_read_not_flagged(self):
        """Verify write after read is not flagged as overwrite."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 1},
        ])

        # Write after Read = safe, not flagged
        assert result["overwrites_without_prior_read"] == 0

    def test_write_with_explicit_prior_read_flag(self):
        """Verify explicit had_prior_read flag prevents overwrite flagging."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "had_prior_read": True, "turn_index": 0}
        ])

        assert result["overwrites_without_prior_read"] == 0

    def test_write_then_immediate_read_detected(self):
        """Verify write-then-immediate-read pattern is detected."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
        ])

        # Write followed by Read on same file = suspicious pattern
        assert result["write_then_immediate_read_count"] == 1

    def test_write_then_read_different_file_not_flagged(self):
        """Verify write-then-read on different files is not flagged."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 1},
        ])

        # Different files = not flagged
        assert result["write_then_immediate_read_count"] == 0

    def test_write_then_other_tool_breaks_sequence(self):
        """Verify other tools break write-then-read sequence."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Bash", "command": "ls", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 2},
        ])

        # Bash breaks the immediate sequence
        assert result["write_then_immediate_read_count"] == 0

    def test_duplicate_writes_detected(self):
        """Verify duplicate writes to same file are detected."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 2},
        ])

        # 1 unique file, written 3 times = 1 duplicate write
        assert result["unique_files_written"] == 1
        assert result["duplicate_writes_count"] == 1

    def test_multiple_unique_files_no_duplicates(self):
        """Verify multiple unique files without duplicates."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Write", "file_path": "file3.py", "turn_index": 2},
        ])

        assert result["unique_files_written"] == 3
        assert result["duplicate_writes_count"] == 0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "turn_index": 2},
            {"tool_name": "Bash", "command": "ls", "turn_index": 3},
        ])

        assert result["total_tool_calls"] == 4
        assert result["write_call_count"] == 1
        assert result["edit_call_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "WRITE", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "write", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "EDIT", "file_path": "file3.py", "turn_index": 2},
        ])

        assert result["write_call_count"] == 2
        assert result["edit_call_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_write_tool_frequency([
            "not a dict",
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["write_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_write_tool_frequency([
            {"file_path": "file.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1

    def test_write_without_file_path_handled(self):
        """Verify Write without file_path is handled gracefully."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "turn_index": 0},
        ])

        assert result["write_call_count"] == 1
        assert result["unique_files_written"] == 0

    def test_write_without_file_size_handled(self):
        """Verify Write without file_size is handled gracefully."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
        ])

        assert result["write_call_count"] == 1
        assert result["avg_file_size_written"] == 0.0

    def test_optimal_pattern_edit_preference(self):
        """Verify optimal pattern of preferring Edit over Write."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "new_file.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "existing1.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "existing2.py", "turn_index": 2},
            {"tool_name": "Edit", "file_path": "existing3.py", "turn_index": 3},
        ])

        # Low write-to-edit ratio = good (prefer Edit for modifications)
        assert result["write_to_edit_ratio"] == 25.0
        assert result["overwrites_without_prior_read"] == 1  # Only new file
        assert result["write_then_immediate_read_count"] == 0

    def test_anti_pattern_excessive_writes(self):
        """Verify anti-pattern of excessive Write usage."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Write", "file_path": "file3.py", "turn_index": 2},
            {"tool_name": "Write", "file_path": "file4.py", "turn_index": 3},
            {"tool_name": "Edit", "file_path": "file5.py", "turn_index": 4},
        ])

        # High write-to-edit ratio = anti-pattern (should use Edit more)
        assert result["write_to_edit_ratio"] == 80.0
        assert result["overwrites_without_prior_read"] == 4

    def test_anti_pattern_write_without_read(self):
        """Verify anti-pattern of writing without reading."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "existing_file.py", "turn_index": 0},
            {"tool_name": "Write", "file_path": "another_file.py", "turn_index": 1},
        ])

        # All writes without prior reads = anti-pattern
        assert result["overwrites_without_prior_read"] == 2

    def test_file_size_boolean_ignored(self):
        """Verify boolean file_size is ignored."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "file.py", "file_size": True, "turn_index": 0},
        ])

        assert result["avg_file_size_written"] == 0.0

    def test_whitespace_handling_in_paths(self):
        """Verify whitespace in file paths is stripped."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Write", "file_path": "  file.py  ", "turn_index": 0},
            {"tool_name": "Write", "file_path": "  file.py  ", "turn_index": 1},
        ])

        # Should be recognized as same file after stripping
        assert result["unique_files_written"] == 1
        assert result["duplicate_writes_count"] == 1

    def test_zero_denominator_in_ratio(self):
        """Verify zero denominator in ratio calculation."""
        result = analyze_session_write_tool_frequency([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
        ])

        # No Write or Edit calls
        assert result["write_to_edit_ratio"] == 0.0
