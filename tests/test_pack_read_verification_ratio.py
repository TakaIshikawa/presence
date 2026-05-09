"""Tests for pack read-verification ratio analyzer."""

import pytest

from synthesis.pack_read_verification_ratio import analyze_pack_read_verification_ratio


class TestAnalyzePackReadVerificationRatio:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_read_verification_ratio([])

        assert result["total_reads"] == 0
        assert result["total_verify_commands"] == 0
        assert result["read_to_verify_ratio"] == 0.0
        assert result["reads_after_edit_count"] == 0
        assert result["reads_after_edit_percentage"] == 0.0
        assert result["strategic_verification_score"] == 50.0  # Base score

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_read_verification_ratio(None)
        assert result["total_reads"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_read_verification_ratio("not a list")

    def test_single_read_no_verify(self):
        """Verify single Read with no verify commands."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0}
        ])

        assert result["total_reads"] == 1
        assert result["total_verify_commands"] == 0
        # With 0 verify, ratio is just read count
        assert result["read_to_verify_ratio"] == 1.0

    def test_single_verify_no_reads(self):
        """Verify single verify with no reads."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Skill", "skill": "verify", "turn_index": 0}
        ])

        assert result["total_reads"] == 0
        assert result["total_verify_commands"] == 1
        assert result["read_to_verify_ratio"] == 0.0

    def test_equal_reads_and_verifies(self):
        """Verify equal reads and verify commands."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 2},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 3},
        ])

        assert result["total_reads"] == 2
        assert result["total_verify_commands"] == 2
        assert result["read_to_verify_ratio"] == 1.0  # 2/2

    def test_optimized_pack_low_read_to_verify_ratio(self):
        """Verify optimized pack with low read-to-verify ratio."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 3},
        ])

        # 1 read, 1 verify = ratio of 1.0
        assert result["total_reads"] == 1
        assert result["total_verify_commands"] == 1
        assert result["read_to_verify_ratio"] == 1.0

    def test_baseline_pack_high_read_to_verify_ratio(self):
        """Verify baseline pack with many re-reads instead of verify."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 4},
        ])

        # 4 reads, 0 verify = high ratio
        assert result["total_reads"] == 4
        assert result["total_verify_commands"] == 0
        assert result["read_to_verify_ratio"] == 4.0

    def test_read_after_edit_pattern_detected(self):
        """Verify read-after-edit pattern within 3 tool calls."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
        ])

        # turn_index 1 - turn_index 0 = 1, which is <= 3
        assert result["reads_after_edit_count"] == 1
        assert result["reads_after_edit_percentage"] == 100.0

    def test_read_after_edit_within_three_calls(self):
        """Verify pattern detected within 3 tool calls."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Bash", "command": "ls", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 3},
        ])

        # turn_index 3 - turn_index 0 = 3, which is <= 3
        assert result["reads_after_edit_count"] == 1

    def test_read_after_edit_beyond_window_not_detected(self):
        """Verify pattern not detected beyond 3 tool calls."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Bash", "command": "ls", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Bash", "command": "ls", "turn_index": 3},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 4},
        ])

        # turn_index 4 - turn_index 0 = 4, which is > 3
        assert result["reads_after_edit_count"] == 0

    def test_write_also_triggers_edit_pattern(self):
        """Verify Write tool also triggers read-after-edit pattern."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
        ])

        assert result["reads_after_edit_count"] == 1

    def test_multiple_reads_after_single_edit(self):
        """Verify multiple reads after edit are all counted."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 3},
        ])

        # All 3 reads are within 3 turns of the edit
        assert result["reads_after_edit_count"] == 3
        assert result["reads_after_edit_percentage"] == 100.0

    def test_reads_after_edit_percentage_calculation(self):
        """Verify reads-after-edit percentage with mixed reads."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 0},  # Not after edit
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 2},  # After edit
            {"tool_name": "Read", "file_path": "file3.py", "turn_index": 10},  # Not after edit
        ])

        # 1 read after edit out of 3 total reads
        assert result["reads_after_edit_count"] == 1
        assert result["reads_after_edit_percentage"] == 33.33

    def test_strategic_verification_score_base(self):
        """Verify base strategic score with no edits."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0}
        ])

        # Base score is 50.0
        assert result["strategic_verification_score"] == 50.0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are counted correctly."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 3},
            {"tool_name": "Skill", "skill": "other", "turn_index": 4},  # Non-verify skill
        ])

        assert result["total_reads"] == 1
        assert result["total_verify_commands"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool and skill name matching is case-insensitive."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "READ", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "SKILL", "skill": "VERIFY", "turn_index": 1},
            {"tool_name": "read", "file_path": "file2.py", "turn_index": 2},
            {"tool_name": "skill", "skill": "verify", "turn_index": 3},
        ])

        assert result["total_reads"] == 2
        assert result["total_verify_commands"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_read_verification_ratio([
            "not a dict",
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
        ])

        assert result["total_reads"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_pack_read_verification_ratio([
            {"file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_non_verify_skill_ignored(self):
        """Verify non-verify skills are ignored."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Skill", "skill": "cache", "turn_index": 0},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 1},
        ])

        assert result["total_verify_commands"] == 1

    def test_whitespace_handling(self):
        """Verify whitespace in fields is stripped."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "  Read  ", "file_path": "  file.py  ", "turn_index": 0},
            {"tool_name": "  Skill  ", "skill": "  verify  ", "turn_index": 1},
        ])

        assert result["total_reads"] == 1
        assert result["total_verify_commands"] == 1

    def test_tool_calls_without_file_path_handled(self):
        """Verify tool calls without file_path are handled gracefully."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "turn_index": 0},
            {"tool_name": "Edit", "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_optimized_workflow_pattern(self):
        """Verify optimized workflow with strategic verify usage."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file1.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "file2.py", "turn_index": 2},
            {"tool_name": "Edit", "file_path": "file3.py", "turn_index": 3},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 4},
            {"tool_name": "Read", "file_path": "file1.py", "turn_index": 5},
        ])

        # 2 reads, 1 verify = ratio 2.0
        # 1 read after edit (turn 5 is > 3 away from turn 3)
        assert result["total_reads"] == 2
        assert result["total_verify_commands"] == 1
        assert result["read_to_verify_ratio"] == 2.0

    def test_baseline_workflow_pattern(self):
        """Verify baseline workflow with excessive re-reads."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 2},
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 4},
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 5},
            {"tool_name": "Read", "file_path": "file.py", "turn_index": 6},
        ])

        # 4 reads, 0 verify, 3 reads after edit
        assert result["total_reads"] == 4
        assert result["total_verify_commands"] == 0
        assert result["reads_after_edit_count"] == 3
        assert result["reads_after_edit_percentage"] == 75.0

    def test_zero_reads_percentage_handled(self):
        """Verify zero reads doesn't cause division by zero."""
        result = analyze_pack_read_verification_ratio([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 1},
        ])

        assert result["reads_after_edit_percentage"] == 0.0
