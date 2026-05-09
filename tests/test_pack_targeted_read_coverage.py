"""Tests for pack targeted read coverage analyzer."""

import pytest

from synthesis.pack_targeted_read_coverage import analyze_pack_targeted_read_coverage


class TestAnalyzePackTargetedReadCoverage:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_targeted_read_coverage([])

        assert result["total_files_analyzed"] == 0
        assert result["total_reads"] == 0
        assert result["targeted_reads"] == 0
        assert result["full_reads"] == 0
        assert result["targeted_read_percentage"] == 0.0
        assert result["avg_lines_per_read"] == 0.0
        assert result["per_file_metrics"] == []
        assert result["verification_reads_detected"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_targeted_read_coverage(None)
        assert result["total_reads"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_targeted_read_coverage("not a list")

    def test_single_targeted_read(self):
        """Verify single targeted read with limit."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 1
        assert result["full_reads"] == 0
        assert result["targeted_read_percentage"] == 100.0
        assert result["avg_lines_per_read"] == 30.0

    def test_single_full_read(self):
        """Verify single full read without offset/limit."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "lines_read": 200, "turn_index": 0}
        ])

        assert result["total_reads"] == 1
        assert result["targeted_reads"] == 0
        assert result["full_reads"] == 1
        assert result["targeted_read_percentage"] == 0.0

    def test_mixed_targeted_and_full_reads(self):
        """Verify mix of targeted and full reads."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 200, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "offset": 50, "lines_read": 100, "turn_index": 2},
        ])

        # 2 targeted (file1, file3), 1 full (file2)
        assert result["total_reads"] == 3
        assert result["targeted_reads"] == 2
        assert result["full_reads"] == 1
        assert result["targeted_read_percentage"] == 66.67

    def test_high_optimization_pack_85_percent_targeted(self):
        """Verify high-optimization pack with 85%+ targeted reads."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 40, "lines_read": 40, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "limit": 50, "lines_read": 50, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "limit": 60, "lines_read": 60, "turn_index": 3},
            {"tool_name": "Read", "file_path": "file5.py", "limit": 70, "lines_read": 70, "turn_index": 4},
            {"tool_name": "Read", "file_path": "file6.py", "limit": 80, "lines_read": 80, "turn_index": 5},
            {"tool_name": "Read", "file_path": "file7.py", "lines_read": 200, "turn_index": 6},  # 1 full
        ])

        # 6 targeted / 7 total = 85.71%
        assert result["targeted_read_percentage"] >= 85.0
        assert result["targeted_reads"] == 6
        assert result["full_reads"] == 1

    def test_per_file_metrics_calculated(self):
        """Verify per-file metrics are calculated correctly."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file1.py", "limit": 40, "lines_read": 40, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 200, "turn_index": 2},
        ])

        assert len(result["per_file_metrics"]) == 2

        # Find file1.py metrics
        file1_metrics = next(m for m in result["per_file_metrics"] if m["file_path"] == "file1.py")
        assert file1_metrics["total_reads"] == 2
        assert file1_metrics["targeted_reads"] == 2
        assert file1_metrics["full_reads"] == 0
        assert file1_metrics["targeted_percentage"] == 100.0
        assert file1_metrics["avg_lines"] == 35.0  # (30 + 40) / 2

        # Find file2.py metrics
        file2_metrics = next(m for m in result["per_file_metrics"] if m["file_path"] == "file2.py")
        assert file2_metrics["total_reads"] == 1
        assert file2_metrics["targeted_reads"] == 0
        assert file2_metrics["full_reads"] == 1
        assert file2_metrics["targeted_percentage"] == 0.0

    def test_expected_files_filter(self):
        """Verify only reads for expected_files are analyzed."""
        result = analyze_pack_targeted_read_coverage(
            [
                {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
                {"tool_name": "Read", "file_path": "file2.py", "limit": 40, "lines_read": 40, "turn_index": 1},
                {"tool_name": "Read", "file_path": "file3.py", "limit": 50, "lines_read": 50, "turn_index": 2},
            ],
            expected_files=["file1.py", "file2.py"]
        )

        # Only file1.py and file2.py should be counted
        assert result["total_reads"] == 2
        assert result["total_files_analyzed"] == 2
        assert len(result["per_file_metrics"]) == 2

    def test_verification_read_detected_after_edit(self):
        """Verify verification reads are detected after Edit."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 1},
        ])

        # Read within 3 turns after edit
        assert result["verification_reads_detected"] == 1

    def test_verification_read_detected_after_write(self):
        """Verify verification reads are detected after Write."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Write", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 2},
        ])

        assert result["verification_reads_detected"] == 1

    def test_verification_read_within_three_turns(self):
        """Verify verification read detected within 3 turns."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Bash", "command": "ls", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 3},
        ])

        # turn 3 - turn 0 = 3, which is <= 3
        assert result["verification_reads_detected"] == 1

    def test_verification_read_beyond_window_not_detected(self):
        """Verify verification read beyond 3 turns not detected."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 0},
            {"tool_name": "Bash", "command": "ls", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Bash", "command": "ls", "turn_index": 3},
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 4},
        ])

        # turn 4 - turn 0 = 4, which is > 3
        assert result["verification_reads_detected"] == 0

    def test_read_efficiency_score_calculation(self):
        """Verify read efficiency score is calculated."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        # Score should be > 0
        assert result["read_efficiency_score"] > 0
        assert result["read_efficiency_score"] <= 100

    def test_per_file_efficiency_score(self):
        """Verify per-file efficiency scores are calculated."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0}
        ])

        file_metrics = result["per_file_metrics"][0]
        assert "efficiency_score" in file_metrics
        assert file_metrics["efficiency_score"] > 0

    def test_mixed_tool_calls(self):
        """Verify mixed tool calls are handled correctly."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Edit", "file_path": "file.py", "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "turn_index": 2},
            {"tool_name": "Skill", "skill": "verify", "turn_index": 3},
        ])

        assert result["total_reads"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "READ", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "read", "file_path": "file2.py", "lines_read": 100, "turn_index": 1},
        ])

        assert result["total_reads"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_targeted_read_coverage([
            "not a dict",
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 0},
        ])

        assert result["total_reads"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_pack_targeted_read_coverage([
            {"file_path": "file.py", "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "limit": 30, "lines_read": 30, "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_read_without_file_path_skipped(self):
        """Verify reads without file_path are skipped."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "lines_read": 30, "turn_index": 1},
        ])

        assert result["total_reads"] == 1

    def test_lines_read_inferred_from_limit(self):
        """Verify lines_read can be inferred from limit parameter."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file.py", "limit": 30, "turn_index": 0}
        ])

        assert result["avg_lines_per_read"] == 30.0

    def test_whitespace_handling(self):
        """Verify whitespace in fields is stripped."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "  Read  ", "file_path": "  file.py  ", "limit": 30, "lines_read": 30, "turn_index": 0},
        ])

        assert result["total_reads"] == 1
        assert result["per_file_metrics"][0]["file_path"] == "file.py"

    def test_baseline_pack_low_targeted_percentage(self):
        """Verify baseline pack with low targeted read percentage."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "file1.py", "lines_read": 200, "turn_index": 0},
            {"tool_name": "Read", "file_path": "file2.py", "lines_read": 300, "turn_index": 1},
            {"tool_name": "Read", "file_path": "file3.py", "lines_read": 250, "turn_index": 2},
            {"tool_name": "Read", "file_path": "file4.py", "limit": 50, "lines_read": 50, "turn_index": 3},
        ])

        # 1 targeted / 4 total = 25%
        assert result["targeted_read_percentage"] == 25.0
        assert result["avg_lines_per_read"] == 200.0  # (200+300+250+50)/4

    def test_expected_files_none_analyzes_all(self):
        """Verify None expected_files analyzes all reads."""
        result = analyze_pack_targeted_read_coverage(
            [
                {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
                {"tool_name": "Read", "file_path": "file2.py", "limit": 40, "lines_read": 40, "turn_index": 1},
            ],
            expected_files=None
        )

        assert result["total_reads"] == 2

    def test_expected_files_empty_list_analyzes_all(self):
        """Verify empty expected_files list analyzes all reads."""
        result = analyze_pack_targeted_read_coverage(
            [
                {"tool_name": "Read", "file_path": "file1.py", "limit": 30, "lines_read": 30, "turn_index": 0},
                {"tool_name": "Read", "file_path": "file2.py", "limit": 40, "lines_read": 40, "turn_index": 1},
            ],
            expected_files=[]
        )

        assert result["total_reads"] == 2

    def test_per_file_metrics_sorted_by_path(self):
        """Verify per-file metrics are sorted by file path."""
        result = analyze_pack_targeted_read_coverage([
            {"tool_name": "Read", "file_path": "c.py", "limit": 30, "lines_read": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "a.py", "limit": 40, "lines_read": 40, "turn_index": 1},
            {"tool_name": "Read", "file_path": "b.py", "limit": 50, "lines_read": 50, "turn_index": 2},
        ])

        paths = [m["file_path"] for m in result["per_file_metrics"]]
        assert paths == ["a.py", "b.py", "c.py"]
