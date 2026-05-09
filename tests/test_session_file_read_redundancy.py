"""Tests for session file read redundancy analyzer."""

import pytest

from synthesis.session_file_read_redundancy import (
    analyze_session_file_read_redundancy,
    _percentage,
    _average,
    _get_most_reread_files,
)


class TestAnalyzeSessionFileReadRedundancy:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_file_read_redundancy([])

        assert result["total_tool_calls"] == 0
        assert result["read_call_count"] == 0
        assert result["unique_files_read"] == 0
        assert result["files_read_multiple_times"] == 0
        assert result["total_rereads"] == 0
        assert result["exact_duplicate_reads"] == 0
        assert result["post_edit_rereads"] == 0
        assert result["exploratory_rereads"] == 0
        assert result["cache_avoidable_reads"] == 0
        assert result["redundancy_ratio"] == 0.0
        assert result["avg_turns_between_rereads"] == 0.0
        assert result["max_reread_count"] == 0
        assert result["most_reread_files"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_file_read_redundancy(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_file_read_redundancy("not a list")

    def test_single_read_no_redundancy(self):
        """Verify single read shows no redundancy."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0}
        ])

        assert result["read_call_count"] == 1
        assert result["unique_files_read"] == 1
        assert result["files_read_multiple_times"] == 0
        assert result["total_rereads"] == 0
        assert result["redundancy_ratio"] == 0.0

    def test_multiple_unique_files_no_redundancy(self):
        """Verify reading multiple unique files shows no redundancy."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "c.py", "turn_index": 2},
        ])

        assert result["read_call_count"] == 3
        assert result["unique_files_read"] == 3
        assert result["files_read_multiple_times"] == 0
        assert result["redundancy_ratio"] == 0.0

    def test_duplicate_read_detected(self):
        """Verify duplicate read is detected."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 1},
        ])

        assert result["read_call_count"] == 2
        assert result["unique_files_read"] == 1
        assert result["files_read_multiple_times"] == 1
        assert result["total_rereads"] == 1
        assert result["redundancy_ratio"] == 50.0

    def test_exact_duplicate_reads_with_same_params(self):
        """Verify exact duplicate reads with same offset/limit."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 1},
        ])

        assert result["exact_duplicate_reads"] == 1
        assert result["cache_avoidable_reads"] == 1

    def test_non_duplicate_reads_different_params(self):
        """Verify reads with different params not exact duplicates."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "offset": 200, "limit": 30, "turn_index": 1},
        ])

        assert result["exact_duplicate_reads"] == 0
        assert result["total_rereads"] == 1

    def test_post_edit_reread_classified_correctly(self):
        """Verify re-read after edit is classified as post-edit."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 2},
        ])

        assert result["total_rereads"] == 1
        assert result["post_edit_rereads"] == 1
        assert result["exploratory_rereads"] == 0

    def test_exploratory_reread_classified_correctly(self):
        """Verify re-read without edit is classified as exploratory."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 1},
        ])

        assert result["total_rereads"] == 1
        assert result["post_edit_rereads"] == 0
        assert result["exploratory_rereads"] == 1

    def test_mixed_post_edit_and_exploratory_rereads(self):
        """Verify mixed re-read types classified correctly."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "a.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 4},
        ])

        assert result["total_rereads"] == 2
        assert result["post_edit_rereads"] == 1  # a.py
        assert result["exploratory_rereads"] == 1  # b.py

    def test_avg_turns_between_rereads_calculation(self):
        """Verify average turns between re-reads calculation."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 5},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 8},
        ])

        # Distance: 5-0=5, 8-5=3, average=(5+3)/2=4
        assert result["avg_turns_between_rereads"] == 4.0

    def test_max_reread_count_tracking(self):
        """Verify maximum re-read count tracking."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 4},
        ])

        # a.py read 3 times
        assert result["max_reread_count"] == 3

    def test_most_reread_files_tracked(self):
        """Verify most re-read files are tracked."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 4},
        ])

        most_reread = result["most_reread_files"]
        assert len(most_reread) == 2
        assert most_reread[0]["file"] == "a.py"
        assert most_reread[0]["read_count"] == 3
        assert most_reread[1]["file"] == "b.py"
        assert most_reread[1]["read_count"] == 2

    def test_most_reread_files_limited_to_five(self):
        """Verify most_reread_files limited to top 5."""
        records = []
        for i in range(10):
            for _ in range(i + 1):
                records.append({
                    "tool_name": "Read",
                    "file_path": f"file{i}.py",
                    "turn_index": len(records)
                })

        result = analyze_session_file_read_redundancy(records)
        assert len(result["most_reread_files"]) == 5

    def test_cache_avoidable_reads_calculation(self):
        """Verify cache-avoidable reads calculation."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 1},
            {"tool_name": "Read", "file_path": "main.py", "offset": 100, "limit": 30, "turn_index": 2},
        ])

        # 2 cache-avoidable re-reads (2nd and 3rd reads)
        assert result["cache_avoidable_reads"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_file_read_redundancy([
            "not a dict",
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_record_without_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_file_read_redundancy([
            {"file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 1},
        ])

        assert result["total_tool_calls"] == 1
        assert result["read_call_count"] == 1

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "READ", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 1},
        ])

        assert result["read_call_count"] == 1
        assert result["total_tool_calls"] == 2

    def test_empty_file_path_handled(self):
        """Verify empty file paths are handled gracefully."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "", "turn_index": 0},
            {"tool_name": "Read", "file_path": "   ", "turn_index": 1},
        ])

        assert result["read_call_count"] == 2
        assert result["unique_files_read"] == 0

    def test_optimal_pattern_no_redundancy(self):
        """Verify optimal usage pattern with no redundancy."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "a.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "a.py", "offset": -30, "limit": 30, "turn_index": 3},
        ])

        # Last read is post-edit verification, justified
        assert result["total_rereads"] == 1
        assert result["post_edit_rereads"] == 1
        assert result["exploratory_rereads"] == 0

    def test_anti_pattern_high_redundancy(self):
        """Verify anti-pattern with high redundancy."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 3},
        ])

        assert result["redundancy_ratio"] == 75.0  # 3 re-reads out of 4
        assert result["exploratory_rereads"] == 3
        assert result["max_reread_count"] == 4

    def test_realistic_session_pattern(self):
        """Verify realistic session with mixed patterns."""
        result = analyze_session_file_read_redundancy([
            # Initial exploration
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "test.py", "turn_index": 1},
            # Edit
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 2},
            # Verification read
            {"tool_name": "Read", "file_path": "main.py", "offset": -30, "limit": 30, "turn_index": 3},
            # Another edit
            {"tool_name": "Edit", "file_path": "test.py", "turn_index": 4},
            # Verification read
            {"tool_name": "Read", "file_path": "test.py", "offset": -30, "limit": 30, "turn_index": 5},
        ])

        assert result["read_call_count"] == 4
        assert result["unique_files_read"] == 2
        assert result["files_read_multiple_times"] == 2
        assert result["total_rereads"] == 2
        assert result["post_edit_rereads"] == 2
        assert result["exploratory_rereads"] == 0

    def test_turn_index_not_required(self):
        """Verify turn_index is not required."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py"},
        ])

        assert result["read_call_count"] == 1

    def test_offset_limit_params_tracked(self):
        """Verify offset and limit parameters are tracked."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "offset": 0, "turn_index": 0},
            {"tool_name": "Read", "file_path": "main.py", "limit": 30, "turn_index": 1},
        ])

        # Both have params but different, so not exact duplicates
        assert result["exact_duplicate_reads"] == 0

    def test_multiple_edits_same_file(self):
        """Verify multiple edits to same file affects classification."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 0},
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 1},
            {"tool_name": "Edit", "file_path": "main.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 3},
            {"tool_name": "Read", "file_path": "main.py", "turn_index": 4},
        ])

        # Both re-reads are post-edit (file was edited)
        assert result["total_rereads"] == 2
        assert result["post_edit_rereads"] == 2

    def test_redundancy_ratio_calculation(self):
        """Verify redundancy ratio calculation."""
        result = analyze_session_file_read_redundancy([
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 0},
            {"tool_name": "Read", "file_path": "a.py", "turn_index": 1},
            {"tool_name": "Read", "file_path": "b.py", "turn_index": 2},
            {"tool_name": "Read", "file_path": "c.py", "turn_index": 3},
        ])

        # 1 re-read out of 4 reads = 25%
        assert result["redundancy_ratio"] == 25.0


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

    def test_get_most_reread_files_empty(self):
        """Verify getting most re-read files from empty dict."""
        result = _get_most_reread_files({})
        assert result == []

    def test_get_most_reread_files_no_rereads(self):
        """Verify files read once not included."""
        file_reads = {
            "a.py": [{"turn_index": 0}],
            "b.py": [{"turn_index": 1}],
        }
        result = _get_most_reread_files(file_reads)
        assert result == []

    def test_get_most_reread_files_sorted_by_count(self):
        """Verify files sorted by read count."""
        file_reads = {
            "a.py": [{"turn_index": i} for i in range(5)],
            "b.py": [{"turn_index": i} for i in range(3)],
            "c.py": [{"turn_index": i} for i in range(7)],
        }
        result = _get_most_reread_files(file_reads)

        assert len(result) == 3
        assert result[0]["file"] == "c.py"
        assert result[0]["read_count"] == 7
        assert result[1]["file"] == "a.py"
        assert result[1]["read_count"] == 5
        assert result[2]["file"] == "b.py"
        assert result[2]["read_count"] == 3

    def test_get_most_reread_files_limited_to_five(self):
        """Verify result limited to top 5 files."""
        file_reads = {
            f"file{i}.py": [{"turn_index": j} for j in range(i + 2)]
            for i in range(10)
        }
        result = _get_most_reread_files(file_reads)
        assert len(result) == 5
