"""Tests for pack file creation ratio analyzer."""

import pytest

from synthesis.pack_file_creation_ratio import analyze_pack_file_creation_ratio


class TestAnalyzePackFileCreationRatio:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_file_creation_ratio([])

        assert result["total_packs"] == 0
        assert result["total_file_operations"] == 0
        assert result["new_file_creations"] == 0
        assert result["file_overwrites"] == 0
        assert result["file_edits"] == 0
        assert result["creation_ratio"] == 0.0
        assert result["overwrite_ratio"] == 0.0
        assert result["edit_ratio"] == 0.0
        assert result["avg_creation_ratio_per_pack"] == 0.0
        assert result["high_creation_ratio_packs"] == 0
        assert result["write_without_read_count"] == 0
        assert result["documentation_creations"] == 0
        assert result["unexpected_file_creations"] == 0
        assert result["expected_files_match_rate"] == 0.0
        assert result["creation_clustering_detected"] == 0
        assert result["avg_files_per_session"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_file_creation_ratio(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_file_creation_ratio("not a list")

    def test_pack_with_only_new_file_creations(self):
        """Verify pack with only new file creations."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/new_file.py",
                "is_new_file": True,
                "expected_files": ["src/new_file.py"],
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "tests/test_new.py",
                "is_new_file": True,
                "expected_files": ["src/new_file.py", "tests/test_new.py"],
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_file_operations"] == 2
        assert result["new_file_creations"] == 2
        assert result["file_edits"] == 0
        assert result["creation_ratio"] == 100.0
        assert result["edit_ratio"] == 0.0
        assert result["high_creation_ratio_packs"] == 1  # >40%

    def test_pack_with_only_edits(self):
        """Verify pack with only edit operations."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/existing.py",
                "is_new_file": False,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "tests/test_existing.py",
                "is_new_file": False,
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_file_operations"] == 2
        assert result["new_file_creations"] == 0
        assert result["file_edits"] == 2
        assert result["creation_ratio"] == 0.0
        assert result["edit_ratio"] == 100.0
        assert result["high_creation_ratio_packs"] == 0

    def test_pack_with_balanced_creates_and_edits(self):
        """Verify pack with balanced creation and edit operations."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/new.py",
                "is_new_file": True,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/existing1.py",
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/existing2.py",
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/existing3.py",
            },
        ])

        assert result["total_file_operations"] == 4
        assert result["new_file_creations"] == 1
        assert result["file_edits"] == 3
        assert result["creation_ratio"] == 25.0  # 1/4
        assert result["edit_ratio"] == 75.0  # 3/4
        assert result["high_creation_ratio_packs"] == 0  # <40%

    def test_high_creation_ratio_antipattern(self):
        """Verify detection of high creation ratio antipattern."""
        # 5 creates, 2 edits = 71.4% creation ratio
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f4.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f5.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e2.py"},
        ])

        assert result["creation_ratio"] > 70.0
        assert result["high_creation_ratio_packs"] == 1
        assert result["avg_creation_ratio_per_pack"] > 70.0

    def test_write_without_read_antipattern(self):
        """Verify detection of Write-without-Read antipattern."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/existing.py",
                "is_new_file": False,
                "had_prior_read": False,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/existing2.py",
                "is_new_file": False,
                "had_prior_read": True,
            },
        ])

        assert result["file_overwrites"] == 2
        assert result["write_without_read_count"] == 1

    def test_documentation_creation_antipattern(self):
        """Verify detection of documentation file creation antipattern."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "README.md",
                "is_new_file": True,
                "is_documentation": True,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "docs/guide.md",
                "is_new_file": True,
                "is_documentation": True,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/code.py",
                "is_new_file": True,
                "is_documentation": False,
            },
        ])

        assert result["documentation_creations"] == 2
        assert result["new_file_creations"] == 3

    def test_expected_files_alignment(self):
        """Verify expected files alignment checking."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/expected.py",
                "is_new_file": True,
                "expected_files": ["src/expected.py", "tests/test_expected.py"],
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "tests/test_expected.py",
                "is_new_file": True,
                "expected_files": ["src/expected.py", "tests/test_expected.py"],
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/unexpected.py",
                "is_new_file": True,
                "expected_files": ["src/expected.py", "tests/test_expected.py"],
            },
        ])

        assert result["new_file_creations"] == 3
        assert result["unexpected_file_creations"] == 1
        # 2 expected matches out of 3 created = 66.67%
        assert result["expected_files_match_rate"] == pytest.approx(66.67, abs=0.01)

    def test_creation_clustering_detection(self):
        """Verify detection of creation clustering in specific sessions."""
        result = analyze_pack_file_creation_ratio([
            # Session 1: 7 operations
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f4.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f5.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f6.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f7.py", "is_new_file": True},
            # Session 2: 2 operations
            {"pack_id": "pack1", "session_id": "session2", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "session2", "tool_name": "Edit", "file_path": "e2.py"},
        ])

        # 7/9 = 77.8% in session1, which is >70% threshold
        assert result["creation_clustering_detected"] == 1

    def test_no_clustering_when_evenly_distributed(self):
        """Verify no clustering detection when operations evenly distributed."""
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session2", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "session2", "tool_name": "Edit", "file_path": "e1.py"},
        ])

        # 2/4 = 50% in session1, which is <70% threshold
        assert result["creation_clustering_detected"] == 0

    def test_multiple_packs_aggregation(self):
        """Verify correct aggregation across multiple packs."""
        result = analyze_pack_file_creation_ratio([
            # Pack 1: 2 creates, 2 edits = 50% creation ratio
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e2.py"},
            # Pack 2: 1 create, 3 edits = 25% creation ratio
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Edit", "file_path": "e3.py"},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Edit", "file_path": "e4.py"},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Edit", "file_path": "e5.py"},
        ])

        assert result["total_packs"] == 2
        assert result["total_file_operations"] == 8
        assert result["new_file_creations"] == 3
        assert result["file_edits"] == 5
        # Overall: 3/8 = 37.5%
        assert result["creation_ratio"] == 37.5
        # Average per pack: (50 + 25) / 2 = 37.5
        assert result["avg_creation_ratio_per_pack"] == 37.5
        # Pack1 has 50% which is >40%
        assert result["high_creation_ratio_packs"] == 1

    def test_avg_files_per_session_calculation(self):
        """Verify average files per session calculation."""
        result = analyze_pack_file_creation_ratio([
            # Pack 1: 6 operations across 2 sessions = 3 avg
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s2", "tool_name": "Edit", "file_path": "e2.py"},
            {"pack_id": "pack1", "session_id": "s2", "tool_name": "Edit", "file_path": "e3.py"},
            {"pack_id": "pack1", "session_id": "s2", "tool_name": "Edit", "file_path": "e4.py"},
            # Pack 2: 4 operations across 1 session = 4 avg
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Write", "file_path": "f4.py", "is_new_file": True},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Edit", "file_path": "e5.py"},
            {"pack_id": "pack2", "session_id": "s1", "tool_name": "Edit", "file_path": "e6.py"},
        ])

        # Average: (3 + 4) / 2 = 3.5
        assert result["avg_files_per_session"] == 3.5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_file_creation_ratio([
            "not a dict",
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/file.py",
                "is_new_file": True,
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_file_operations"] == 1

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "file_path": "src/file.py",
                "is_new_file": True,
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/other.py",
            },
        ])

        # Only the Edit operation counted
        assert result["total_file_operations"] == 1
        assert result["file_edits"] == 1

    def test_file_path_normalization(self):
        """Verify file path normalization in expected files."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/file.py",
                "is_new_file": True,
                "expected_files": ["./src/file.py"],  # Leading ./
            },
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "tests\\test_file.py",  # Backslashes
                "is_new_file": True,
                "expected_files": ["tests/test_file.py"],  # Forward slashes
            },
        ])

        # Both should match after normalization
        assert result["expected_files_match_rate"] == 100.0

    def test_optimal_pattern_low_creation_high_edits(self):
        """Verify optimal pattern: low creation ratio, high edit ratio."""
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e2.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e3.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e4.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e5.py"},
        ])

        assert result["creation_ratio"] < 20.0  # 1/6 = 16.67%
        assert result["edit_ratio"] > 80.0  # 5/6 = 83.33%
        assert result["high_creation_ratio_packs"] == 0

    def test_edge_case_single_operation(self):
        """Verify handling of pack with single operation."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Edit",
                "file_path": "src/file.py",
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_file_operations"] == 1
        assert result["creation_ratio"] == 0.0
        assert result["edit_ratio"] == 100.0

    def test_edge_case_no_expected_files(self):
        """Verify handling when expected_files is not provided."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/file.py",
                "is_new_file": True,
            },
        ])

        # Without expected_files, match rate should be 0
        assert result["expected_files_match_rate"] == 0.0
        assert result["unexpected_file_creations"] == 0

    def test_overwrite_ratio_calculation(self):
        """Verify overwrite ratio calculation."""
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": False},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": False},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
        ])

        # 2 overwrites out of 4 operations = 50%
        assert result["file_overwrites"] == 2
        assert result["overwrite_ratio"] == 50.0

    def test_case_insensitive_tool_names(self):
        """Verify tool names are case-insensitive."""
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "WRITE", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "write", "file_path": "f2.py", "is_new_file": True},
        ])

        assert result["new_file_creations"] == 2
        assert result["file_edits"] == 1

    def test_pack_with_no_expected_files_list(self):
        """Verify handling when expected_files is provided but empty."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "session1",
                "tool_name": "Write",
                "file_path": "src/file.py",
                "is_new_file": True,
                "expected_files": [],
            },
        ])

        assert result["expected_files_match_rate"] == 0.0

    def test_comprehensive_antipattern_detection(self):
        """Verify comprehensive antipattern detection in realistic pack."""
        result = analyze_pack_file_creation_ratio([
            # High creation ratio
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f3.py", "is_new_file": True},
            # Write without read
            {
                "pack_id": "pack1",
                "session_id": "s1",
                "tool_name": "Write",
                "file_path": "existing.py",
                "is_new_file": False,
                "had_prior_read": False,
            },
            # Documentation creation
            {
                "pack_id": "pack1",
                "session_id": "s1",
                "tool_name": "Write",
                "file_path": "README.md",
                "is_new_file": True,
                "is_documentation": True,
            },
            # Unexpected file
            {
                "pack_id": "pack1",
                "session_id": "s1",
                "tool_name": "Write",
                "file_path": "unexpected.py",
                "is_new_file": True,
                "expected_files": ["f1.py", "f2.py", "f3.py"],
            },
            # Normal edit
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
        ])

        assert result["high_creation_ratio_packs"] == 1
        assert result["write_without_read_count"] == 1
        assert result["documentation_creations"] == 1
        assert result["unexpected_file_creations"] >= 1

    def test_multiple_sessions_in_single_pack(self):
        """Verify handling of multiple sessions within single pack."""
        result = analyze_pack_file_creation_ratio([
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Write", "file_path": "f1.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s1", "tool_name": "Edit", "file_path": "e1.py"},
            {"pack_id": "pack1", "session_id": "s2", "tool_name": "Write", "file_path": "f2.py", "is_new_file": True},
            {"pack_id": "pack1", "session_id": "s2", "tool_name": "Edit", "file_path": "e2.py"},
            {"pack_id": "pack1", "session_id": "s3", "tool_name": "Edit", "file_path": "e3.py"},
        ])

        assert result["total_packs"] == 1
        # 5 operations across 3 sessions
        assert result["avg_files_per_session"] == pytest.approx(1.67, abs=0.01)

    def test_perfect_expected_files_alignment(self):
        """Verify perfect alignment when all created files are expected."""
        result = analyze_pack_file_creation_ratio([
            {
                "pack_id": "pack1",
                "session_id": "s1",
                "tool_name": "Write",
                "file_path": "src/analyzer.py",
                "is_new_file": True,
                "expected_files": ["src/analyzer.py", "tests/test_analyzer.py"],
            },
            {
                "pack_id": "pack1",
                "session_id": "s1",
                "tool_name": "Write",
                "file_path": "tests/test_analyzer.py",
                "is_new_file": True,
                "expected_files": ["src/analyzer.py", "tests/test_analyzer.py"],
            },
        ])

        assert result["expected_files_match_rate"] == 100.0
        assert result["unexpected_file_creations"] == 0
