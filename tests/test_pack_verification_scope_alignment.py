"""Tests for pack verification scope alignment analyzer."""

import pytest

from synthesis.pack_verification_scope_alignment import (
    analyze_pack_verification_scope_alignment,
    _calculate_alignment_score,
    _normalize_files,
    _average,
)


class TestAnalyzePackVerificationScopeAlignment:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_verification_scope_alignment([])

        assert result["total_packs"] == 0
        assert result["perfect_alignment_count"] == 0
        assert result["alignment_scores"] == {}
        assert result["avg_alignment_score"] == 0.0
        assert result["unexpected_files_count"] == 0
        assert result["missing_expected_count"] == 0
        assert result["drift_examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_scope_alignment(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_scope_alignment("not a list")

    def test_perfect_alignment_single_file(self):
        """Verify perfect alignment with single file."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/foo.py"],
                "task_title": "Test task",
            }
        ])

        assert result["total_packs"] == 1
        assert result["perfect_alignment_count"] == 1
        assert result["alignment_scores"]["pack1"] == 1.0
        assert result["avg_alignment_score"] == 1.0
        assert result["unexpected_files_count"] == 0
        assert result["missing_expected_count"] == 0

    def test_perfect_alignment_multiple_files(self):
        """Verify perfect alignment with multiple files."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py", "tests/test_foo.py"],
                "modified_files": ["src/foo.py", "src/bar.py", "tests/test_foo.py"],
            }
        ])

        assert result["perfect_alignment_count"] == 1
        assert result["alignment_scores"]["pack1"] == 1.0

    def test_unexpected_file_modification(self):
        """Verify detection of unexpected file modifications."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        assert result["perfect_alignment_count"] == 0
        assert result["unexpected_files_count"] == 1
        assert result["drift_examples"][0]["unexpected_files"] == ["src/bar.py"]

    def test_missing_expected_file(self):
        """Verify detection of missing expected files."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "modified_files": ["src/foo.py"],
            }
        ])

        assert result["perfect_alignment_count"] == 0
        assert result["missing_expected_count"] == 1
        assert result["drift_examples"][0]["missing_expected"] == ["src/bar.py"]

    def test_both_unexpected_and_missing_files(self):
        """Verify detection of both unexpected and missing files."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "modified_files": ["src/foo.py", "src/baz.py"],
            }
        ])

        assert result["unexpected_files_count"] == 1
        assert result["missing_expected_count"] == 1
        example = result["drift_examples"][0]
        assert "src/baz.py" in example["unexpected_files"]
        assert "src/bar.py" in example["missing_expected"]

    def test_no_overlap_returns_zero_alignment(self):
        """Verify complete mismatch returns zero alignment."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/bar.py"],
            }
        ])

        assert result["alignment_scores"]["pack1"] == 0.0
        assert result["unexpected_files_count"] == 1
        assert result["missing_expected_count"] == 1

    def test_empty_expected_files_with_modifications(self):
        """Verify empty expected files but files were modified."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "modified_files": ["src/foo.py"],
            }
        ])

        assert result["alignment_scores"]["pack1"] == 0.0
        assert result["unexpected_files_count"] == 1

    def test_expected_files_but_none_modified(self):
        """Verify expected files specified but none modified."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": [],
            }
        ])

        assert result["alignment_scores"]["pack1"] == 0.0
        assert result["missing_expected_count"] == 1

    def test_both_empty_lists_perfect_alignment(self):
        """Verify both empty lists is perfect alignment."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "modified_files": [],
            }
        ])

        # Both empty means perfect alignment but shouldn't count as perfect
        # since no expected files
        assert result["alignment_scores"]["pack1"] == 1.0
        assert result["perfect_alignment_count"] == 0

    def test_multiple_packs_average_score(self):
        """Verify average alignment score across multiple packs."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/foo.py"],  # Perfect: 1.0
            },
            {
                "pack_id": "pack2",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "modified_files": ["src/foo.py"],  # 1/2 = 0.5
            },
        ])

        assert result["total_packs"] == 2
        # Average of 1.0 and 0.5 = 0.75
        assert result["avg_alignment_score"] == 0.75

    def test_drift_examples_limited_to_five(self):
        """Verify drift examples are limited to 5."""
        records = [
            {
                "pack_id": f"pack{i}",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/bar.py"],  # All have drift
            }
            for i in range(10)
        ]

        result = analyze_pack_verification_scope_alignment(records)
        assert len(result["drift_examples"]) == 5

    def test_drift_example_structure(self):
        """Verify drift example contains expected fields."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "test_pack",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "modified_files": ["src/foo.py", "src/baz.py"],
                "task_title": "Test Task",
            }
        ])

        example = result["drift_examples"][0]
        assert example["pack_id"] == "test_pack"
        assert example["task_title"] == "Test Task"
        assert example["alignment_score"] == 0.333  # 1 matched / 3 total
        assert example["unexpected_files"] == ["src/baz.py"]
        assert example["missing_expected"] == ["src/bar.py"]
        assert example["matched_files_count"] == 1

    def test_missing_pack_id_uses_index(self):
        """Verify missing pack_id uses index."""
        result = analyze_pack_verification_scope_alignment([
            {
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/bar.py"],
            }
        ])

        assert "pack_0" in result["alignment_scores"]

    def test_missing_task_title_uses_unknown(self):
        """Verify missing task_title uses 'unknown'."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/bar.py"],
            }
        ])

        assert result["drift_examples"][0]["task_title"] == "unknown"

    def test_file_paths_normalized(self):
        """Verify file paths are normalized."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["./src/foo.py", "src\\bar.py"],
                "modified_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        # Should be perfect alignment after normalization
        assert result["alignment_scores"]["pack1"] == 1.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_scope_alignment([
            "not a dict",
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": ["src/foo.py"],
            },
        ])

        assert result["total_packs"] == 1

    def test_files_as_string_converted_to_list(self):
        """Verify single file as string is converted to list."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": "src/foo.py",
                "modified_files": "src/foo.py",
            }
        ])

        assert result["alignment_scores"]["pack1"] == 1.0

    def test_duplicate_files_handled(self):
        """Verify duplicate files are handled correctly."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/foo.py", "src/bar.py"],
                "modified_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        # Duplicates should be removed, so perfect match
        assert result["alignment_scores"]["pack1"] == 1.0

    def test_long_file_lists_truncated_in_examples(self):
        """Verify long file lists are truncated in examples."""
        many_files = [f"src/file{i}.py" for i in range(10)]
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "modified_files": many_files,
            }
        ])

        example = result["drift_examples"][0]
        assert len(example["unexpected_files"]) == 5


class TestCalculateAlignmentScore:
    """Test alignment score calculation helper."""

    def test_both_empty_returns_perfect(self):
        """Verify both empty sets return 1.0."""
        assert _calculate_alignment_score(set(), set()) == 1.0

    def test_one_empty_returns_zero(self):
        """Verify one empty set returns 0.0."""
        assert _calculate_alignment_score({"a"}, set()) == 0.0
        assert _calculate_alignment_score(set(), {"a"}) == 0.0

    def test_perfect_match_returns_one(self):
        """Verify perfect match returns 1.0."""
        assert _calculate_alignment_score({"a", "b"}, {"a", "b"}) == 1.0

    def test_no_overlap_returns_zero(self):
        """Verify no overlap returns 0.0."""
        assert _calculate_alignment_score({"a", "b"}, {"c", "d"}) == 0.0

    def test_partial_overlap_calculates_correctly(self):
        """Verify partial overlap calculates Jaccard similarity correctly."""
        # {a, b} and {b, c} -> intersection: {b}, union: {a, b, c}
        # Score = 1/3 = 0.333
        score = _calculate_alignment_score({"a", "b"}, {"b", "c"})
        assert score == 0.333

    def test_subset_calculates_correctly(self):
        """Verify subset calculates correctly."""
        # {a} and {a, b} -> intersection: {a}, union: {a, b}
        # Score = 1/2 = 0.5
        score = _calculate_alignment_score({"a"}, {"a", "b"})
        assert score == 0.5

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        # 1/7 = 0.142857... -> rounds to 0.143
        score = _calculate_alignment_score({"a", "b"}, {"b", "c", "d", "e", "f", "g"})
        assert score == 0.143


class TestNormalizeFiles:
    """Test file normalization helper."""

    def test_empty_list_returns_empty(self):
        """Verify empty list returns empty list."""
        assert _normalize_files([]) == []

    def test_none_returns_empty(self):
        """Verify None returns empty list."""
        assert _normalize_files(None) == []

    def test_single_string_converted_to_list(self):
        """Verify single string is converted to list."""
        assert _normalize_files("src/foo.py") == ["src/foo.py"]

    def test_list_of_strings_returned(self):
        """Verify list of strings is returned as-is (normalized)."""
        result = _normalize_files(["src/foo.py", "src/bar.py"])
        assert result == ["src/foo.py", "src/bar.py"]

    def test_tuple_converted_to_list(self):
        """Verify tuple is converted to list."""
        result = _normalize_files(("src/foo.py", "src/bar.py"))
        assert result == ["src/foo.py", "src/bar.py"]

    def test_leading_dot_slash_removed(self):
        """Verify leading ./ is removed."""
        assert _normalize_files(["./src/foo.py"]) == ["src/foo.py"]

    def test_backslashes_converted_to_forward_slashes(self):
        """Verify backslashes are converted to forward slashes."""
        assert _normalize_files(["src\\foo.py"]) == ["src/foo.py"]

    def test_whitespace_stripped(self):
        """Verify whitespace is stripped."""
        assert _normalize_files(["  src/foo.py  "]) == ["src/foo.py"]

    def test_empty_strings_filtered_out(self):
        """Verify empty strings are filtered out."""
        result = _normalize_files(["src/foo.py", "", "  ", "src/bar.py"])
        assert result == ["src/foo.py", "src/bar.py"]

    def test_non_string_items_filtered_out(self):
        """Verify non-string items are filtered out."""
        result = _normalize_files(["src/foo.py", 123, None, "src/bar.py"])
        assert result == ["src/foo.py", "src/bar.py"]

    def test_complex_normalization(self):
        """Verify complex normalization with multiple transformations."""
        result = _normalize_files(["./src\\foo.py", "  tests/test.py  ", "", "src/bar.py"])
        assert result == ["src/foo.py", "tests/test.py", "src/bar.py"]


class TestAverage:
    """Test average calculation helper."""

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0.0."""
        assert _average(10.0, 0) == 0.0

    def test_negative_count_returns_zero(self):
        """Verify negative count returns 0.0."""
        assert _average(10.0, -5) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(10.0, 4) == 2.5

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        assert _average(10.0, 3) == 3.333


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_scoped_pack_execution(self):
        """Simulate well-scoped pack with perfect alignment."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "task_123",
                "expected_files": [
                    "src/synthesis/new_analyzer.py",
                    "tests/test_new_analyzer.py",
                ],
                "modified_files": [
                    "src/synthesis/new_analyzer.py",
                    "tests/test_new_analyzer.py",
                ],
                "task_title": "Add new analyzer",
            }
        ])

        assert result["perfect_alignment_count"] == 1
        assert result["avg_alignment_score"] == 1.0
        assert len(result["drift_examples"]) == 0

    def test_scope_creep_scenario(self):
        """Simulate scope creep with unexpected modifications."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "task_123",
                "expected_files": ["src/foo.py"],
                "modified_files": [
                    "src/foo.py",
                    "src/bar.py",
                    "src/baz.py",
                    "src/utils.py",
                ],
                "task_title": "Simple fix",
            }
        ])

        assert result["unexpected_files_count"] == 3
        assert result["alignment_scores"]["task_123"] < 0.5

    def test_incomplete_execution_scenario(self):
        """Simulate incomplete execution missing expected files."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "task_123",
                "expected_files": [
                    "src/foo.py",
                    "src/bar.py",
                    "tests/test_foo.py",
                    "tests/test_bar.py",
                ],
                "modified_files": ["src/foo.py"],
                "task_title": "Partial implementation",
            }
        ])

        assert result["missing_expected_count"] == 3
        assert result["alignment_scores"]["task_123"] == 0.25

    def test_batch_execution_mixed_alignment(self):
        """Simulate batch execution with varying alignment."""
        result = analyze_pack_verification_scope_alignment([
            {
                "pack_id": "task_1",
                "expected_files": ["src/a.py"],
                "modified_files": ["src/a.py"],  # Perfect
            },
            {
                "pack_id": "task_2",
                "expected_files": ["src/b.py", "src/c.py"],
                "modified_files": ["src/b.py"],  # Partial
            },
            {
                "pack_id": "task_3",
                "expected_files": ["src/d.py"],
                "modified_files": ["src/e.py"],  # Complete drift
            },
        ])

        assert result["total_packs"] == 3
        assert result["perfect_alignment_count"] == 1
        # Scores: 1.0, 0.5, 0.0 -> avg = 0.5
        assert result["avg_alignment_score"] == 0.5
