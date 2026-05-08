"""Tests for pack expected file accuracy analyzer."""

import pytest

from synthesis.pack_expected_file_accuracy import (
    analyze_pack_expected_file_accuracy,
<<<<<<< HEAD
    _calculate_precision,
    _calculate_recall,
    _calculate_f1_score,
    _categorize_file,
    _normalize_files,
    _average,
=======
    _auto_categorize_file,
    _calculate_f1_score,
    _calculate_precision,
    _calculate_recall,
    _classify_accuracy_pattern,
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
)


class TestAnalyzePackExpectedFileAccuracy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
<<<<<<< HEAD
        result = analyze_pack_expected_file_accuracy([])

        assert result["total_packs"] == 0
        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 0.0
        assert result["avg_f1_score"] == 0.0
        assert result["perfect_predictions_count"] == 0
        assert result["total_false_positives"] == 0
        assert result["total_false_negatives"] == 0
        assert result["commonly_missed_categories"] == []
        assert result["prediction_examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_expected_file_accuracy(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_expected_file_accuracy("not a list")

    def test_perfect_prediction_single_file(self):
        """Verify perfect prediction with single file."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/foo.py"],
                "task_title": "Test task",
            }
        ])

        assert result["total_packs"] == 1
        assert result["perfect_predictions_count"] == 1
        assert result["avg_precision"] == 1.0
        assert result["avg_recall"] == 1.0
        assert result["avg_f1_score"] == 1.0
        assert result["total_false_positives"] == 0
        assert result["total_false_negatives"] == 0

    def test_perfect_prediction_multiple_files(self):
        """Verify perfect prediction with multiple files."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py", "tests/test_foo.py"],
                "actual_files": ["src/foo.py", "src/bar.py", "tests/test_foo.py"],
            }
        ])

        assert result["perfect_predictions_count"] == 1
        assert result["avg_f1_score"] == 1.0

    def test_false_positive_detection(self):
        """Verify detection of false positives (expected but not modified)."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "actual_files": ["src/foo.py"],
            }
        ])

        assert result["total_false_positives"] == 1
        assert result["avg_precision"] == 0.5  # 1 TP / 2 expected
        assert result["avg_recall"] == 1.0  # 1 TP / 1 actual
        example = result["prediction_examples"][0]
        assert "src/bar.py" in example["false_positives"]

    def test_false_negative_detection(self):
        """Verify detection of false negatives (modified but not expected)."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        assert result["total_false_negatives"] == 1
        assert result["avg_precision"] == 1.0  # 1 TP / 1 expected
        assert result["avg_recall"] == 0.5  # 1 TP / 2 actual
        example = result["prediction_examples"][0]
        assert "src/bar.py" in example["false_negatives"]

    def test_both_false_positives_and_negatives(self):
        """Verify detection of both false positives and negatives."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "actual_files": ["src/foo.py", "src/baz.py"],
            }
        ])

        assert result["total_false_positives"] == 1
        assert result["total_false_negatives"] == 1
        assert result["avg_precision"] == 0.5
        assert result["avg_recall"] == 0.5
        # F1 = 2 * (0.5 * 0.5) / (0.5 + 0.5) = 0.5
        assert result["avg_f1_score"] == 0.5

    def test_complete_mismatch_returns_zero_scores(self):
        """Verify complete mismatch returns zero scores."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/bar.py"],
            }
        ])

        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 0.0
        assert result["avg_f1_score"] == 0.0
        assert result["total_false_positives"] == 1
        assert result["total_false_negatives"] == 1

    def test_empty_expected_files_with_actual_modifications(self):
        """Verify empty expected files but files were modified."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "actual_files": ["src/foo.py"],
            }
        ])

        assert result["avg_precision"] == 1.0  # Edge case: no expected = perfect precision
        assert result["avg_recall"] == 0.0
        assert result["total_false_negatives"] == 1

    def test_expected_files_but_none_modified(self):
        """Verify expected files specified but none modified."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": [],
            }
        ])

        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 1.0  # Edge case: no actual = perfect recall
        assert result["total_false_positives"] == 1

    def test_both_empty_lists(self):
        """Verify both empty lists returns edge case metrics."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "actual_files": [],
            }
        ])

        # Edge case: both empty means precision and recall = 1.0
        assert result["avg_precision"] == 1.0
        assert result["avg_recall"] == 1.0
        assert result["avg_f1_score"] == 1.0
        # But not counted as perfect prediction since no expected files
        assert result["perfect_predictions_count"] == 0

    def test_multiple_packs_average_metrics(self):
        """Verify average metrics across multiple packs."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/foo.py"],  # Perfect: P=1.0, R=1.0, F1=1.0
            },
            {
                "pack_id": "pack2",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "actual_files": ["src/foo.py"],  # P=0.5, R=1.0, F1=0.667
            },
        ])

        assert result["total_packs"] == 2
        # Avg precision: (1.0 + 0.5) / 2 = 0.75
        assert result["avg_precision"] == 0.75
        # Avg recall: (1.0 + 1.0) / 2 = 1.0
        assert result["avg_recall"] == 1.0
        # Avg F1: (1.0 + 0.667) / 2 = 0.834
        assert result["avg_f1_score"] == 0.834

    def test_commonly_missed_categories_tracked(self):
        """Verify commonly missed file categories are tracked."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                    "config/settings.json",
                    "types/foo.d.ts",
                ],
            }
        ])

        categories = result["commonly_missed_categories"]
        category_names = [c["category"] for c in categories]
        assert "test" in category_names
        assert "config" in category_names
        assert "types" in category_names

    def test_commonly_missed_categories_sorted_by_frequency(self):
        """Verify commonly missed categories are sorted by frequency."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                    "tests/test_bar.py",
                    "tests/test_baz.py",
                    "config/settings.json",
                ],
            }
        ])

        categories = result["commonly_missed_categories"]
        # Test category should be first (3 occurrences)
        assert categories[0]["category"] == "test"
        assert categories[0]["count"] == 3

    def test_commonly_missed_categories_limited_to_five(self):
        """Verify commonly missed categories are limited to 5."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": [
                    "src/foo.py",
                    "tests/test1.py",
                    "config/c1.json",
                    "types/t1.d.ts",
                    "docs/d1.md",
                    "src/other1.py",
                    "src/other2.py",
                ],
            }
        ])

        assert len(result["commonly_missed_categories"]) <= 5

    def test_prediction_examples_structure(self):
        """Verify prediction examples contain expected fields."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "test_pack",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "actual_files": ["src/foo.py", "src/baz.py"],
                "task_title": "Test Task",
            }
        ])

        example = result["prediction_examples"][0]
        assert example["pack_id"] == "test_pack"
        assert example["task_title"] == "Test Task"
        assert example["precision"] == 0.5
        assert example["recall"] == 0.5
        assert example["f1_score"] == 0.5
        assert example["false_positives"] == ["src/bar.py"]
        assert example["false_negatives"] == ["src/baz.py"]
        assert example["true_positives_count"] == 1

    def test_prediction_examples_limited_to_five(self):
        """Verify prediction examples are limited to 5."""
        records = [
            {
                "pack_id": f"pack{i}",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/bar.py"],  # All have mismatches
            }
            for i in range(10)
        ]

        result = analyze_pack_expected_file_accuracy(records)
        assert len(result["prediction_examples"]) == 5

    def test_missing_pack_id_uses_index(self):
        """Verify missing pack_id uses index."""
        result = analyze_pack_expected_file_accuracy([
            {
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/foo.py"],
            }
        ])

        assert result["total_packs"] == 1
        example = result["prediction_examples"][0]
        assert example["pack_id"] == "pack_0"

    def test_missing_task_title_uses_unknown(self):
        """Verify missing task_title uses 'unknown'."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/bar.py"],
            }
        ])

        assert result["prediction_examples"][0]["task_title"] == "unknown"

    def test_file_paths_normalized(self):
        """Verify file paths are normalized."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["./src/foo.py", "src\\bar.py"],
                "actual_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        # Should be perfect match after normalization
        assert result["avg_f1_score"] == 1.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_expected_file_accuracy([
            "not a dict",
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": ["src/foo.py"],
            },
        ])

        assert result["total_packs"] == 1

    def test_files_as_string_converted_to_list(self):
        """Verify single file as string is converted to list."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": "src/foo.py",
                "actual_files": "src/foo.py",
            }
        ])

        assert result["avg_f1_score"] == 1.0

    def test_duplicate_files_handled(self):
        """Verify duplicate files are handled correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "src/foo.py", "src/bar.py"],
                "actual_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        # Duplicates should be removed, so perfect match
        assert result["avg_f1_score"] == 1.0

    def test_long_file_lists_truncated_in_examples(self):
        """Verify long file lists are truncated in examples."""
        many_files = [f"src/file{i}.py" for i in range(10)]
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "actual_files": many_files,
            }
        ])

        example = result["prediction_examples"][0]
        assert len(example["false_negatives"]) == 5


class TestCalculatePrecision:
    """Test precision calculation helper."""

    def test_zero_expected_returns_one(self):
        """Verify zero expected files returns 1.0 (edge case)."""
        assert _calculate_precision(0, 0) == 1.0

    def test_perfect_precision_returns_one(self):
        """Verify perfect precision returns 1.0."""
        assert _calculate_precision(5, 5) == 1.0

    def test_partial_precision_calculated_correctly(self):
        """Verify partial precision is calculated correctly."""
        # 3 true positives out of 6 expected = 0.5
        assert _calculate_precision(3, 6) == 0.5

    def test_zero_true_positives_returns_zero(self):
        """Verify zero true positives returns 0.0."""
        assert _calculate_precision(0, 5) == 0.0

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        # 1 TP / 3 expected = 0.333...
        assert _calculate_precision(1, 3) == 0.333


class TestCalculateRecall:
    """Test recall calculation helper."""

    def test_zero_actual_returns_one(self):
        """Verify zero actual files returns 1.0 (edge case)."""
        assert _calculate_recall(0, 0) == 1.0

    def test_perfect_recall_returns_one(self):
        """Verify perfect recall returns 1.0."""
        assert _calculate_recall(5, 5) == 1.0

    def test_partial_recall_calculated_correctly(self):
        """Verify partial recall is calculated correctly."""
        # 3 true positives out of 6 actual = 0.5
        assert _calculate_recall(3, 6) == 0.5

    def test_zero_true_positives_returns_zero(self):
        """Verify zero true positives returns 0.0."""
        assert _calculate_recall(0, 5) == 0.0

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        # 1 TP / 3 actual = 0.333...
        assert _calculate_recall(1, 3) == 0.333


class TestCalculateF1Score:
    """Test F1 score calculation helper."""

    def test_both_zero_returns_zero(self):
        """Verify both precision and recall zero returns 0.0."""
        assert _calculate_f1_score(0.0, 0.0) == 0.0

    def test_perfect_f1_score_returns_one(self):
        """Verify perfect F1 score returns 1.0."""
        assert _calculate_f1_score(1.0, 1.0) == 1.0

    def test_f1_score_calculated_correctly(self):
        """Verify F1 score is calculated correctly."""
        # P=0.5, R=1.0 -> F1 = 2 * (0.5 * 1.0) / (0.5 + 1.0) = 0.667
        assert _calculate_f1_score(0.5, 1.0) == 0.667

    def test_f1_score_balanced_case(self):
        """Verify F1 score for balanced precision and recall."""
        # P=0.75, R=0.75 -> F1 = 2 * (0.75 * 0.75) / (0.75 + 0.75) = 0.75
        assert _calculate_f1_score(0.75, 0.75) == 0.75

    def test_result_rounded_to_three_decimals(self):
        """Verify result is rounded to 3 decimal places."""
        # P=1/3, R=1/3 -> F1 = 0.333...
        assert _calculate_f1_score(0.333, 0.333) == 0.333


class TestCategorizeFile:
    """Test file categorization helper."""

    def test_test_file_categorized(self):
        """Verify test files are categorized correctly."""
        assert _categorize_file("tests/test_foo.py") == "test"
        assert _categorize_file("src/foo_test.py") == "test"
        assert _categorize_file("test_helper.py") == "test"

    def test_config_file_categorized(self):
        """Verify config files are categorized correctly."""
        assert _categorize_file("config/settings.json") == "config"
        assert _categorize_file("app.yaml") == "config"
        assert _categorize_file("pyproject.toml") == "config"
        assert _categorize_file("settings.ini") == "config"

    def test_types_file_categorized(self):
        """Verify type definition files are categorized correctly."""
        assert _categorize_file("types/foo.d.ts") == "types"
        assert _categorize_file("src/types/bar.d.ts") == "types"
        assert _categorize_file("stubs/foo.pyi") == "types"

    def test_docs_file_categorized(self):
        """Verify documentation files are categorized correctly."""
        assert _categorize_file("README.md") == "docs"
        assert _categorize_file("docs/guide.rst") == "docs"
        assert _categorize_file("CHANGELOG.txt") == "docs"

    def test_other_file_categorized(self):
        """Verify other files are categorized as 'other'."""
        assert _categorize_file("src/main.py") == "other"
        assert _categorize_file("lib/utils.js") == "other"


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
=======
        result = analyze_pack_expected_file_accuracy({})

        assert result["expected_files"] == []
        assert result["changed_files"] == []
        assert result["correctly_expected"] == []
        assert result["false_positives"] == []
        assert result["false_negatives"] == []
        assert result["precision"] == 0.0
        assert result["recall"] == 0.0
        assert result["f1_score"] == 0.0
        assert result["missed_categories"] == []
        assert result["accuracy_pattern"] == "empty"

    def test_none_input_treated_as_empty_dict(self):
        """Verify None input is treated as empty dict."""
        result = analyze_pack_expected_file_accuracy(None)
        assert result["precision"] == 0.0

    def test_invalid_input_type_raises_error(self):
        """Verify non-dict input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a dictionary"):
            analyze_pack_expected_file_accuracy("not a dict")

    def test_perfect_match(self):
        """Verify perfect match between expected and changed."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/main.py", "src/utils.py"],
            "changed_files": ["src/main.py", "src/utils.py"],
        })

        assert result["correctly_expected"] == ["src/main.py", "src/utils.py"]
        assert result["false_positives"] == []
        assert result["false_negatives"] == []
        assert result["precision"] == 100.0
        assert result["recall"] == 100.0
        assert result["f1_score"] == 100.0
        assert result["accuracy_pattern"] == "perfect"

    def test_partial_match(self):
        """Verify partial match calculates metrics correctly."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/main.py", "src/utils.py", "src/config.py"],
            "changed_files": ["src/main.py", "src/utils.py"],
        })

        assert result["correctly_expected"] == ["src/main.py", "src/utils.py"]
        assert result["false_positives"] == ["src/config.py"]
        assert result["false_negatives"] == []
        assert result["precision"] == pytest.approx(66.67, abs=0.01)
        assert result["recall"] == 100.0

    def test_over_predicted(self):
        """Verify over-prediction detection."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["a.py", "b.py", "c.py", "d.py"],
            "changed_files": ["a.py", "b.py"],
        })

        assert len(result["false_positives"]) == 2
        assert result["accuracy_pattern"] == "over_predicted"

    def test_under_predicted(self):
        """Verify under-prediction detection."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/main.py"],
            "changed_files": ["src/main.py", "tests/test_main.py", "src/utils.py"],
        })

        assert len(result["false_negatives"]) == 2
        assert "tests/test_main.py" in result["false_negatives"]
        assert "src/utils.py" in result["false_negatives"]
        assert result["accuracy_pattern"] == "under_predicted"

    def test_no_match(self):
        """Verify complete mismatch."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/foo.py"],
            "changed_files": ["src/bar.py"],
        })

        assert result["correctly_expected"] == []
        assert result["precision"] == 0.0
        assert result["recall"] == 0.0
        assert result["f1_score"] == 0.0
        assert result["accuracy_pattern"] == "poor"

    def test_missed_categories_detection(self):
        """Verify missed file categories are identified."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/main.py"],
            "changed_files": [
                "src/main.py",
                "tests/test_main.py",
                "tests/test_utils.py",
                "package.json",
            ],
        })

        categories = result["missed_categories"]
        assert len(categories) >= 2
        # Should detect test and config categories
        category_names = [c["category"] for c in categories]
        assert "test" in category_names
        assert "config" in category_names

    def test_file_path_normalization(self):
        """Verify file paths are normalized."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["./src/main.py", "src\\utils.py"],
            "changed_files": ["src/main.py", "src/utils.py"],
        })

        assert result["precision"] == 100.0
        assert result["recall"] == 100.0

    def test_accurate_pattern_classification(self):
        """Verify accurate pattern (high precision and recall)."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["a.py", "b.py", "c.py", "d.py", "e.py"],
            "changed_files": ["a.py", "b.py", "c.py", "d.py"],
        })

        # 4/5 expected changed = 80% precision
        # 4/4 changed were expected = 100% recall
        assert result["precision"] == 80.0
        assert result["recall"] == 100.0
        assert result["accuracy_pattern"] == "accurate"

    def test_f1_score_calculation(self):
        """Verify F1 score is calculated correctly."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["a.py", "b.py", "c.py"],
            "changed_files": ["a.py", "b.py", "d.py"],
        })

        # Precision: 2/3 = 66.67%
        # Recall: 2/3 = 66.67%
        # F1: 2 * (66.67 * 66.67) / (66.67 + 66.67) = 66.67
        assert result["precision"] == pytest.approx(66.67, abs=0.01)
        assert result["recall"] == pytest.approx(66.67, abs=0.01)
        assert result["f1_score"] == pytest.approx(66.67, abs=0.01)

    def test_custom_file_categories(self):
        """Verify custom file categories are used."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["a.py"],
            "changed_files": ["a.py", "b.py", "c.py"],
            "file_categories": {
                "b.py": "custom_category",
                "c.py": "custom_category",
            },
        })

        categories = result["missed_categories"]
        assert len(categories) == 1
        assert categories[0]["category"] == "custom_category"
        assert categories[0]["count"] == 2


class TestHelperFunctions:
    """Test helper functions."""

    def test_calculate_precision_normal(self):
        """Verify precision calculation."""
        assert _calculate_precision(8, 10) == 80.0
        assert _calculate_precision(3, 4) == 75.0

    def test_calculate_precision_zero_expected(self):
        """Verify precision with zero expected files."""
        assert _calculate_precision(0, 0) == 0.0

    def test_calculate_precision_rounding(self):
        """Verify precision is rounded to 2 decimals."""
        assert _calculate_precision(1, 3) == 33.33

    def test_calculate_recall_normal(self):
        """Verify recall calculation."""
        assert _calculate_recall(8, 10) == 80.0
        assert _calculate_recall(2, 5) == 40.0

    def test_calculate_recall_zero_changed(self):
        """Verify recall with zero changed files."""
        assert _calculate_recall(0, 0) == 0.0

    def test_calculate_recall_rounding(self):
        """Verify recall is rounded to 2 decimals."""
        assert _calculate_recall(2, 3) == 66.67

    def test_calculate_f1_score_normal(self):
        """Verify F1 score calculation."""
        # Precision=80, Recall=80 -> F1=80
        assert _calculate_f1_score(80.0, 80.0) == 80.0

    def test_calculate_f1_score_different_values(self):
        """Verify F1 score with different precision/recall."""
        # Precision=75, Recall=60 -> F1 = 2*(75*60)/(75+60) = 66.67
        assert _calculate_f1_score(75.0, 60.0) == pytest.approx(66.67, abs=0.01)

    def test_calculate_f1_score_zero_values(self):
        """Verify F1 score with zero values."""
        assert _calculate_f1_score(0.0, 0.0) == 0.0

    def test_auto_categorize_file_test(self):
        """Verify test file categorization."""
        assert _auto_categorize_file("tests/test_main.py") == "test"
        assert _auto_categorize_file("test_utils.py") == "test"
        assert _auto_categorize_file("src/main_test.py") == "test"

    def test_auto_categorize_file_config(self):
        """Verify config file categorization."""
        assert _auto_categorize_file("package.json") == "config"
        assert _auto_categorize_file("tsconfig.json") == "config"
        assert _auto_categorize_file("pyproject.toml") == "config"
        assert _auto_categorize_file("Dockerfile") == "config"

    def test_auto_categorize_file_types(self):
        """Verify type definition categorization."""
        assert _auto_categorize_file("src/types.d.ts") == "types"
        assert _auto_categorize_file("src/main.types.ts") == "types"
        assert _auto_categorize_file("stubs/module.pyi") == "types"

    def test_auto_categorize_file_docs(self):
        """Verify documentation categorization."""
        assert _auto_categorize_file("README.md") == "docs"
        assert _auto_categorize_file("docs/guide.rst") == "docs"

    def test_auto_categorize_file_source(self):
        """Verify source file categorization (default)."""
        assert _auto_categorize_file("src/main.py") == "source"
        assert _auto_categorize_file("lib/utils.ts") == "source"

    def test_classify_accuracy_pattern_perfect(self):
        """Verify perfect pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=100.0,
            recall=100.0,
            false_positive_count=0,
            false_negative_count=0,
        )
        assert pattern == "perfect"

    def test_classify_accuracy_pattern_accurate(self):
        """Verify accurate pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=85.0,
            recall=90.0,
            false_positive_count=1,
            false_negative_count=1,
        )
        assert pattern == "accurate"

    def test_classify_accuracy_pattern_over_predicted(self):
        """Verify over-predicted pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=50.0,
            recall=80.0,
            false_positive_count=5,
            false_negative_count=1,
        )
        assert pattern == "over_predicted"

    def test_classify_accuracy_pattern_under_predicted(self):
        """Verify under-predicted pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=80.0,
            recall=50.0,
            false_positive_count=1,
            false_negative_count=5,
        )
        assert pattern == "under_predicted"

    def test_classify_accuracy_pattern_poor(self):
        """Verify poor pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=30.0,
            recall=40.0,
            false_positive_count=3,
            false_negative_count=3,
        )
        assert pattern == "poor"

    def test_classify_accuracy_pattern_empty(self):
        """Verify empty pattern classification."""
        pattern = _classify_accuracy_pattern(
            precision=0.0,
            recall=0.0,
            false_positive_count=0,
            false_negative_count=0,
        )
        assert pattern == "empty"
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

<<<<<<< HEAD
    def test_perfect_prediction_scenario(self):
        """Simulate perfect file prediction."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": [
                    "src/synthesis/new_analyzer.py",
                    "tests/test_new_analyzer.py",
                ],
                "actual_files": [
                    "src/synthesis/new_analyzer.py",
                    "tests/test_new_analyzer.py",
                ],
                "task_title": "Add new analyzer",
            }
        ])

        assert result["perfect_predictions_count"] == 1
        assert result["avg_precision"] == 1.0
        assert result["avg_recall"] == 1.0
        assert result["avg_f1_score"] == 1.0

    def test_over_estimation_scenario(self):
        """Simulate over-estimation (expected more than actually changed)."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": [
                    "src/foo.py",
                    "src/bar.py",
                    "src/baz.py",
                    "tests/test_all.py",
                ],
                "actual_files": ["src/foo.py", "src/bar.py"],
                "task_title": "Simple fix",
            }
        ])

        # Precision: 2/4 = 0.5 (only 2 out of 4 expected were actually modified)
        # Recall: 2/2 = 1.0 (all actual modifications were expected)
        assert result["avg_precision"] == 0.5
        assert result["avg_recall"] == 1.0
        assert result["total_false_positives"] == 2

    def test_under_estimation_scenario(self):
        """Simulate under-estimation (more files changed than expected)."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": ["src/foo.py"],
                "actual_files": [
                    "src/foo.py",
                    "src/bar.py",
                    "tests/test_foo.py",
                    "tests/test_bar.py",
                ],
                "task_title": "Add feature",
            }
        ])

        # Precision: 1/1 = 1.0 (all expected were modified)
        # Recall: 1/4 = 0.25 (only 1 out of 4 actual modifications were expected)
        assert result["avg_precision"] == 1.0
        assert result["avg_recall"] == 0.25
        assert result["total_false_negatives"] == 3

    def test_missed_test_files_scenario(self):
        """Simulate commonly missed test files."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": ["src/feature.py"],
                "actual_files": [
                    "src/feature.py",
                    "tests/test_feature.py",
                    "tests/test_integration.py",
                ],
                "task_title": "Add feature",
            }
        ])

        categories = result["commonly_missed_categories"]
        assert any(c["category"] == "test" for c in categories)
        # Test category should have count of 2
        test_category = next(c for c in categories if c["category"] == "test")
        assert test_category["count"] == 2

    def test_mixed_accuracy_batch(self):
        """Simulate batch execution with varying prediction accuracy."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_1",
                "expected_files": ["src/a.py"],
                "actual_files": ["src/a.py"],  # Perfect: P=1.0, R=1.0, F1=1.0
            },
            {
                "pack_id": "task_2",
                "expected_files": ["src/b.py", "src/c.py"],
                "actual_files": ["src/b.py"],  # P=0.5, R=1.0, F1=0.667
            },
            {
                "pack_id": "task_3",
                "expected_files": ["src/d.py"],
                "actual_files": ["src/e.py"],  # P=0.0, R=0.0, F1=0.0
            },
        ])

        assert result["total_packs"] == 3
        assert result["perfect_predictions_count"] == 1
        # Avg precision: (1.0 + 0.5 + 0.0) / 3 = 0.5
        assert result["avg_precision"] == 0.5
        # Avg recall: (1.0 + 1.0 + 0.0) / 3 = 0.667
        assert result["avg_recall"] == 0.667
        # Avg F1: (1.0 + 0.667 + 0.0) / 3 = 0.556
        assert result["avg_f1_score"] == 0.556
=======
    def test_well_planned_pack(self):
        """Simulate well-planned pack with accurate predictions."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": [
                "src/main.py",
                "src/utils.py",
                "tests/test_main.py",
            ],
            "changed_files": [
                "src/main.py",
                "src/utils.py",
                "tests/test_main.py",
            ],
        })

        assert result["accuracy_pattern"] == "perfect"
        assert result["f1_score"] == 100.0

    def test_forgot_test_files(self):
        """Simulate common pattern of forgetting test files."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": ["src/main.py", "src/utils.py"],
            "changed_files": [
                "src/main.py",
                "src/utils.py",
                "tests/test_main.py",
                "tests/test_utils.py",
            ],
        })

        assert result["accuracy_pattern"] == "under_predicted"
        categories = result["missed_categories"]
        assert categories[0]["category"] == "test"
        assert categories[0]["count"] == 2

    def test_over_estimated_scope(self):
        """Simulate over-estimated scope with unnecessary files."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": [
                "src/main.py",
                "src/utils.py",
                "src/config.py",
                "src/types.py",
                "tests/test_main.py",
            ],
            "changed_files": [
                "src/main.py",
                "tests/test_main.py",
            ],
        })

        assert result["accuracy_pattern"] == "over_predicted"
        assert len(result["false_positives"]) == 3

    def test_mixed_accuracy(self):
        """Simulate mixed accuracy with some correct, some missed."""
        result = analyze_pack_expected_file_accuracy({
            "expected_files": [
                "src/main.py",
                "src/utils.py",
                "src/unused.py",
            ],
            "changed_files": [
                "src/main.py",
                "src/utils.py",
                "tests/test_main.py",
                "package.json",
            ],
        })

        assert len(result["correctly_expected"]) == 2
        assert len(result["false_positives"]) == 1
        assert len(result["false_negatives"]) == 2
        assert result["precision"] == pytest.approx(66.67, abs=0.01)
        assert result["recall"] == 50.0
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
