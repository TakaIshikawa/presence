"""Tests for pack expected file accuracy analyzer."""

import pytest

from synthesis.pack_expected_file_accuracy import analyze_pack_expected_file_accuracy


class TestAnalyzePackExpectedFileAccuracy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_expected_file_accuracy([])

        assert result["total_packs"] == 0
        assert result["perfect_predictions"] == 0
        assert result["average_precision"] == 0.0
        assert result["average_recall"] == 0.0
        assert result["average_f1"] == 0.0
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

    def test_perfect_prediction(self):
        """Verify perfect prediction when expected matches changed."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        assert result["total_packs"] == 1
        assert result["perfect_predictions"] == 1
        assert result["average_precision"] == 1.0
        assert result["average_recall"] == 1.0
        assert result["average_f1"] == 1.0
        assert result["total_false_positives"] == 0
        assert result["total_false_negatives"] == 0

    def test_false_positives_detected(self):
        """Verify false positives (expected but not changed) are detected."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py", "src/bar.py", "src/baz.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["total_false_positives"] == 2
        assert result["average_precision"] < 1.0
        # Precision = 1 / 3 = 0.333
        assert result["average_precision"] == 0.333

    def test_false_negatives_detected(self):
        """Verify false negatives (changed but not expected) are detected."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py", "src/bar.py", "src/baz.py"],
            }
        ])

        assert result["total_false_negatives"] == 2
        assert result["average_recall"] < 1.0
        # Recall = 1 / 3 = 0.333
        assert result["average_recall"] == 0.333

    def test_f1_score_calculation(self):
        """Verify F1 score is calculated correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py", "src/baz.py"],
            }
        ])

        # Precision = 1/2 = 0.5, Recall = 1/2 = 0.5
        # F1 = 2 * (0.5 * 0.5) / (0.5 + 0.5) = 0.5
        assert result["average_precision"] == 0.5
        assert result["average_recall"] == 0.5
        assert result["average_f1"] == 0.5

    def test_no_overlap_zero_f1(self):
        """Verify zero F1 when no files match."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/bar.py"],
            }
        ])

        assert result["average_precision"] == 0.0
        assert result["average_recall"] == 0.0
        assert result["average_f1"] == 0.0

    def test_test_files_categorized(self):
        """Verify test files are categorized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["tests/test_foo.py"],
            }
        ])

        assert len(result["commonly_missed_categories"]) == 1
        assert result["commonly_missed_categories"][0]["category"] == "test"

    def test_config_files_categorized(self):
        """Verify config files are categorized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["package.json", "tsconfig.json"],
            }
        ])

        categories = [c["category"] for c in result["commonly_missed_categories"]]
        assert "config" in categories

    def test_type_files_categorized(self):
        """Verify type definition files are categorized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.ts"],
                "changed_files": ["src/foo.d.ts"],
            }
        ])

        assert result["commonly_missed_categories"][0]["category"] == "types"

    def test_docs_files_categorized(self):
        """Verify documentation files are categorized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["README.md", "docs/guide.md"],
            }
        ])

        categories = [c["category"] for c in result["commonly_missed_categories"]]
        assert "docs" in categories

    def test_source_files_categorized(self):
        """Verify regular source files are categorized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/bar.py"],
            }
        ])

        assert result["commonly_missed_categories"][0]["category"] == "source"

    def test_multiple_packs_analyzed(self):
        """Verify multiple packs are analyzed independently."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            },
            {
                "pack_id": "pack-2",
                "expected_files": ["src/bar.py"],
                "changed_files": ["src/baz.py"],
            }
        ])

        assert result["total_packs"] == 2
        assert result["perfect_predictions"] == 1

    def test_average_metrics_calculated(self):
        """Verify average metrics are calculated across packs."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            },
            {
                "pack_id": "pack-2",
                "expected_files": ["src/bar.py", "src/baz.py"],
                "changed_files": ["src/bar.py"],
            }
        ])

        # Pack 1: Precision 1.0, Recall 1.0
        # Pack 2: Precision 0.5, Recall 1.0
        # Average precision: (1.0 + 0.5) / 2 = 0.75
        # Average recall: (1.0 + 1.0) / 2 = 1.0
        assert result["average_precision"] == 0.75
        assert result["average_recall"] == 1.0

    def test_prediction_examples_collected(self):
        """Verify prediction examples are collected for low accuracy."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert len(result["prediction_examples"]) == 1
        example = result["prediction_examples"][0]
        assert example["pack_id"] == "pack-1"
        assert example["precision"] == 0.5
        assert "src/bar.py" in example["false_positives"]

    def test_examples_limited_to_five(self):
        """Verify prediction examples are capped at 5."""
        records = []
        for i in range(10):
            records.append({
                "pack_id": f"pack-{i}",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py"],
            })

        result = analyze_pack_expected_file_accuracy(records)

        assert len(result["prediction_examples"]) <= 5

    def test_category_counts_accumulated(self):
        """Verify category counts are accumulated across packs."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["tests/test_foo.py"],
            },
            {
                "pack_id": "pack-2",
                "expected_files": ["src/bar.py"],
                "changed_files": ["tests/test_bar.py"],
            }
        ])

        assert result["commonly_missed_categories"][0]["count"] == 2

    def test_commonly_missed_limited_to_five(self):
        """Verify commonly missed categories are capped at 5."""
        records = []
        for i in range(10):
            records.append({
                "pack_id": f"pack-{i}",
                "expected_files": ["src/foo.py"],
                "changed_files": [f"category{i}/file.py"],
            })

        result = analyze_pack_expected_file_accuracy(records)

        assert len(result["commonly_missed_categories"]) <= 5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_expected_file_accuracy([
            "not a dict",
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["total_packs"] == 1

    def test_empty_file_lists_skipped(self):
        """Verify packs with no expected or changed files are skipped."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": [],
                "changed_files": [],
            }
        ])

        assert result["total_packs"] == 0

    def test_file_path_normalization(self):
        """Verify file paths are normalized correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["./src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["perfect_predictions"] == 1

    def test_windows_path_normalization(self):
        """Verify Windows paths are normalized to forward slashes."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src\\foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["perfect_predictions"] == 1

    def test_single_file_as_string(self):
        """Verify single file as string is handled correctly."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": "src/foo.py",
                "changed_files": "src/foo.py",
            }
        ])

        assert result["perfect_predictions"] == 1

    def test_no_expected_files_perfect_precision(self):
        """Verify precision is 1.0 when no files expected."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": [],
                "changed_files": ["src/foo.py"],
            }
        ])

        # No expected files means precision = 1.0 (no false predictions)
        assert result["total_packs"] == 1
        assert result["average_precision"] == 1.0
        # But recall = 0.0 (all changed files were unexpected)
        assert result["average_recall"] == 0.0

    def test_no_changed_files_perfect_recall(self):
        """Verify recall is 1.0 when no files changed."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py"],
                "changed_files": [],
            }
        ])

        # No changed files means recall = 1.0 (no missed files)
        assert result["total_packs"] == 1
        assert result["average_recall"] == 1.0
        # But precision = 0.0 (expected file not changed)
        assert result["average_precision"] == 0.0

    def test_task_title_included_in_examples(self):
        """Verify task title is included in prediction examples."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "task_title": "Add feature X",
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["prediction_examples"][0]["task_title"] == "Add feature X"

    def test_missing_pack_id_uses_index(self):
        """Verify missing pack_id uses index as fallback."""
        result = analyze_pack_expected_file_accuracy([
            {
                "expected_files": ["src/foo.py", "src/bar.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert result["prediction_examples"][0]["pack_id"] == "pack_0"

    def test_false_positives_limited_in_examples(self):
        """Verify false positives list is limited to 3 in examples."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["a.py", "b.py", "c.py", "d.py", "e.py"],
                "changed_files": [],
            }
        ])

        # Has a pack with low accuracy
        assert result["total_packs"] == 1
        assert len(result["prediction_examples"]) == 1
        # False positives should be limited to 3
        assert len(result["prediction_examples"][0]["false_positives"]) == 3

    def test_false_negatives_limited_in_examples(self):
        """Verify false negatives list is limited to 3 in examples."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": [],
                "changed_files": ["a.py", "b.py", "c.py", "d.py", "e.py"],
            }
        ])

        # Has a pack with low recall
        assert result["total_packs"] == 1
        assert len(result["prediction_examples"]) == 1
        # False negatives should be limited to 3
        assert len(result["prediction_examples"][0]["false_negatives"]) == 3

    def test_high_accuracy_not_in_examples(self):
        """Verify high accuracy packs are not included in examples."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/foo.py", "src/bar.py", "src/baz.py", "src/qux.py"],
                "changed_files": ["src/foo.py", "src/bar.py", "src/baz.py"],
            }
        ])

        # Precision = 3/4 = 0.75, should not be in examples
        assert len(result["prediction_examples"]) == 0

    def test_case_sensitive_file_matching(self):
        """Verify file matching is case-sensitive."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack-1",
                "expected_files": ["src/Foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        # Different case - should be treated as different files
        assert result["perfect_predictions"] == 0
        assert result["total_false_positives"] == 1
        assert result["total_false_negatives"] == 1
