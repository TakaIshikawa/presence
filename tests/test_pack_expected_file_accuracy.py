"""Tests for pack expected file accuracy analyzer."""

import pytest

from synthesis.pack_expected_file_accuracy import (
    analyze_pack_expected_file_accuracy,
    _precision,
    _recall,
    _f1_score,
    _categorize_file,
    _normalize_files,
)


class TestAnalyzePackExpectedFileAccuracy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_pack_expected_file_accuracy([])

        assert result["total_packs"] == 0
        assert result["perfect_predictions"] == 0
        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 0.0
        assert result["avg_f1_score"] == 0.0
        assert result["false_positives_count"] == 0
        assert result["false_negatives_count"] == 0
        assert result["commonly_missed_types"] == []
        assert result["examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_expected_file_accuracy(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_expected_file_accuracy("not a list")

    def test_perfect_prediction(self):
        """Verify perfect prediction metrics."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py", "tests/test_foo.py"],
                "changed_files": ["src/foo.py", "tests/test_foo.py"],
            }
        ])

        assert result["total_packs"] == 1
        assert result["perfect_predictions"] == 1
        assert result["avg_precision"] == 100.0
        assert result["avg_recall"] == 100.0
        assert result["avg_f1_score"] == 100.0
        assert result["false_positives_count"] == 0
        assert result["false_negatives_count"] == 0

    def test_over_prediction_low_precision(self):
        """Verify over-prediction scenario with low precision."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/a.py", "src/b.py", "src/c.py"],
                "changed_files": ["src/a.py"],  # Only 1 of 3 expected
            }
        ])

        # Precision: 1/3 = 33.33%
        assert result["avg_precision"] == 33.33
        # Recall: 1/1 = 100%
        assert result["avg_recall"] == 100.0
        assert result["false_positives_count"] == 2
        assert result["false_negatives_count"] == 0

    def test_under_prediction_low_recall(self):
        """Verify under-prediction scenario with low recall."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py", "src/bar.py", "src/baz.py"],
            }
        ])

        # Precision: 1/1 = 100%
        assert result["avg_precision"] == 100.0
        # Recall: 1/3 = 33.33%
        assert result["avg_recall"] == 33.33
        assert result["false_positives_count"] == 0
        assert result["false_negatives_count"] == 2

    def test_no_overlap_zero_metrics(self):
        """Verify complete mismatch returns zero metrics."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/a.py"],
                "changed_files": ["src/b.py"],
            }
        ])

        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 0.0
        assert result["avg_f1_score"] == 0.0
        assert result["false_positives_count"] == 1
        assert result["false_negatives_count"] == 1

    def test_multiple_packs_averaged(self):
        """Verify metrics are averaged across multiple packs."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/a.py"],
                "changed_files": ["src/a.py"],  # Perfect: 100% precision, 100% recall
            },
            {
                "pack_id": "pack2",
                "expected_files": ["src/b.py", "src/c.py"],
                "changed_files": ["src/b.py"],  # 50% precision, 100% recall
            },
        ])

        # Avg precision: (100 + 50) / 2 = 75
        assert result["avg_precision"] == 75.0
        # Avg recall: (100 + 100) / 2 = 100
        assert result["avg_recall"] == 100.0

    def test_commonly_missed_types_detected(self):
        """Verify commonly missed file types are detected."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py", "tests/test_foo.py", "config.json"],
            }
        ])

        missed_types = result["commonly_missed_types"]
        assert len(missed_types) == 2
        # Should detect test and config as missed types
        types = [item["file_type"] for item in missed_types]
        assert "test" in types
        assert "config" in types

    def test_examples_collected(self):
        """Verify examples of imperfect predictions are collected."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/a.py", "src/b.py"],
                "changed_files": ["src/a.py"],
                "task_title": "Test task",
            }
        ])

        assert len(result["examples"]) == 1
        example = result["examples"][0]
        assert example["pack_id"] == "pack1"
        assert example["task_title"] == "Test task"
        assert example["precision"] == 50.0
        assert example["recall"] == 100.0
        assert "src/b.py" in example["false_positives"]

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        packs = [
            {
                "pack_id": f"pack{i}",
                "expected_files": ["src/a.py"],
                "changed_files": ["src/b.py"],  # All imperfect
            }
            for i in range(10)
        ]

        result = analyze_pack_expected_file_accuracy(packs)
        assert len(result["examples"]) == 5

    def test_perfect_prediction_not_in_examples(self):
        """Verify perfect predictions are not included in examples."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            }
        ])

        assert len(result["examples"]) == 0

    def test_file_paths_normalized(self):
        """Verify file paths are normalized."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["./src/foo.py", "src\\bar.py"],
                "changed_files": ["src/foo.py", "src/bar.py"],
            }
        ])

        # Should be perfect match after normalization
        assert result["perfect_predictions"] == 1

    def test_empty_expected_files(self):
        """Verify handling of empty expected files."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "changed_files": ["src/foo.py"],
            }
        ])

        # Precision: 100% (0 expected, 0 TP - nothing expected, nothing matched)
        # Recall: 0% (0 of 1 actual was expected)
        assert result["avg_precision"] == 100.0
        assert result["avg_recall"] == 0.0

    def test_empty_changed_files(self):
        """Verify handling of empty changed files."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": [],
            }
        ])

        # Precision: 0% (0 of 1 expected was changed)
        # Recall: 100% (0 actual, 0 TP - nothing changed, nothing missed)
        assert result["avg_precision"] == 0.0
        assert result["avg_recall"] == 100.0

    def test_both_empty_perfect_prediction(self):
        """Verify both empty lists is treated as perfect prediction."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": [],
                "changed_files": [],
            }
        ])

        # Both empty: precision and recall = 100%
        assert result["avg_precision"] == 100.0
        assert result["avg_recall"] == 100.0
        assert result["perfect_predictions"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_expected_file_accuracy([
            "not a dict",
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/foo.py"],
            },
        ])

        assert result["total_packs"] == 1

    def test_missing_pack_id_uses_index(self):
        """Verify missing pack_id uses index."""
        result = analyze_pack_expected_file_accuracy([
            {
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/bar.py"],
            }
        ])

        assert result["examples"][0]["pack_id"] == "pack_0"

    def test_missing_task_title_uses_unknown(self):
        """Verify missing task_title uses 'unknown'."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "pack1",
                "expected_files": ["src/foo.py"],
                "changed_files": ["src/bar.py"],
            }
        ])

        assert result["examples"][0]["task_title"] == "unknown"


class TestPrecision:
    """Test precision calculation helper."""

    def test_perfect_precision(self):
        """Verify perfect precision returns 100."""
        assert _precision(5, 5) == 100.0

    def test_partial_precision(self):
        """Verify partial precision calculation."""
        assert _precision(1, 3) == 33.33

    def test_zero_predicted_zero_tp(self):
        """Verify zero predicted and zero TP returns 100."""
        assert _precision(0, 0) == 100.0

    def test_zero_predicted_nonzero_tp(self):
        """Verify zero predicted with nonzero TP returns 0 (invalid case)."""
        assert _precision(1, 0) == 0.0


class TestRecall:
    """Test recall calculation helper."""

    def test_perfect_recall(self):
        """Verify perfect recall returns 100."""
        assert _recall(5, 5) == 100.0

    def test_partial_recall(self):
        """Verify partial recall calculation."""
        assert _recall(1, 3) == 33.33

    def test_zero_actual_zero_tp(self):
        """Verify zero actual and zero TP returns 100."""
        assert _recall(0, 0) == 100.0

    def test_zero_actual_nonzero_tp(self):
        """Verify zero actual with nonzero TP returns 0 (invalid case)."""
        assert _recall(1, 0) == 0.0


class TestF1Score:
    """Test F1 score calculation helper."""

    def test_perfect_f1(self):
        """Verify perfect precision and recall gives F1 = 100."""
        assert _f1_score(100.0, 100.0) == 100.0

    def test_zero_precision_and_recall(self):
        """Verify zero precision and recall gives F1 = 0."""
        assert _f1_score(0.0, 0.0) == 0.0

    def test_balanced_f1(self):
        """Verify F1 calculation with equal precision and recall."""
        # F1 should equal precision/recall when they're equal
        assert _f1_score(50.0, 50.0) == 50.0

    def test_imbalanced_f1(self):
        """Verify F1 calculation with imbalanced metrics."""
        # High precision (100%), low recall (33.33%)
        # F1 = 2 * (1.0 * 0.3333) / (1.0 + 0.3333) = 0.6666 / 1.3333 = 0.5 = 50%
        f1 = _f1_score(100.0, 33.33)
        assert 49.0 < f1 < 51.0  # Allow some rounding tolerance


class TestCategorizeFile:
    """Test file categorization helper."""

    def test_test_file_detection(self):
        """Verify test files are categorized as 'test'."""
        assert _categorize_file("tests/test_foo.py") == "test"
        assert _categorize_file("src/test_bar.py") == "test"

    def test_config_file_detection(self):
        """Verify config files are categorized as 'config'."""
        assert _categorize_file("config.json") == "config"
        assert _categorize_file("settings.yaml") == "config"
        assert _categorize_file("app.config.ts") == "config"

    def test_types_file_detection(self):
        """Verify type definition files are categorized as 'types'."""
        assert _categorize_file("types.d.ts") == "types"
        assert _categorize_file("user.types.ts") == "types"

    def test_docs_file_detection(self):
        """Verify documentation files are categorized as 'docs'."""
        assert _categorize_file("README.md") == "docs"
        assert _categorize_file("docs/guide.rst") == "docs"

    def test_extension_fallback(self):
        """Verify extension is used as fallback."""
        assert _categorize_file("src/main.py") == "py"
        assert _categorize_file("src/app.ts") == "ts"

    def test_no_extension_returns_other(self):
        """Verify files without extension return 'other'."""
        assert _categorize_file("Makefile") == "other"


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

    def test_leading_dot_slash_removed(self):
        """Verify leading ./ is removed."""
        assert _normalize_files(["./src/foo.py"]) == ["src/foo.py"]

    def test_backslashes_converted_to_forward_slashes(self):
        """Verify backslashes are converted to forward slashes."""
        assert _normalize_files(["src\\foo.py"]) == ["src/foo.py"]


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_predicted_pack(self):
        """Simulate well-predicted pack execution."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": [
                    "src/synthesis/analyzer.py",
                    "tests/test_analyzer.py",
                ],
                "changed_files": [
                    "src/synthesis/analyzer.py",
                    "tests/test_analyzer.py",
                ],
                "task_title": "Add analyzer",
            }
        ])

        assert result["perfect_predictions"] == 1
        assert result["avg_f1_score"] == 100.0

    def test_missed_test_files(self):
        """Simulate common case of missing test files in prediction."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_123",
                "expected_files": ["src/foo.py"],
                "changed_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                    "tests/test_integration.py",
                ],
            }
        ])

        assert result["avg_recall"] == 33.33  # 1 of 3
        missed_types = result["commonly_missed_types"]
        assert any(item["file_type"] == "test" for item in missed_types)

    def test_batch_execution_varying_accuracy(self):
        """Simulate batch execution with varying prediction accuracy."""
        result = analyze_pack_expected_file_accuracy([
            {
                "pack_id": "task_1",
                "expected_files": ["src/a.py"],
                "changed_files": ["src/a.py"],  # Perfect
            },
            {
                "pack_id": "task_2",
                "expected_files": ["src/b.py", "src/c.py"],
                "changed_files": ["src/b.py"],  # Over-predicted
            },
            {
                "pack_id": "task_3",
                "expected_files": ["src/d.py"],
                "changed_files": ["src/d.py", "src/e.py", "config.json"],  # Under-predicted
            },
        ])

        assert result["total_packs"] == 3
        assert result["perfect_predictions"] == 1
        # Should have examples of imperfect predictions
        assert len(result["examples"]) == 2
