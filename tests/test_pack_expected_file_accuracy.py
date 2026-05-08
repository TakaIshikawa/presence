"""Tests for pack expected file accuracy analyzer."""

import pytest

from synthesis.pack_expected_file_accuracy import (
    analyze_pack_expected_file_accuracy,
    _auto_categorize_file,
    _calculate_f1_score,
    _calculate_precision,
    _calculate_recall,
    _classify_accuracy_pattern,
)


class TestAnalyzePackExpectedFileAccuracy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
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


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

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
