"""Tests for pack test coverage correlation analyzer."""

import pytest

from synthesis.pack_test_coverage_correlation import (
    analyze_pack_test_coverage_correlation,
    _is_test_file,
    _is_file_level_test,
    _calculate_coverage,
    _calculate_correlation,
)


class TestAnalyzePackTestCoverageCorrelation:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_test_coverage_correlation([])

        assert result["total_tasks"] == 0
        assert result["tasks_with_test_command"] == 0
        assert result["total_expected_files"] == 0
        assert result["source_file_count"] == 0
        assert result["test_file_count"] == 0
        assert result["test_to_source_ratio"] == 0.0
        assert result["tasks_with_companion_tests"] == 0
        assert result["tasks_missing_tests"] == 0
        assert result["coverage_success_correlation"] == "insufficient_data"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_test_coverage_correlation(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_test_coverage_correlation("not a list")

    def test_single_task_with_tests(self):
        """Verify single task with test files."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "tests/test_main.py"],
                "test_command": "pytest tests/test_main.py",
                "outcome": "completed",
            }
        ])

        assert result["total_tasks"] == 1
        assert result["source_file_count"] == 1
        assert result["test_file_count"] == 1
        assert result["test_to_source_ratio"] == 1.0
        assert result["tasks_with_companion_tests"] == 1

    def test_task_without_tests(self):
        """Verify task without test files."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/main.py", "src/utils.py"],
                "outcome": "completed",
            }
        ])

        assert result["source_file_count"] == 2
        assert result["test_file_count"] == 0
        assert result["tasks_missing_tests"] == 1
        assert result["tasks_with_companion_tests"] == 0

    def test_test_file_detection(self):
        """Verify test file detection patterns."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": [
                    "test_main.py",
                    "main_test.py",
                    "tests/test_utils.py",
                    "file.test.js",
                    "spec.ts",
                    "src/main.py",
                ],
            }
        ])

        assert result["test_file_count"] == 5
        assert result["source_file_count"] == 1

    def test_test_to_source_ratio_calculation(self):
        """Verify test-to-source ratio calculation."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "tests/test_a.py",
                ],
            }
        ])

        # 1 test file / 2 source files = 0.5
        assert result["test_to_source_ratio"] == 0.5

    def test_file_level_test_command_detection(self):
        """Verify file-level test command detection."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "test_command": "pytest tests/test_specific.py",
            },
            {
                "task_id": "task2",
                "test_command": "npm test file.test.js",
            },
        ])

        assert result["file_level_test_commands"] == 2
        assert result["package_level_test_commands"] == 0
        assert result["test_specificity_ratio"] == 100.0

    def test_package_level_test_command_detection(self):
        """Verify package-level test command detection."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "test_command": "pytest tests/",
            },
            {
                "task_id": "task2",
                "test_command": "npm test",
            },
        ])

        assert result["file_level_test_commands"] == 0
        assert result["package_level_test_commands"] == 2
        assert result["test_specificity_ratio"] == 0.0

    def test_test_command_presence_tracking(self):
        """Verify test command presence tracking."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "test_command": "pytest tests/",
            },
            {
                "task_id": "task2",
                # No test command
            },
        ])

        assert result["tasks_with_test_command"] == 1

    def test_tests_executed_tracking(self):
        """Verify test execution tracking."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "has_tests_executed": True,
            },
            {
                "task_id": "task2",
                "has_tests_executed": False,
            },
        ])

        assert result["tasks_with_tests_executed"] == 1

    def test_tests_created_not_executed(self):
        """Verify tracking of tests created but not executed."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["tests/test_a.py", "tests/test_b.py"],
                "has_tests_executed": False,
            }
        ])

        assert result["tests_created_not_executed"] == 2

    def test_outcome_tracking(self):
        """Verify outcome tracking."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/a.py"],
                "outcome": "completed",
            },
            {
                "task_id": "task2",
                "expected_files": ["src/b.py"],
                "outcome": "failed",
            },
        ])

        assert result["completed_tasks_count"] == 1
        assert result["failed_tasks_count"] == 1

    def test_positive_coverage_correlation(self):
        """Verify positive correlation detection."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/a.py", "tests/test_a.py"],
                "outcome": "completed",
            },
            {
                "task_id": "task2",
                "expected_files": ["src/b.py"],  # No tests
                "outcome": "failed",
            },
        ])

        # Completed tasks have better coverage
        assert result["coverage_success_correlation"] == "positive"
        assert result["avg_coverage_completed"] > result["avg_coverage_failed"]

    def test_negative_coverage_correlation(self):
        """Verify negative correlation detection."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/a.py"],  # No tests
                "outcome": "completed",
            },
            {
                "task_id": "task2",
                "expected_files": ["src/b.py", "tests/test_b.py"],
                "outcome": "failed",
            },
        ])

        # Failed tasks have better coverage (unusual)
        assert result["coverage_success_correlation"] == "negative"

    def test_neutral_coverage_correlation(self):
        """Verify neutral correlation detection."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": ["src/a.py", "tests/test_a.py"],
                "outcome": "completed",
            },
            {
                "task_id": "task2",
                "expected_files": ["src/b.py", "tests/test_b.py"],
                "outcome": "failed",
            },
        ])

        # Similar coverage
        assert result["coverage_success_correlation"] == "neutral"

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_test_coverage_correlation([
            "not a dict",
            {
                "task_id": "task1",
                "expected_files": ["src/main.py"],
            },
        ])

        assert result["total_tasks"] == 1

    def test_empty_expected_files(self):
        """Verify empty expected_files is handled."""
        result = analyze_pack_test_coverage_correlation([
            {"task_id": "task1", "expected_files": []},
        ])

        assert result["total_expected_files"] == 0

    def test_non_list_expected_files(self):
        """Verify non-list expected_files is handled."""
        result = analyze_pack_test_coverage_correlation([
            {"task_id": "task1", "expected_files": "not a list"},
        ])

        assert result["total_expected_files"] == 0

    def test_realistic_pack_pattern(self):
        """Verify realistic pack with mixed coverage."""
        result = analyze_pack_test_coverage_correlation([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/analyzer.py",
                    "tests/test_analyzer.py",
                ],
                "test_command": "pytest tests/test_analyzer.py -v",
                "has_tests_executed": True,
                "outcome": "completed",
            },
            {
                "task_id": "task2",
                "expected_files": [
                    "src/utils.py",
                    "tests/test_utils.py",
                ],
                "test_command": "pytest tests/test_utils.py -v",
                "has_tests_executed": True,
                "outcome": "completed",
            },
            {
                "task_id": "task3",
                "expected_files": [
                    "src/feature.py",
                ],
                "outcome": "failed",
            },
        ])

        assert result["total_tasks"] == 3
        assert result["source_file_count"] == 3
        assert result["test_file_count"] == 2
        assert result["tasks_with_companion_tests"] == 2
        assert result["tasks_missing_tests"] == 1
        assert result["completed_tasks_count"] == 2
        assert result["failed_tasks_count"] == 1
        assert result["coverage_success_correlation"] == "positive"


class TestHelperFunctions:
    """Test helper functions."""

    def test_is_test_file_various_patterns(self):
        """Verify test file detection patterns."""
        assert _is_test_file("test_main.py") is True
        assert _is_test_file("main_test.py") is True
        assert _is_test_file("tests/test_utils.py") is True
        assert _is_test_file("file.test.js") is True
        assert _is_test_file("spec.ts") is True
        assert _is_test_file("src/main.py") is False
        assert _is_test_file("utils.py") is False

    def test_is_file_level_test_various_commands(self):
        """Verify file-level test command detection."""
        assert _is_file_level_test("pytest tests/test_main.py") is True
        assert _is_file_level_test("pytest tests/test_main.py::test_function") is True
        assert _is_file_level_test("python test_file.py") is True
        assert _is_file_level_test("npm test file.test.js") is True
        assert _is_file_level_test("jest specific.test.ts") is True

        assert _is_file_level_test("pytest tests/") is False
        assert _is_file_level_test("npm test") is False
        assert _is_file_level_test("cargo test") is False

    def test_calculate_coverage_ratio(self):
        """Verify coverage calculation."""
        assert _calculate_coverage(1, 1) == 1.0
        assert _calculate_coverage(1, 2) == 0.5
        assert _calculate_coverage(0, 1) == 0.0
        assert _calculate_coverage(2, 1) == 2.0  # 200% coverage

    def test_calculate_coverage_zero_source_files(self):
        """Verify coverage with zero source files."""
        assert _calculate_coverage(1, 0) == 0.0

    def test_calculate_correlation_positive(self):
        """Verify positive correlation calculation."""
        assert _calculate_correlation(1.0, 0.5) == "positive"
        assert _calculate_correlation(0.8, 0.3) == "positive"

    def test_calculate_correlation_negative(self):
        """Verify negative correlation calculation."""
        assert _calculate_correlation(0.3, 0.8) == "negative"
        assert _calculate_correlation(0.0, 0.5) == "negative"

    def test_calculate_correlation_neutral(self):
        """Verify neutral correlation calculation."""
        assert _calculate_correlation(0.5, 0.5) == "neutral"
        assert _calculate_correlation(0.6, 0.5) == "neutral"

    def test_calculate_correlation_insufficient_data(self):
        """Verify insufficient data detection."""
        assert _calculate_correlation(0.0, 0.0) == "insufficient_data"
