"""Tests for pack source-test pairing analyzer."""

import pytest

from synthesis.pack_source_test_pairing import analyze_pack_source_test_pairing


class TestAnalyzePackSourceTestPairing:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_source_test_pairing([])

        assert result["total_tasks"] == 0
        assert result["total_source_files"] == 0
        assert result["total_test_files"] == 0
        assert result["paired_source_files"] == 0
        assert result["unpaired_source_files_count"] == 0
        assert result["orphaned_test_files_count"] == 0
        assert result["pairing_score"] == 0.0
        assert result["pairing_ratio"] == 0.0
        assert result["project_standard_ratio"] == 0.64
        assert result["meets_project_standard"] is False
        assert result["convention_compliant_tests"] == 0
        assert result["convention_violation_count"] == 0
        assert result["tasks_mentioning_tests"] == 0
        assert result["well_paired_tasks"] == 0
        assert result["poorly_paired_tasks"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_source_test_pairing(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_source_test_pairing("not a list")

    def test_single_paired_source_test(self):
        """Verify single source-test pair is detected."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1
        assert result["total_test_files"] == 1
        assert result["paired_source_files"] == 1
        assert result["pairing_score"] == 1.0
        assert result["pairing_ratio"] == 100.0

    def test_unpaired_source_file_detected(self):
        """Verify unpaired source file is detected."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1
        assert result["total_test_files"] == 0
        assert result["paired_source_files"] == 0
        assert result["unpaired_source_files_count"] == 1
        assert "src/foo.py" in result["unpaired_source_files"]
        assert result["pairing_score"] == 0.0

    def test_orphaned_test_file_detected(self):
        """Verify orphaned test file is detected."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/test_orphan.py",
                ],
            }
        ])

        assert result["total_test_files"] == 1
        assert result["total_source_files"] == 0
        assert result["orphaned_test_files_count"] == 1
        assert "tests/test_orphan.py" in result["orphaned_test_files"]

    def test_pairing_ratio_calculation(self):
        """Verify pairing ratio calculation."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                    "src/bar.py",
                    "src/baz.py",
                    "tests/test_foo.py",
                    "tests/test_bar.py",
                ],
            }
        ])

        # 2 paired out of 3 = 66.67%
        assert result["total_source_files"] == 3
        assert result["paired_source_files"] == 2
        assert result["pairing_ratio"] == 66.67

    def test_pairing_score_calculation(self):
        """Verify pairing score calculation."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "src/c.py",
                    "src/d.py",
                    "tests/test_a.py",
                    "tests/test_b.py",
                    "tests/test_c.py",
                ],
            }
        ])

        # 3/4 = 0.75
        assert result["pairing_score"] == 0.75

    def test_project_standard_ratio_constant(self):
        """Verify project standard ratio is 0.64."""
        result = analyze_pack_source_test_pairing([])
        assert result["project_standard_ratio"] == 0.64

    def test_meets_project_standard_true(self):
        """Verify meets_project_standard for ratio >= 0.64."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "src/c.py",
                    "tests/test_a.py",
                    "tests/test_b.py",
                ],
            }
        ])

        # 2/3 = 0.667 >= 0.64
        assert result["meets_project_standard"] is True

    def test_meets_project_standard_false(self):
        """Verify meets_project_standard for ratio < 0.64."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "src/c.py",
                    "src/d.py",
                    "tests/test_a.py",
                ],
            }
        ])

        # 1/4 = 0.25 < 0.64
        assert result["meets_project_standard"] is False

    def test_convention_compliant_test_file(self):
        """Verify convention-compliant test file detection."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/test_foo.py",
                ],
            }
        ])

        assert result["convention_compliant_tests"] == 1
        assert result["convention_violation_count"] == 0

    def test_convention_violation_no_tests_prefix(self):
        """Verify convention violation for test not in tests/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "test_foo.py",
                ],
            }
        ])

        assert result["convention_compliant_tests"] == 0
        assert result["convention_violation_count"] == 1

    def test_convention_violation_no_test_prefix(self):
        """Verify convention violation for file not starting with test_."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/foo_test.py",
                ],
            }
        ])

        # foo_test.py is detected as test file but violates convention
        assert result["convention_violation_count"] == 1

    def test_tasks_mentioning_tests_in_acceptance_criteria(self):
        """Verify detection of test mentions in ACs."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    "Test suite covers all edge cases",
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 1

    def test_tasks_mentioning_coverage_keyword(self):
        """Verify detection of coverage keyword in ACs."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    "Code has 90% test coverage",
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 1

    def test_tasks_mentioning_pytest(self):
        """Verify detection of pytest keyword in ACs."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    "All pytest tests pass",
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 1

    def test_tasks_not_mentioning_tests(self):
        """Verify no false positives for test mentions."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    "Implementation is complete",
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 0

    def test_well_paired_task_detection(self):
        """Verify well-paired task detection (100% pairing)."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                ],
            }
        ])

        assert result["well_paired_tasks"] == 1
        assert result["poorly_paired_tasks"] == 0

    def test_poorly_paired_task_detection(self):
        """Verify poorly-paired task detection (<50% pairing)."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "src/c.py",
                    "tests/test_a.py",
                ],
            }
        ])

        # 1/3 = 33% < 50%
        assert result["poorly_paired_tasks"] == 1
        assert result["well_paired_tasks"] == 0

    def test_source_file_detection_in_src_directory(self):
        """Verify source file detection in src/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
            }
        ])

        assert result["total_source_files"] == 1

    def test_source_file_detection_in_lib_directory(self):
        """Verify source file detection in lib/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["lib/bar.py"],
            }
        ])

        assert result["total_source_files"] == 1

    def test_source_file_detection_in_synthesis_directory(self):
        """Verify source file detection in synthesis/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["synthesis/analyzer.py"],
            }
        ])

        assert result["total_source_files"] == 1

    def test_source_file_detection_in_evaluation_directory(self):
        """Verify source file detection in evaluation/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["evaluation/metrics.py"],
            }
        ])

        assert result["total_source_files"] == 1

    def test_init_files_excluded_from_source(self):
        """Verify __init__.py files are excluded from source count."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/__init__.py",
                    "src/foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1

    def test_setup_files_excluded_from_source(self):
        """Verify setup.py files are excluded from source count."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "setup.py",
                    "src/foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1

    def test_conftest_files_excluded_from_source(self):
        """Verify conftest.py files are excluded from source count."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/conftest.py",
                    "src/foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1

    def test_test_file_detection_in_tests_directory(self):
        """Verify test file detection in tests/ directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["tests/test_foo.py"],
            }
        ])

        assert result["total_test_files"] == 1

    def test_test_file_detection_with_test_prefix(self):
        """Verify test file detection with test_ prefix."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["test_bar.py"],
            }
        ])

        assert result["total_test_files"] == 1

    def test_test_file_detection_with_test_suffix(self):
        """Verify test file detection with _test.py suffix."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["foo_test.py"],
            }
        ])

        assert result["total_test_files"] == 1

    def test_pairing_pattern_simple(self):
        """Verify simple pairing pattern: src/foo.py → tests/test_foo.py."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                ],
            }
        ])

        assert result["paired_source_files"] == 1
        assert result["unpaired_source_files_count"] == 0

    def test_pairing_pattern_nested_source(self):
        """Verify pairing for nested source: src/bar/baz.py → tests/test_bar_baz.py."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/bar/baz.py",
                    "tests/test_bar_baz.py",
                ],
            }
        ])

        assert result["paired_source_files"] == 1

    def test_pairing_pattern_directory_structure(self):
        """Verify pairing with directory structure: src/bar/baz.py → tests/bar/test_baz.py."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/bar/baz.py",
                    "tests/bar/test_baz.py",
                ],
            }
        ])

        assert result["paired_source_files"] == 1

    def test_pairing_fuzzy_match(self):
        """Verify fuzzy matching for pairing."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "synthesis/session_analyzer.py",
                    "tests/test_session_analyzer.py",
                ],
            }
        ])

        assert result["paired_source_files"] == 1

    def test_reverse_pairing_simple(self):
        """Verify reverse pairing: tests/test_foo.py → src/foo.py."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/test_foo.py",
                    "src/foo.py",
                ],
            }
        ])

        assert result["orphaned_test_files_count"] == 0

    def test_reverse_pairing_synthesis_directory(self):
        """Verify reverse pairing with synthesis directory."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "tests/test_analyzer.py",
                    "synthesis/analyzer.py",
                ],
            }
        ])

        assert result["orphaned_test_files_count"] == 0

    def test_multiple_tasks_aggregation(self):
        """Verify metrics are aggregated across multiple tasks."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/foo.py",
                    "tests/test_foo.py",
                ],
            },
            {
                "task_id": "task2",
                "expected_files": [
                    "src/bar.py",
                    "tests/test_bar.py",
                ],
            },
        ])

        assert result["total_tasks"] == 2
        assert result["total_source_files"] == 2
        assert result["total_test_files"] == 2
        assert result["paired_source_files"] == 2

    def test_unpaired_files_list_limited_to_ten(self):
        """Verify unpaired files list is limited to 10."""
        expected_files = [f"src/file{i}.py" for i in range(20)]
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": expected_files,
            }
        ])

        assert result["unpaired_source_files_count"] == 20
        assert len(result["unpaired_source_files"]) == 10

    def test_orphaned_files_list_limited_to_ten(self):
        """Verify orphaned files list is limited to 10."""
        expected_files = [f"tests/test_file{i}.py" for i in range(20)]
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": expected_files,
            }
        ])

        assert result["orphaned_test_files_count"] == 20
        assert len(result["orphaned_test_files"]) == 10

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_source_test_pairing([
            "not a dict",
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
            },
        ])

        assert result["total_tasks"] == 1

    def test_missing_expected_files_handled(self):
        """Verify missing expected_files field is handled gracefully."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
            }
        ])

        assert result["total_tasks"] == 1
        assert result["total_source_files"] == 0

    def test_non_list_expected_files_handled(self):
        """Verify non-list expected_files is handled gracefully."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": "not a list",
            }
        ])

        assert result["total_tasks"] == 1
        assert result["total_source_files"] == 0

    def test_empty_expected_files_list(self):
        """Verify empty expected_files list is handled."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [],
            }
        ])

        assert result["total_tasks"] == 1
        assert result["total_source_files"] == 0

    def test_non_string_file_paths_skipped(self):
        """Verify non-string file paths are skipped."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    123,
                    "src/foo.py",
                    None,
                ],
            }
        ])

        assert result["total_source_files"] == 1

    def test_empty_string_file_paths_skipped(self):
        """Verify empty string file paths are skipped."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "",
                    "   ",
                    "src/foo.py",
                ],
            }
        ])

        assert result["total_source_files"] == 1

    def test_whitespace_trimmed_from_file_paths(self):
        """Verify whitespace is trimmed from file paths."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "  src/foo.py  ",
                    "  tests/test_foo.py  ",
                ],
            }
        ])

        assert result["paired_source_files"] == 1

    def test_missing_acceptance_criteria_handled(self):
        """Verify missing acceptance_criteria is handled gracefully."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
            }
        ])

        assert result["tasks_mentioning_tests"] == 0

    def test_non_list_acceptance_criteria_handled(self):
        """Verify non-list acceptance_criteria is handled gracefully."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": "not a list",
            }
        ])

        assert result["tasks_mentioning_tests"] == 0

    def test_non_string_acceptance_criteria_skipped(self):
        """Verify non-string acceptance criteria are skipped."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    123,
                    "Test coverage is good",
                    None,
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 1

    def test_case_insensitive_test_keyword_matching(self):
        """Verify test keyword matching is case-insensitive."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["src/foo.py"],
                "acceptance_criteria": [
                    "TEST suite covers edge cases",
                ],
            }
        ])

        assert result["tasks_mentioning_tests"] == 1

    def test_zero_denominator_in_percentages(self):
        """Verify zero denominator in percentage calculations."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": ["tests/test_foo.py"],
            }
        ])

        # No source files
        assert result["pairing_ratio"] == 0.0

    def test_comprehensive_well_paired_pack(self):
        """Verify comprehensive well-paired pack analysis."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/analyzer.py",
                    "tests/test_analyzer.py",
                ],
                "acceptance_criteria": [
                    "Test suite covers all functionality",
                ],
            },
            {
                "task_id": "task2",
                "expected_files": [
                    "synthesis/metrics.py",
                    "tests/test_metrics.py",
                ],
                "acceptance_criteria": [
                    "100% test coverage",
                ],
            },
        ])

        assert result["total_tasks"] == 2
        assert result["pairing_score"] == 1.0
        assert result["meets_project_standard"] is True
        assert result["well_paired_tasks"] == 2
        assert result["poorly_paired_tasks"] == 0
        assert result["tasks_mentioning_tests"] == 2
        assert result["convention_compliant_tests"] == 2

    def test_comprehensive_poorly_paired_pack(self):
        """Verify comprehensive poorly-paired pack analysis."""
        result = analyze_pack_source_test_pairing([
            {
                "task_id": "task1",
                "expected_files": [
                    "src/a.py",
                    "src/b.py",
                    "src/c.py",
                ],
                "acceptance_criteria": [
                    "Implementation is complete",
                ],
            },
            {
                "task_id": "task2",
                "expected_files": [
                    "src/d.py",
                    "test_orphan.py",
                ],
            },
        ])

        assert result["total_source_files"] == 4
        assert result["paired_source_files"] == 0
        assert result["pairing_score"] == 0.0
        assert result["meets_project_standard"] is False
        assert result["poorly_paired_tasks"] == 2
        assert result["tasks_mentioning_tests"] == 0
        assert result["convention_violation_count"] == 1
