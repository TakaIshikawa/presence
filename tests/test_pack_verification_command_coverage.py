"""Tests for pack verification command coverage analyzer."""

import pytest

from synthesis.pack_verification_command_coverage import analyze_pack_verification_command_coverage


class TestAnalyzePackVerificationCommandCoverage:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_verification_command_coverage([])

        assert result["total_tasks"] == 0
        assert result["has_test_command"] == 0
        assert result["has_type_check"] == 0
        assert result["has_lint"] == 0
        assert result["targeted_command_count"] == 0
        assert result["workspace_wide_count"] == 0
        assert result["empty_command_count"] == 0
        assert result["comprehensive_coverage_count"] == 0
        assert result["missing_verification_count"] == 0
        assert result["over_broad_verification_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_command_coverage(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_command_coverage("not a list")

    def test_task_with_test_command(self):
        """Verify test command detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_analyzer.py -v",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["has_test_command"] == 1

    def test_task_with_type_check(self):
        """Verify type check detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "mypy src/analyzer.py",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["has_type_check"] == 1

    def test_task_with_lint(self):
        """Verify lint detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "ruff check src/analyzer.py",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["has_lint"] == 1

    def test_comprehensive_coverage_all_three_types(self):
        """Verify comprehensive coverage detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/ && mypy src/ && ruff check src/",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["has_test_command"] == 1
        assert result["has_type_check"] == 1
        assert result["has_lint"] == 1
        assert result["comprehensive_coverage_count"] == 1

    def test_targeted_command_with_specific_file(self):
        """Verify targeted command detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_analyzer.py",
                "expected_files": ["src/analyzer.py", "tests/test_analyzer.py"],
            }
        ])

        assert result["targeted_command_count"] == 1

    def test_workspace_wide_command(self):
        """Verify workspace-wide command detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["workspace_wide_count"] == 1

    def test_empty_verification_command(self):
        """Verify empty command handling."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["empty_command_count"] == 1
        assert result["missing_verification_count"] == 1

    def test_missing_verification_command(self):
        """Verify missing verification_command field."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        assert result["missing_verification_count"] == 1

    def test_over_broad_verification_for_single_file(self):
        """Verify over-broad verification detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "expected_files": ["src/analyzer.py"],
            }
        ])

        # Workspace-wide test for 1 file = over-broad
        assert result["over_broad_verification_count"] == 1

    def test_not_over_broad_for_many_files(self):
        """Verify workspace-wide verification acceptable for many files."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "expected_files": ["src/file1.py", "src/file2.py", "src/file3.py"],
            }
        ])

        # Workspace-wide test for 3+ files = acceptable
        assert result["over_broad_verification_count"] == 0

    def test_various_test_commands(self):
        """Verify various test command patterns."""
        test_commands = [
            "pytest tests/",
            "npm test",
            "jest --coverage",
            "go test ./...",
            "cargo test",
        ]

        for cmd in test_commands:
            result = analyze_pack_verification_command_coverage([
                {"task_id": "task1", "verification_command": cmd}
            ])
            assert result["has_test_command"] == 1, f"Should detect test in: {cmd}"

    def test_various_type_check_commands(self):
        """Verify various type check patterns."""
        type_commands = [
            "mypy src/",
            "tsc --noEmit",
            "pyright .",
            "flow check",
        ]

        for cmd in type_commands:
            result = analyze_pack_verification_command_coverage([
                {"task_id": "task1", "verification_command": cmd}
            ])
            assert result["has_type_check"] == 1, f"Should detect type check in: {cmd}"

    def test_various_lint_commands(self):
        """Verify various lint patterns."""
        lint_commands = [
            "ruff check src/",
            "eslint src/",
            "pylint src/",
            "black --check src/",
        ]

        for cmd in lint_commands:
            result = analyze_pack_verification_command_coverage([
                {"task_id": "task1", "verification_command": cmd}
            ])
            assert result["has_lint"] == 1, f"Should detect lint in: {cmd}"

    def test_verification_ratio_calculation(self):
        """Verify verification-to-file ratio calculation."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/ && mypy src/",
                "expected_files": ["src/file1.py", "src/file2.py"],
            }
        ])

        # 2 verification types / 2 files = 1.0 ratio = 33.33 score
        assert result["avg_verification_to_file_ratio"] == 33.33

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_verification_command_coverage([
            "not a dict",
            {"task_id": "task1", "verification_command": "pytest tests/"},
        ])

        assert result["total_tasks"] == 1

    def test_mixed_verification_quality(self):
        """Verify mixed verification quality detection."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_1.py && mypy src/1.py && ruff check src/1.py",
                "expected_files": ["src/1.py"],
            },
            {
                "task_id": "task2",
                "verification_command": "",
                "expected_files": ["src/2.py"],
            },
            {
                "task_id": "task3",
                "verification_command": "pytest tests/",
                "expected_files": ["src/3.py"],
            },
        ])

        assert result["comprehensive_coverage_count"] == 1
        assert result["missing_verification_count"] == 1
        assert result["over_broad_verification_count"] == 1

    def test_optimal_targeted_verification_pattern(self):
        """Verify optimal verification pattern."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_analyzer.py && mypy src/analyzer.py && ruff check src/analyzer.py",
                "expected_files": ["src/analyzer.py", "tests/test_analyzer.py"],
            }
        ])

        assert result["has_test_command"] == 1
        assert result["has_type_check"] == 1
        assert result["has_lint"] == 1
        assert result["comprehensive_coverage_count"] == 1
        assert result["targeted_command_count"] == 1
        assert result["missing_verification_count"] == 0
        assert result["over_broad_verification_count"] == 0

    def test_anti_pattern_missing_verification(self):
        """Verify anti-pattern of missing verification."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "",
                "expected_files": ["src/analyzer.py"],
            },
            {
                "task_id": "task2",
                "expected_files": ["src/utils.py"],
            },
        ])

        assert result["missing_verification_count"] == 2
        assert result["empty_command_count"] == 2  # Both tasks have no/empty command

    def test_anti_pattern_over_broad_commands(self):
        """Verify anti-pattern of over-broad verification."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/ && mypy src/ && ruff check src/",
                "expected_files": ["src/single_file.py"],
            }
        ])

        assert result["workspace_wide_count"] == 1
        assert result["over_broad_verification_count"] == 1

    def test_changed_files_takes_precedence_over_expected(self):
        """Verify changed_files used for count when available."""
        result = analyze_pack_verification_command_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "expected_files": ["src/file1.py", "src/file2.py", "src/file3.py"],
                "changed_files": ["src/file1.py"],  # Actually only changed 1
            }
        ])

        # Should use changed_files count (1), making workspace check over-broad
        assert result["over_broad_verification_count"] == 1

    def test_case_insensitive_command_matching(self):
        """Verify command matching is case-insensitive."""
        result = analyze_pack_verification_command_coverage([
            {"task_id": "task1", "verification_command": "PYTEST TESTS/"},
            {"task_id": "task2", "verification_command": "MyPy src/"},
            {"task_id": "task3", "verification_command": "RUFF CHECK src/"},
        ])

        assert result["has_test_command"] == 1
        assert result["has_type_check"] == 1
        assert result["has_lint"] == 1
