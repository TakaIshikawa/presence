"""Tests for pack verification strategy consistency analyzer."""

import pytest

from synthesis.pack_verification_strategy_consistency import analyze_pack_verification_strategy_consistency


class TestAnalyzePackVerificationStrategyConsistency:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_verification_strategy_consistency([])

        assert result["total_tasks"] == 0
        assert result["has_verification_command"] == 0
        assert result["has_test_command"] == 0
        assert result["verification_covers_test"] == 0
        assert result["verification_test_alignment_ratio"] == 0.0
        assert result["package_manager_consistent"] is True
        assert result["detected_package_manager"] == "unknown"
        assert result["file_pattern_matches"] == 0
        assert result["risk_verification_aligned"] == 0
        assert result["risk_misalignment_count"] == 0
        assert result["missing_coverage_files_count"] == 0
        assert result["consistency_score"] == 0.0
        assert result["well_aligned_tasks"] == 0
        assert result["poorly_aligned_tasks"] == 0
        assert result["unified_strategy"] is False

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_verification_strategy_consistency(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_verification_strategy_consistency("not a list")

    def test_verification_covers_test_same_command(self):
        """Verify detection when verification and test are same."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_foo.py",
                "test_command": "pytest tests/test_foo.py",
            }
        ])

        assert result["verification_covers_test"] == 1
        assert result["verification_test_alignment_ratio"] == 100.0

    def test_verification_covers_test_broader_path(self):
        """Verify detection when verification is broader."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "test_command": "pytest tests/test_foo.py",
            }
        ])

        assert result["verification_covers_test"] == 1

    def test_verification_does_not_cover_test(self):
        """Verify detection when verification doesn't cover test."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_foo.py",
                "test_command": "pytest tests/test_bar.py",
            }
        ])

        assert result["verification_covers_test"] == 0

    def test_package_manager_detection_pytest(self):
        """Verify pytest package manager detection."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "test_command": "pytest tests/",
            }
        ])

        assert result["detected_package_manager"] == "pytest"

    def test_package_manager_detection_npm(self):
        """Verify npm package manager detection."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "test_command": "npm test",
            }
        ])

        assert result["detected_package_manager"] == "npm"

    def test_package_manager_consistent_single_pm(self):
        """Verify package manager consistency with single PM."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "test_command": "pytest tests/test_a.py",
            },
            {
                "task_id": "task2",
                "test_command": "pytest tests/test_b.py",
            },
        ])

        assert result["package_manager_consistent"] is True

    def test_package_manager_inconsistent_multiple_pms(self):
        """Verify package manager inconsistency with multiple PMs."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "test_command": "pytest tests/",
            },
            {
                "task_id": "task2",
                "test_command": "npm test",
            },
        ])

        assert result["package_manager_consistent"] is False

    def test_risk_verification_alignment_high_risk_broad_verify(self):
        """Verify risk-verification alignment for high-risk with broad verify."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "risk_level": "high",
            }
        ])

        assert result["risk_verification_aligned"] == 1
        assert result["risk_misalignment_count"] == 0

    def test_file_pattern_matching_with_test_files(self):
        """Verify file pattern matching with test files."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_foo.py",
                "expected_files": ["tests/test_foo.py"],
            }
        ])

        assert result["file_pattern_matches"] == 1

    def test_consistency_score_perfect_alignment(self):
        """Verify consistency score for perfect alignment."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "test_command": "pytest tests/test_foo.py",
                "expected_files": ["tests/test_foo.py"],
            }
        ])

        assert result["consistency_score"] >= 0.7
        assert result["unified_strategy"] is True

    def test_well_aligned_task_detection(self):
        """Verify well-aligned task detection."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
                "test_command": "pytest tests/test_foo.py",
                "expected_files": ["tests/test_foo.py"],
            }
        ])

        assert result["well_aligned_tasks"] == 1
        assert result["poorly_aligned_tasks"] == 0

    def test_poorly_aligned_task_detection(self):
        """Verify poorly-aligned task detection."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_a.py",
                "test_command": "pytest tests/test_b.py",
            }
        ])

        assert result["poorly_aligned_tasks"] == 1

    def test_comprehensive_consistent_pack(self):
        """Verify comprehensive consistent pack analysis."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/ -v",
                "test_command": "pytest tests/test_a.py",
                "expected_files": ["src/a.py", "tests/test_a.py"],
                "risk_level": "medium",
            },
            {
                "task_id": "task2",
                "verification_command": "pytest tests/ -v",
                "test_command": "pytest tests/test_b.py",
                "expected_files": ["src/b.py", "tests/test_b.py"],
                "risk_level": "low",
            },
        ])

        assert result["package_manager_consistent"] is True
        assert result["verification_test_alignment_ratio"] == 100.0
        assert result["unified_strategy"] is True
        assert result["missing_coverage_files_count"] == 0

    def test_comprehensive_inconsistent_pack(self):
        """Verify comprehensive inconsistent pack analysis."""
        result = analyze_pack_verification_strategy_consistency([
            {
                "task_id": "task1",
                "test_command": "pytest tests/test_a.py",
                "expected_files": ["tests/test_a.py"],
            },
            {
                "task_id": "task2",
                "test_command": "npm test",
                "expected_files": ["tests/test_c.py"],
                "risk_level": "high",
            },
        ])

        assert result["package_manager_consistent"] is False
        assert result["unified_strategy"] is False
