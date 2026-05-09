"""Tests for pack acceptance criteria coverage analyzer."""

import pytest

from synthesis.pack_acceptance_criteria_coverage import analyze_pack_acceptance_criteria_coverage


class TestAnalyzePackAcceptanceCriteriaCoverage:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_acceptance_criteria_coverage([])

        assert result["total_tasks"] == 0
        assert result["has_acceptance_criteria"] == 0
        assert result["avg_criteria_per_task"] == 0.0
        assert result["total_criteria"] == 0
        assert result["measurable_criteria_count"] == 0
        assert result["vague_criteria_count"] == 0
        assert result["avg_measurability_score"] == 0.0
        assert result["verification_aligned_count"] == 0
        assert result["unvalidated_criteria_count"] == 0
        assert result["missing_criteria_count"] == 0
        assert result["well_defined_task_count"] == 0
        assert result["poorly_defined_task_count"] == 0
        assert result["common_vague_terms"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_acceptance_criteria_coverage(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_acceptance_criteria_coverage("not a list")

    def test_task_with_well_defined_criteria(self):
        """Verify task with measurable, specific acceptance criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass for src/analyzer.py",
                    "Detects cache usage in session tool calls",
                    "Calculates cache hit/miss ratios from query results",
                ],
                "verification_command": "pytest tests/test_analyzer.py -v",
            }
        ])

        assert result["has_acceptance_criteria"] == 1
        assert result["total_criteria"] == 3
        assert result["avg_criteria_per_task"] == 3.0
        assert result["measurable_criteria_count"] == 3
        assert result["vague_criteria_count"] == 0
        assert result["well_defined_task_count"] == 1
        assert result["poorly_defined_task_count"] == 0

    def test_task_with_vague_criteria(self):
        """Verify task with vague, unmeasurable criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Code should be clean and maintainable",
                    "Handles various edge cases properly",
                    "Works well with different inputs",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["has_acceptance_criteria"] == 1
        assert result["vague_criteria_count"] >= 2  # At least "clean", "proper" criteria
        assert result["poorly_defined_task_count"] == 1
        assert result["well_defined_task_count"] == 0
        assert len(result["common_vague_terms"]) > 0

    def test_task_with_missing_criteria(self):
        """Verify task without acceptance criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/test_analyzer.py",
            }
        ])

        assert result["has_acceptance_criteria"] == 0
        assert result["missing_criteria_count"] == 1
        assert result["poorly_defined_task_count"] == 1

    def test_task_with_empty_criteria_list(self):
        """Verify task with empty acceptance criteria list."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["has_acceptance_criteria"] == 0
        assert result["missing_criteria_count"] == 1

    def test_verification_alignment_with_test_criteria(self):
        """Verify alignment between test criteria and test commands."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "All tests pass",
                    "Test coverage includes edge cases",
                ],
                "verification_command": "pytest tests/test_analyzer.py -v",
            }
        ])

        assert result["verification_aligned_count"] == 1
        assert result["unvalidated_criteria_count"] == 0

    def test_verification_alignment_with_type_criteria(self):
        """Verify alignment between type check criteria and mypy commands."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Type checks pass without errors",
                    "Mypy validation succeeds",
                ],
                "verification_command": "mypy src/analyzer.py",
            }
        ])

        assert result["verification_aligned_count"] == 1

    def test_unvalidated_criteria_detection(self):
        """Verify detection of criteria without verification coverage."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass",
                    "Documentation is comprehensive",
                    "Performance improves significantly",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        # Only "Tests pass" is validated by pytest command
        # "Documentation" and "Performance" are unvalidated
        assert result["unvalidated_criteria_count"] >= 1

    def test_measurability_score_calculation(self):
        """Verify measurability score calculation."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Detects errors with specific file locations",  # High score
                    "Code is clean",  # Low score
                ],
                "verification_command": "pytest tests/",
            }
        ])

        # First criterion should score high, second low
        # Average should be moderate
        assert 30 <= result["avg_measurability_score"] <= 70

    def test_criteria_from_string_format(self):
        """Verify handling of criteria as newline-separated string."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": "Tests pass\nDetects cache usage\nCalculates ratios",
                "verification_command": "pytest tests/",
            }
        ])

        assert result["total_criteria"] == 3
        assert result["has_acceptance_criteria"] == 1

    def test_criteria_with_file_references(self):
        """Verify criteria with specific file references."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass for src/analyzer.py",
                    "Validation covers tests/test_analyzer.py",
                ],
                "verification_command": "pytest tests/test_analyzer.py && mypy src/analyzer.py",
            }
        ])

        # File-specific criteria should be validated
        assert result["verification_aligned_count"] == 1
        assert result["measurable_criteria_count"] == 2

    def test_mixed_quality_criteria(self):
        """Verify handling of mixed quality criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Extracts error messages from tool results",  # Measurable
                    "Identifies patterns of clear vs vague errors",  # Measurable
                    "Code quality is good",  # Vague
                ],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["measurable_criteria_count"] >= 2
        assert result["vague_criteria_count"] >= 1
        assert result["total_criteria"] == 3

    def test_comprehensive_verification_coverage(self):
        """Verify comprehensive verification coverage detection."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass for all modules",
                    "Type checks pass without errors",
                    "Linting passes with no warnings",
                ],
                "verification_command": "pytest tests/ && mypy src/ && ruff check src/",
            }
        ])

        assert result["verification_aligned_count"] == 1
        assert result["well_defined_task_count"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_acceptance_criteria_coverage([
            "not a dict",
            {
                "task_id": "task1",
                "acceptance_criteria": ["Tests pass"],
                "verification_command": "pytest tests/",
            },
        ])

        assert result["total_tasks"] == 1

    def test_multiple_tasks_aggregation(self):
        """Verify correct aggregation across multiple tasks."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass",
                    "Detects cache usage",
                ],
                "verification_command": "pytest tests/test_1.py",
            },
            {
                "task_id": "task2",
                "acceptance_criteria": [
                    "Type checks pass",
                ],
                "verification_command": "mypy src/2.py",
            },
            {
                "task_id": "task3",
                # No criteria
                "verification_command": "pytest tests/",
            },
        ])

        assert result["total_tasks"] == 3
        assert result["has_acceptance_criteria"] == 2
        assert result["total_criteria"] == 3
        assert result["missing_criteria_count"] == 1
        assert result["avg_criteria_per_task"] == 1.0  # (2+1+0)/3

    def test_vague_term_tracking(self):
        """Verify tracking of common vague terms."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Code is clean and proper",
                    "Handles various cases well",
                    "Good quality implementation",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        vague_terms = [term["term"] for term in result["common_vague_terms"]]
        assert "clean" in vague_terms or "proper" in vague_terms or "good" in vague_terms

    def test_optimal_criteria_pattern(self):
        """Verify optimal acceptance criteria pattern."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Extracts acceptance criteria from pack task definitions",
                    "Scores criteria based on measurability and specificity",
                    "Analyzes alignment between criteria and test commands",
                    "Identifies criteria that lack verification coverage",
                    "Tests cover well-defined criteria, vague criteria, and missing criteria",
                ],
                "verification_command": "python -m pytest tests/test_pack_acceptance_criteria_coverage.py -v",
            }
        ])

        assert result["well_defined_task_count"] == 1
        assert result["poorly_defined_task_count"] == 0
        assert result["measurable_criteria_count"] >= 2  # At least some criteria are measurable
        assert result["verification_aligned_count"] == 1
        assert result["avg_measurability_score"] >= 60

    def test_anti_pattern_missing_criteria(self):
        """Verify anti-pattern of missing acceptance criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "verification_command": "pytest tests/",
            },
            {
                "task_id": "task2",
                "acceptance_criteria": [],
                "verification_command": "mypy src/",
            },
        ])

        assert result["missing_criteria_count"] == 2
        assert result["poorly_defined_task_count"] == 2

    def test_anti_pattern_vague_criteria(self):
        """Verify anti-pattern of vague acceptance criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Implementation should be clean and maintainable",
                    "Handles all edge cases properly",
                    "Works well in various scenarios",
                    "Good test coverage",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["vague_criteria_count"] >= 3
        assert result["poorly_defined_task_count"] == 1
        assert result["avg_measurability_score"] <= 40

    def test_anti_pattern_unvalidated_criteria(self):
        """Verify anti-pattern of criteria without verification."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Documentation is complete",
                    "User experience is improved",
                    "Performance is optimized",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        # None of these criteria can be validated by pytest
        assert result["unvalidated_criteria_count"] >= 2
        assert result["verification_aligned_count"] == 0

    def test_criteria_with_observable_outcomes(self):
        """Verify criteria with observable outcomes score highly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Test output contains success message",
                    "Error count equals zero",
                    "File src/output.txt exists after execution",
                    "Function returns list of validated items",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["measurable_criteria_count"] >= 3
        assert result["avg_measurability_score"] >= 60

    def test_criteria_count_per_task_distribution(self):
        """Verify criteria count distribution across tasks."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": ["AC1"],
            },
            {
                "task_id": "task2",
                "acceptance_criteria": ["AC1", "AC2", "AC3"],
            },
            {
                "task_id": "task3",
                "acceptance_criteria": ["AC1", "AC2"],
            },
        ])

        assert result["avg_criteria_per_task"] == 2.0  # (1+3+2)/3
        assert result["total_criteria"] == 6

    def test_empty_string_criteria_filtered(self):
        """Verify empty string criteria are filtered out."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Valid criterion",
                    "",
                    "   ",
                    "Another valid criterion",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        # Only 2 valid criteria (empty strings filtered)
        assert result["total_criteria"] == 2

    def test_case_insensitive_validation_matching(self):
        """Verify validation matching is case-insensitive."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "TESTS PASS FOR MODULE",
                    "Type Checks Succeed",
                ],
                "verification_command": "PYTEST tests/ && MYPY src/",
            }
        ])

        assert result["verification_aligned_count"] == 1

    def test_partial_verification_alignment(self):
        """Verify partial alignment detection (some criteria validated)."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Tests pass",
                    "Type checks pass",
                    "Documentation is complete",  # Not validated
                    "Performance is optimal",  # Not validated
                ],
                "verification_command": "pytest tests/ && mypy src/",
            }
        ])

        # Only 2/4 criteria validated (50%), needs 75% for alignment
        assert result["verification_aligned_count"] == 0
        assert result["unvalidated_criteria_count"] >= 2

    def test_criteria_with_specific_metrics(self):
        """Verify criteria with specific metrics are measurable."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                "task_id": "task1",
                "acceptance_criteria": [
                    "Cache hit ratio exceeds 75%",
                    "Response time under 100ms",
                    "Error count is zero",
                    "Test coverage above 80%",
                ],
                "verification_command": "pytest tests/ --cov",
            }
        ])

        assert result["measurable_criteria_count"] >= 3
        assert result["avg_measurability_score"] >= 65
