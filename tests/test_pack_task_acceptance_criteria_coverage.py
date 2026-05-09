"""Tests for pack task acceptance criteria coverage analyzer."""

import pytest

from synthesis.pack_task_acceptance_criteria_coverage import analyze_pack_task_acceptance_criteria_coverage


class TestAnalyzePackTaskAcceptanceCriteriaCoverage:
    """Test main analyzer function."""

    def test_empty_tasks_returns_zeroed_metrics(self):
        """Verify empty task list returns zero metrics."""
        result = analyze_pack_task_acceptance_criteria_coverage([])

        assert result["total_packs"] == 0
        assert result["total_tasks"] == 0
        assert result["tasks_with_criteria"] == 0
        assert result["total_criteria"] == 0
        assert result["criteria_validation_rate"] == 0.0
        assert result["untested_criteria_count"] == 0
        assert result["verification_to_criteria_alignment_score"] == 0.0
        assert result["common_validation_gaps"] == []
        assert result["criteria_specificity_score"] == 0.0
        assert result["fully_validated_tasks"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_acceptance_criteria_coverage(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_acceptance_criteria_coverage("not a list")

    def test_task_with_single_criterion_string(self):
        """Verify task with single criterion as string."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "pack_id": "pack1",
                "task_id": "task1",
                "acceptance_criteria": "All tests pass",
                "verification_command": "pytest tests/",
                "verification_passed": True,
            }
        ])

        assert result["total_tasks"] == 1
        assert result["tasks_with_criteria"] == 1
        assert result["total_criteria"] == 1

    def test_task_with_criteria_list(self):
        """Verify task with criteria as list."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": [
                    "All tests pass",
                    "Coverage exceeds 90%",
                    "No type errors",
                ]
            }
        ])

        assert result["total_criteria"] == 3

    def test_criterion_validated_when_in_command(self):
        """Verify criterion is validated when keywords appear in command."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["All tests pass with pytest"],
                "verification_command": "pytest tests/ -v",
                "verification_passed": True,
            }
        ])

        # "tests" and "pytest" in both
        assert result["criteria_validation_rate"] == 100.0
        assert result["fully_validated_tasks"] == 1

    def test_criterion_not_validated_when_missing_from_command(self):
        """Verify criterion not validated when keywords missing."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Type checking passes"],
                "verification_command": "pytest tests/",
            }
        ])

        assert result["criteria_validation_rate"] == 0.0
        assert result["untested_criteria_count"] == 1

    def test_multiple_criteria_partial_validation(self):
        """Verify partial validation of multiple criteria."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": [
                    "All tests pass",
                    "Type checking passes",
                    "Coverage exceeds 90%",
                ],
                "verification_command": "pytest --cov=src tests/",
            }
        ])

        # "tests" and "coverage" validated, "type" not
        assert result["criteria_validation_rate"] > 0.0
        assert result["criteria_validation_rate"] < 100.0

    def test_specificity_score_with_numbers(self):
        """Verify specificity score includes criteria with numbers."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Coverage exceeds 90%"]
            }
        ])

        # Has percentage, measurable verb (exceeds)
        assert result["criteria_specificity_score"] > 0.3

    def test_specificity_score_with_measurable_verbs(self):
        """Verify specificity score for measurable verbs."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Tests pass without errors"]
            }
        ])

        # Has "pass" verb
        assert result["criteria_specificity_score"] > 0.0

    def test_alignment_score_full_validation(self):
        """Verify alignment score when all criteria validated."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Tests pass"],
                "verification_command": "pytest tests/",
                "verification_passed": True,
            }
        ])

        assert result["verification_to_criteria_alignment_score"] == 1.0

    def test_alignment_score_partial_validation(self):
        """Verify alignment score with partial validation."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": [
                    "Tests pass",
                    "Linting succeeds",
                ],
                "verification_command": "pytest tests/",
            }
        ])

        # 1 out of 2 = 0.5
        assert result["verification_to_criteria_alignment_score"] == 0.5

    def test_common_validation_gaps_identified(self):
        """Verify common validation gap patterns identified."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Test coverage exceeds 90%"]},
            {"acceptance_criteria": ["All tests have coverage"]},
            {"acceptance_criteria": ["Type checking passes"]},
        ])

        gap_patterns = [g["gap_pattern"] for g in result["common_validation_gaps"]]
        assert "test_coverage" in gap_patterns
        assert "type_checking" in gap_patterns

    def test_fully_validated_tasks_count(self):
        """Verify fully validated tasks are counted."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Tests pass"],
                "verification_command": "pytest tests/",
                "verification_passed": True,
            },
            {
                "acceptance_criteria": ["Tests pass"],
                "verification_command": "pytest tests/",
                "verification_passed": False,  # Failed
            },
            {
                "acceptance_criteria": ["Tests pass", "Types valid"],
                "verification_command": "pytest tests/",  # Not all validated
            }
        ])

        # Only first task fully validated and passed
        assert result["fully_validated_tasks"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            "not a dict",
            {"acceptance_criteria": ["Tests pass"]},
        ])

        assert result["total_tasks"] == 1

    def test_task_without_criteria_skipped(self):
        """Verify tasks without criteria don't affect criteria counts."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"task_id": "task1"},
            {"acceptance_criteria": []},
            {"acceptance_criteria": ["Tests pass"]},
        ])

        assert result["total_tasks"] == 3
        assert result["tasks_with_criteria"] == 1
        assert result["total_criteria"] == 1

    def test_empty_criterion_strings_skipped(self):
        """Verify empty criterion strings are skipped."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Tests pass", "", "   "]
            }
        ])

        assert result["total_criteria"] == 1

    def test_criteria_as_dict_format(self):
        """Verify criteria can be dict with criteria list."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": {
                    "criteria": ["Tests pass", "Coverage good"]
                }
            }
        ])

        assert result["total_criteria"] == 2

    def test_criteria_as_list_of_dicts(self):
        """Verify criteria can be list of dicts with criterion key."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": [
                    {"criterion": "Tests pass"},
                    {"criterion": "No errors"},
                ]
            }
        ])

        assert result["total_criteria"] == 2

    def test_multiple_packs_tracked(self):
        """Verify multiple packs are tracked."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"pack_id": "pack1", "acceptance_criteria": ["A"]},
            {"pack_id": "pack2", "acceptance_criteria": ["B"]},
            {"pack_id": "pack1", "acceptance_criteria": ["C"]},
        ])

        assert result["total_packs"] == 2
        assert result["total_tasks"] == 3

    def test_gap_pattern_test_coverage(self):
        """Verify test coverage gap pattern identified."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Test coverage exceeds 90%"]}
        ])

        assert result["common_validation_gaps"][0]["gap_pattern"] == "test_coverage"

    def test_gap_pattern_type_checking(self):
        """Verify type checking gap pattern identified."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Type checking passes"]}
        ])

        assert result["common_validation_gaps"][0]["gap_pattern"] == "type_checking"

    def test_gap_pattern_performance(self):
        """Verify performance gap pattern identified."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Performance meets requirements"]}
        ])

        assert result["common_validation_gaps"][0]["gap_pattern"] == "performance"

    def test_gap_pattern_error_handling(self):
        """Verify error handling gap pattern identified."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Error cases handled properly"]}
        ])

        assert result["common_validation_gaps"][0]["gap_pattern"] == "error_handling"

    def test_gap_counts_sorted_descending(self):
        """Verify common gaps sorted by frequency."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {"acceptance_criteria": ["Test coverage A"]},
            {"acceptance_criteria": ["Test coverage B"]},
            {"acceptance_criteria": ["Type checking C"]},
        ])

        # test_coverage appears twice, type_checking once
        assert result["common_validation_gaps"][0]["gap_pattern"] == "test_coverage"
        assert result["common_validation_gaps"][0]["count"] == 2

    def test_short_words_ignored_in_validation(self):
        """Verify short words (<4 chars) ignored in validation matching."""
        result = analyze_pack_task_acceptance_criteria_coverage([
            {
                "acceptance_criteria": ["Run the test"],
                "verification_command": "pytest",
            }
        ])

        # "test" matches, but "Run" and "the" too short
        assert result["criteria_validation_rate"] == 100.0
