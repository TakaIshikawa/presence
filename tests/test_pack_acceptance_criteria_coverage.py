"""Tests for pack acceptance criteria coverage analyzer."""

import pytest

<<<<<<< HEAD
from synthesis.pack_acceptance_criteria_coverage import analyze_pack_acceptance_criteria_coverage
=======
from synthesis.pack_acceptance_criteria_coverage import (
    analyze_pack_acceptance_criteria_coverage,
    _calculate_coverage_score,
    _criterion_validated_by_command,
    _extract_file_paths,
    _is_measurable_criterion,
    _is_vague_criterion,
    _percentage,
)
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME


class TestAnalyzePackAcceptanceCriteriaCoverage:
    """Test main analyzer function."""

<<<<<<< HEAD
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
=======
    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_acceptance_criteria_coverage([])

        assert result['total_tasks'] == 0
        assert result['tasks_with_criteria'] == 0
        assert result['tasks_without_criteria'] == 0
        assert result['total_criteria'] == 0
        assert result['avg_criteria_per_task'] == 0.0
        assert result['measurable_criteria'] == 0
        assert result['vague_criteria'] == 0
        assert result['measurability_rate'] == 0.0
        assert result['tasks_with_test_commands'] == 0
        assert result['tasks_with_aligned_verification'] == 0
        assert result['unvalidated_criteria_count'] == 0
        assert result['coverage_score'] == 0.0
        assert result['examples'] == []
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_acceptance_criteria_coverage(None)
<<<<<<< HEAD
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
=======
        assert result['total_tasks'] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match='records must be a list'):
            analyze_pack_acceptance_criteria_coverage('not a list')

    def test_task_without_criteria(self):
        """Verify task without criteria is counted correctly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [],
                'test_command': 'pytest tests/',
            }
        ])

        assert result['total_tasks'] == 1
        assert result['tasks_without_criteria'] == 1
        assert result['tasks_with_criteria'] == 0

    def test_task_with_measurable_criteria(self):
        """Verify task with measurable criteria is scored correctly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Tests pass for error detection',
                    'Function returns correct value',
                    'Assert proper validation',
                ],
                'test_command': 'pytest tests/test_module.py -v',
                'expected_files': ['src/module.py', 'tests/test_module.py'],
            }
        ])

        assert result['total_tasks'] == 1
        assert result['tasks_with_criteria'] == 1
        assert result['total_criteria'] == 3
        assert result['measurable_criteria'] == 3  # All have measurable keywords
        assert result['measurability_rate'] == 100.0

    def test_task_with_vague_criteria(self):
        """Verify task with vague criteria is identified."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Improve the code quality',
                    'Ensure better performance',
                    'Optimize various functions',
                ],
                'test_command': 'pytest tests/',
            }
        ])

        assert result['total_criteria'] == 3
        assert result['vague_criteria'] == 3  # All have vague terms
        assert result['coverage_score'] < 50.0  # Should score poorly

    def test_mixed_measurable_and_vague_criteria(self):
        """Verify mixed criteria quality."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Tests verify input validation',  # Measurable
                    'Improve error handling',  # Vague
                    'Function returns status code',  # Measurable
                ],
                'test_command': 'pytest tests/',
            }
        ])

        assert result['total_criteria'] == 3
        assert result['measurable_criteria'] == 2
        assert result['vague_criteria'] == 1

    def test_task_with_test_command(self):
        """Verify task with test command is counted."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': ['Tests pass'],
                'test_command': 'pytest tests/test_foo.py',
            }
        ])

        assert result['tasks_with_test_commands'] == 1

    def test_task_without_test_command(self):
        """Verify task without test command has unvalidated criteria."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': ['Tests pass', 'Code compiles'],
                'test_command': '',
            }
        ])

        assert result['tasks_with_test_commands'] == 0
        assert result['unvalidated_criteria_count'] == 2  # Both criteria unvalidated

    def test_aligned_verification(self):
        """Verify task with aligned verification is counted."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Tests pass for session_cache module',
                    'Function calculates cache hit ratio correctly',
                ],
                'test_command': 'pytest tests/test_session_cache_hit_ratio.py -v',
                'expected_files': ['src/synthesis/session_cache_hit_ratio.py', 'tests/test_session_cache_hit_ratio.py'],
            }
        ])

        assert result['tasks_with_aligned_verification'] == 1
        assert result['unvalidated_criteria_count'] == 0

    def test_avg_criteria_per_task_calculation(self):
        """Verify average criteria per task is calculated correctly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': ['AC1', 'AC2'],
                'test_command': 'pytest tests/',
            },
            {
                'task_id': 'task-2',
                'acceptance_criteria': ['AC1', 'AC2', 'AC3', 'AC4'],
                'test_command': 'pytest tests/',
            },
        ])

        # (2 + 4) / 2 = 3.0
        assert result['avg_criteria_per_task'] == 3.0

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        tasks = [
            {
                'task_id': f'task-{i}',
                'acceptance_criteria': [],
                'test_command': '',
            }
            for i in range(10)
        ]

        result = analyze_pack_acceptance_criteria_coverage(tasks)
        assert len(result['examples']) == 5

    def test_missing_criteria_example_structure(self):
        """Verify missing criteria example contains expected fields."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-123',
                'acceptance_criteria': [],
                'test_command': 'pytest tests/',
            }
        ])

        example = result['examples'][0]
        assert example['task_id'] == 'task-123'
        assert example['issue'] == 'missing_criteria'
        assert 'description' in example

    def test_unvalidated_criterion_example_structure(self):
        """Verify unvalidated criterion example contains expected fields."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-456',
                'acceptance_criteria': ['Some criterion not covered by tests'],
                'test_command': 'pytest tests/unrelated.py',
                'expected_files': ['src/other.py'],
            }
        ])

        # Should have an unvalidated criterion example
        unvalidated_examples = [ex for ex in result['examples'] if ex['issue'] == 'unvalidated_criterion']
        assert len(unvalidated_examples) > 0
        example = unvalidated_examples[0]
        assert example['task_id'] == 'task-456'
        assert 'criterion' in example
        assert 'test_command' in example

    def test_vague_criteria_example_structure(self):
        """Verify vague criteria example contains expected fields."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-789',
                'acceptance_criteria': ['Improve the performance'],
                'test_command': 'pytest tests/',
            }
        ])

        vague_examples = [ex for ex in result['examples'] if ex['issue'] == 'vague_criteria']
        assert len(vague_examples) > 0
        example = vague_examples[0]
        assert example['task_id'] == 'task-789'
        assert 'criterion' in example
        assert 'Improve' in example['criterion']
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_acceptance_criteria_coverage([
<<<<<<< HEAD
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
=======
            'not a dict',
            {
                'task_id': 'task-1',
                'acceptance_criteria': ['AC1'],
                'test_command': 'pytest',
            },
        ])

        assert result['total_tasks'] == 1

    def test_empty_criterion_string_handled(self):
        """Verify empty criterion strings are handled gracefully."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': ['Valid AC', '', '   ', 'Another AC'],
                'test_command': 'pytest',
            }
        ])

        # Should still count all criteria even if some are empty
        assert result['total_criteria'] == 4


class TestIsMeasurableCriterion:
    """Test measurable criterion detection helper."""

    def test_test_keyword_detected(self):
        """Verify 'test' keyword makes criterion measurable."""
        assert _is_measurable_criterion('All tests pass') is True

    def test_verify_keyword_detected(self):
        """Verify 'verify' keyword makes criterion measurable."""
        assert _is_measurable_criterion('Verify input validation') is True

    def test_assert_keyword_detected(self):
        """Verify 'assert' keyword makes criterion measurable."""
        assert _is_measurable_criterion('Assert correct behavior') is True

    def test_check_keyword_detected(self):
        """Verify 'check' keyword makes criterion measurable."""
        assert _is_measurable_criterion('Check error handling') is True

    def test_return_keyword_detected(self):
        """Verify 'return' keyword makes criterion measurable."""
        assert _is_measurable_criterion('Function returns correct value') is True

    def test_detect_keyword_detected(self):
        """Verify 'detect' keyword makes criterion measurable."""
        assert _is_measurable_criterion('Detects invalid input') is True

    def test_no_measurable_keywords_returns_false(self):
        """Verify criterion without measurable keywords returns False."""
        assert _is_measurable_criterion('Code is good') is False

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _is_measurable_criterion('') is False

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _is_measurable_criterion('TESTS PASS') is True


class TestIsVagueCriterion:
    """Test vague criterion detection helper."""

    def test_improve_keyword_detected(self):
        """Verify 'improve' makes criterion vague."""
        assert _is_vague_criterion('Improve code quality') is True

    def test_better_keyword_detected(self):
        """Verify 'better' makes criterion vague."""
        assert _is_vague_criterion('Better error handling') is True

    def test_enhance_keyword_detected(self):
        """Verify 'enhance' makes criterion vague."""
        assert _is_vague_criterion('Enhance performance') is True

    def test_optimize_keyword_detected(self):
        """Verify 'optimize' makes criterion vague."""
        assert _is_vague_criterion('Optimize various functions') is True

    def test_ensure_quality_detected(self):
        """Verify 'ensure quality' makes criterion vague."""
        assert _is_vague_criterion('Ensure quality standards') is True

    def test_no_vague_terms_returns_false(self):
        """Verify criterion without vague terms returns False."""
        assert _is_vague_criterion('Tests pass for module') is False

    def test_empty_string_returns_true(self):
        """Verify empty string is considered vague."""
        assert _is_vague_criterion('') is True

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _is_vague_criterion('IMPROVE PERFORMANCE') is True


class TestCriterionValidatedByCommand:
    """Test criterion validation by command helper."""

    def test_file_overlap_validates_criterion(self):
        """Verify file overlap between criterion and test command validates it."""
        assert _criterion_validated_by_command(
            'Tests pass for session_cache_hit_ratio.py',
            'pytest tests/test_session_cache_hit_ratio.py',
            ['src/synthesis/session_cache_hit_ratio.py', 'tests/test_session_cache_hit_ratio.py'],
        ) is True

    def test_keyword_overlap_validates_criterion(self):
        """Verify keyword overlap validates criterion."""
        # File in expected_files is tested by command
        assert _criterion_validated_by_command(
            'Error detection works correctly',
            'pytest tests/test_error_detection.py -v',
            ['tests/test_error_detection.py'],  # File is in test command
        ) is True

    def test_no_overlap_returns_false(self):
        """Verify no overlap returns False."""
        assert _criterion_validated_by_command(
            'Authentication works properly',
            'pytest tests/test_database.py',
            ['src/auth.py'],
        ) is False

    def test_empty_test_command_returns_false(self):
        """Verify empty test command returns False."""
        assert _criterion_validated_by_command(
            'Tests pass',
            '',
            ['src/module.py'],
        ) is False

    def test_expected_files_in_command(self):
        """Verify expected files in test command validates criterion."""
        assert _criterion_validated_by_command(
            'Module functionality verified',
            'pytest tests/test_module.py',
            ['tests/test_module.py'],
        ) is True


class TestExtractFilePaths:
    """Test file path extraction helper."""

    def test_extract_python_file(self):
        """Verify Python file path is extracted."""
        paths = _extract_file_paths('Tests for src/module.py pass')
        assert 'src/module.py' in paths

    def test_extract_typescript_file(self):
        """Verify TypeScript file path is extracted."""
        paths = _extract_file_paths('Check app/main.ts functionality')
        assert 'app/main.ts' in paths

    def test_extract_multiple_files(self):
        """Verify multiple files are extracted."""
        paths = _extract_file_paths('Tests in test_a.py and test_b.py pass')
        assert 'test_a.py' in paths
        assert 'test_b.py' in paths

    def test_no_files_returns_empty_list(self):
        """Verify text without files returns empty list."""
        paths = _extract_file_paths('This is just text without any files')
        assert paths == []


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_negative_denominator_returns_zero(self):
        """Verify negative denominator returns 0.0."""
        assert _percentage(10, -5) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_equal_values_returns_100(self):
        """Verify equal values return 100.0."""
        assert _percentage(10, 10) == 100.0

    def test_half_returns_50(self):
        """Verify half returns 50.0."""
        assert _percentage(5, 10) == 50.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestCalculateCoverageScore:
    """Test coverage score calculation helper."""

    def test_perfect_coverage_scores_high(self):
        """Verify perfect coverage scores high."""
        score = _calculate_coverage_score(
            criteria_presence_rate=100.0,
            measurability_rate=100.0,
            alignment_rate=100.0,
            vague_rate=0.0,
        )
        # 100*0.25 + 100*0.30 + 100*0.30 - 0*0.15 = 85
        assert score == 85.0

    def test_no_criteria_scores_zero(self):
        """Verify no criteria scores 0."""
        score = _calculate_coverage_score(
            criteria_presence_rate=0.0,
            measurability_rate=0.0,
            alignment_rate=0.0,
            vague_rate=0.0,
        )
        assert score == 0.0

    def test_high_vague_rate_penalized(self):
        """Verify high vague rate is penalized."""
        score = _calculate_coverage_score(
            criteria_presence_rate=100.0,
            measurability_rate=50.0,
            alignment_rate=50.0,
            vague_rate=80.0,
        )
        # 100*0.25 + 50*0.30 + 50*0.30 - 80*0.15 = 25 + 15 + 15 - 12 = 43
        assert score == 43.0

    def test_mixed_quality(self):
        """Verify mixed quality calculates correctly."""
        score = _calculate_coverage_score(
            criteria_presence_rate=80.0,
            measurability_rate=70.0,
            alignment_rate=60.0,
            vague_rate=20.0,
        )
        # 80*0.25 + 70*0.30 + 60*0.30 - 20*0.15 = 20 + 21 + 18 - 3 = 56
        assert score == 56.0

    def test_score_clamped_to_zero(self):
        """Verify score doesn't go below 0."""
        score = _calculate_coverage_score(
            criteria_presence_rate=0.0,
            measurability_rate=0.0,
            alignment_rate=0.0,
            vague_rate=100.0,
        )
        # 0*0.25 + 0*0.30 + 0*0.30 - 100*0.15 = -15, clamped to 0
        assert score == 0.0

    def test_score_clamped_to_100(self):
        """Verify score doesn't exceed 100."""
        score = _calculate_coverage_score(
            criteria_presence_rate=200.0,
            measurability_rate=200.0,
            alignment_rate=200.0,
            vague_rate=0.0,
        )
        assert score == 100.0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_well_defined_pack(self):
        """Verify pack with well-defined criteria scores highly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Tests pass for cache module',
                    'Function calculates hit rate correctly',
                    'Validates cache snapshot operations',
                ],
                'test_command': 'pytest tests/test_cache.py -v',
                'expected_files': ['src/cache.py', 'tests/test_cache.py'],
            },
            {
                'task_id': 'task-2',
                'acceptance_criteria': [
                    'Error detection tests pass',
                    'Returns proper error codes',
                ],
                'test_command': 'pytest tests/test_errors.py -v',
                'expected_files': ['src/errors.py', 'tests/test_errors.py'],
            },
        ])

        assert result['total_tasks'] == 2
        assert result['tasks_with_criteria'] == 2
        assert result['measurability_rate'] == 100.0
        assert result['tasks_with_aligned_verification'] == 2
        assert result['coverage_score'] > 70.0

    def test_poorly_defined_pack(self):
        """Verify pack with vague criteria scores poorly."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Improve the code',
                    'Better performance',
                ],
                'test_command': '',
            },
            {
                'task_id': 'task-2',
                'acceptance_criteria': [],
                'test_command': 'pytest tests/',
            },
        ])

        assert result['tasks_without_criteria'] == 1
        assert result['vague_criteria'] == 2
        assert result['unvalidated_criteria_count'] == 2
        assert result['coverage_score'] < 30.0

    def test_pack_with_missing_verification(self):
        """Verify pack with criteria but no verification."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'task-1',
                'acceptance_criteria': [
                    'Tests pass',
                    'Code compiles',
                ],
                'test_command': '',
            }
        ])

        assert result['unvalidated_criteria_count'] == 2
        assert result['tasks_with_aligned_verification'] == 0

    def test_real_world_pack_example(self):
        """Verify real-world pack with mixed quality."""
        result = analyze_pack_acceptance_criteria_coverage([
            {
                'task_id': 'add-error-analyzer',
                'acceptance_criteria': [
                    'Extracts error messages from session tool results and agent messages',
                    'Scores errors based on presence of location info and actionable guidance',
                    'Measures average error message length and complexity',
                    'Identifies patterns of clear vs vague error messages',
                    'Tests cover sessions with no errors, clear errors, and vague errors',
                ],
                'test_command': 'python -m pytest tests/test_session_error_message_clarity.py -v',
                'expected_files': ['src/synthesis/session_error_message_clarity.py', 'tests/test_session_error_message_clarity.py'],
            },
        ])

        assert result['total_criteria'] == 5
        # Criteria 1-3 have measurable keywords (Extracts, Scores, Measures, Identifies, Tests)
        # But not all may be detected depending on keyword matching
        assert result['measurable_criteria'] >= 2
        assert result['tasks_with_aligned_verification'] == 1
        assert result['coverage_score'] > 50.0
>>>>>>> relay/claude-code/add-session-error-message-clarity-analyzer-01KR3GME
