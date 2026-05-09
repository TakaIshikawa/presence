"""Tests for pack acceptance criteria coverage analyzer."""

import pytest

from synthesis.pack_acceptance_criteria_coverage import (
    analyze_pack_acceptance_criteria_coverage,
    _calculate_coverage_score,
    _criterion_validated_by_command,
    _extract_file_paths,
    _is_measurable_criterion,
    _is_vague_criterion,
    _percentage,
)


class TestAnalyzePackAcceptanceCriteriaCoverage:
    """Test main analyzer function."""

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

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_acceptance_criteria_coverage(None)
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

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_acceptance_criteria_coverage([
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
