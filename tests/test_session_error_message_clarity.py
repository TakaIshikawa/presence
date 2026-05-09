"""Tests for session error message clarity analyzer."""

import pytest

from synthesis.session_error_message_clarity import (
    analyze_session_error_message_clarity,
    _calculate_error_quality_score,
    _calculate_overall_clarity_score,
    _has_actionable_guidance,
    _has_location_info,
    _has_specific_error_type,
    _is_vague_error,
    _percentage,
)


class TestAnalyzeSessionErrorMessageClarity:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_error_message_clarity([])

        assert result['total_errors'] == 0
        assert result['errors_with_location'] == 0
        assert result['errors_with_actionable_guidance'] == 0
        assert result['vague_errors'] == 0
        assert result['errors_with_specific_types'] == 0
        assert result['location_rate'] == 0.0
        assert result['actionable_rate'] == 0.0
        assert result['vague_rate'] == 0.0
        assert result['specific_type_rate'] == 0.0
        assert result['avg_message_length'] == 0.0
        assert result['avg_message_complexity'] == 0.0
        assert result['clarity_score'] == 0.0
        assert result['examples'] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_error_message_clarity(None)
        assert result['total_errors'] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match='records must be a list'):
            analyze_session_error_message_clarity('not a list')

    def test_clear_error_with_location_and_guidance(self):
        """Verify clear error with location and actionable guidance is scored highly."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'TypeError at src/main.py:42: Expected string but got int. Please fix the type annotation.',
                'source': 'tool_result',
                'turn_index': 0,
                'tool_type': 'bash',
            }
        ])

        assert result['total_errors'] == 1
        assert result['errors_with_location'] == 1
        assert result['errors_with_actionable_guidance'] == 1
        assert result['errors_with_specific_types'] == 1
        assert result['vague_errors'] == 0
        assert result['location_rate'] == 100.0
        assert result['actionable_rate'] == 100.0
        assert result['vague_rate'] == 0.0
        assert result['specific_type_rate'] == 100.0
        assert result['clarity_score'] > 80.0  # Should be high quality

    def test_vague_error_scored_poorly(self):
        """Verify vague error is scored poorly."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'Something went wrong',
                'source': 'agent_message',
                'turn_index': 0,
            }
        ])

        assert result['total_errors'] == 1
        assert result['vague_errors'] == 1
        assert result['vague_rate'] == 100.0
        assert result['errors_with_location'] == 0
        assert result['errors_with_actionable_guidance'] == 0
        assert result['clarity_score'] < 20.0  # Should be very low

    def test_error_with_location_but_no_guidance(self):
        """Verify error with location but no actionable guidance."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'Error in file.py:123',
                'source': 'tool_result',
                'turn_index': 0,
            }
        ])

        assert result['errors_with_location'] == 1
        assert result['errors_with_actionable_guidance'] == 0
        assert result['location_rate'] == 100.0
        assert result['actionable_rate'] == 0.0

    def test_error_with_guidance_but_no_location(self):
        """Verify error with actionable guidance but no location."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'Please update the configuration to fix this issue',
                'source': 'agent_message',
                'turn_index': 0,
            }
        ])

        assert result['errors_with_location'] == 0
        assert result['errors_with_actionable_guidance'] == 1
        assert result['location_rate'] == 0.0
        assert result['actionable_rate'] == 100.0

    def test_multiple_errors_with_mixed_quality(self):
        """Verify multiple errors with varying quality."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'ValueError at test.py:10: Invalid input. Please check the value.',
                'source': 'tool_result',
                'turn_index': 0,
            },
            {
                'message': 'Something went wrong',
                'source': 'agent_message',
                'turn_index': 1,
            },
            {
                'message': 'Error occurred',
                'source': 'tool_result',
                'turn_index': 2,
            },
        ])

        assert result['total_errors'] == 3
        assert result['errors_with_location'] == 1
        assert result['errors_with_actionable_guidance'] == 1
        assert result['vague_errors'] == 2
        assert result['errors_with_specific_types'] == 1
        assert result['location_rate'] == pytest.approx(33.33)
        assert result['vague_rate'] == pytest.approx(66.67)

    def test_message_length_calculation(self):
        """Verify average message length is calculated correctly."""
        result = analyze_session_error_message_clarity([
            {'message': 'Short error', 'source': 'tool_result', 'turn_index': 0},
            {'message': 'A much longer error message with more details', 'source': 'tool_result', 'turn_index': 1},
        ])

        total_length = len('Short error') + len('A much longer error message with more details')
        expected_avg = total_length / 2
        assert result['avg_message_length'] == pytest.approx(expected_avg)

    def test_message_complexity_calculation(self):
        """Verify average message complexity (word count) is calculated correctly."""
        result = analyze_session_error_message_clarity([
            {'message': 'Error occurred', 'source': 'tool_result', 'turn_index': 0},  # 2 words
            {'message': 'This is a longer message', 'source': 'tool_result', 'turn_index': 1},  # 5 words
        ])

        expected_avg = (2 + 5) / 2
        assert result['avg_message_complexity'] == pytest.approx(expected_avg)

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        records = [
            {
                'message': f'Error {i}',
                'source': 'tool_result',
                'turn_index': i,
            }
            for i in range(10)
        ]

        result = analyze_session_error_message_clarity(records)
        assert len(result['examples']) == 5

    def test_example_structure(self):
        """Verify example contains expected fields."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'TypeError at main.py:42: Fix the type',
                'source': 'tool_result',
                'turn_index': 5,
                'tool_type': 'pytest',
            }
        ])

        example = result['examples'][0]
        assert example['turn_index'] == 5
        assert example['source'] == 'tool_result'
        assert example['tool_type'] == 'pytest'
        assert 'TypeError' in example['message_excerpt']
        assert example['length'] > 0
        assert example['word_count'] > 0
        assert example['has_location'] is True
        assert example['has_actionable_guidance'] is True
        assert example['is_vague'] is False
        assert example['has_specific_type'] is True
        assert example['quality_score'] > 0

    def test_long_message_truncated_in_example(self):
        """Verify long messages are truncated in examples."""
        long_message = 'Error: ' + 'x' * 300
        result = analyze_session_error_message_clarity([
            {
                'message': long_message,
                'source': 'tool_result',
                'turn_index': 0,
            }
        ])

        example = result['examples'][0]
        assert len(example['message_excerpt']) == 200

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_error_message_clarity([
            'not a dict',
            {'message': 'Real error', 'source': 'tool_result', 'turn_index': 0},
        ])

        assert result['total_errors'] == 1

    def test_empty_message_skipped(self):
        """Verify records with empty messages are skipped."""
        result = analyze_session_error_message_clarity([
            {'message': '', 'source': 'tool_result', 'turn_index': 0},
            {'message': '   ', 'source': 'tool_result', 'turn_index': 1},
            {'message': 'Real error', 'source': 'tool_result', 'turn_index': 2},
        ])

        assert result['total_errors'] == 1

    def test_missing_turn_index_uses_record_index(self):
        """Verify missing turn_index uses record index."""
        result = analyze_session_error_message_clarity([
            {'message': 'Error 1', 'source': 'tool_result'},
        ])

        assert result['examples'][0]['turn_index'] == 0

    def test_missing_source_handled(self):
        """Verify missing source is handled gracefully."""
        result = analyze_session_error_message_clarity([
            {'message': 'Error without source', 'turn_index': 0},
        ])

        assert result['examples'][0]['source'] == 'unknown'

    def test_missing_tool_type_handled(self):
        """Verify missing tool_type is handled gracefully."""
        result = analyze_session_error_message_clarity([
            {'message': 'Error without tool', 'source': 'tool_result', 'turn_index': 0},
        ])

        assert result['examples'][0]['tool_type'] is None


class TestHasLocationInfo:
    """Test location info detection helper."""

    def test_python_file_line_detected(self):
        """Verify Python file:line pattern is detected."""
        assert _has_location_info('Error in main.py:42') is True
        assert _has_location_info('Traceback in src/utils/helper.py:123') is True

    def test_typescript_file_line_detected(self):
        """Verify TypeScript file:line pattern is detected."""
        assert _has_location_info('Error in app.ts:99') is True

    def test_javascript_file_line_detected(self):
        """Verify JavaScript file:line pattern is detected."""
        assert _has_location_info('Error in main.js:5') is True

    def test_java_file_line_detected(self):
        """Verify Java file:line pattern is detected."""
        assert _has_location_info('Error in Main.java:200') is True

    def test_generic_line_number_detected(self):
        """Verify generic 'line N' pattern is detected."""
        assert _has_location_info('Error on line 42') is True
        assert _has_location_info('Failed at line 123') is True

    def test_at_location_pattern_detected(self):
        """Verify 'at file:line' pattern is detected."""
        assert _has_location_info('Exception at main.py:10') is True

    def test_no_location_returns_false(self):
        """Verify message without location returns False."""
        assert _has_location_info('Something went wrong') is False
        assert _has_location_info('Error occurred') is False

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _has_location_info('') is False

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _has_location_info('Error at MAIN.PY:42') is True


class TestHasActionableGuidance:
    """Test actionable guidance detection helper."""

    def test_fix_keyword_detected(self):
        """Verify 'fix' keyword is detected."""
        assert _has_actionable_guidance('Please fix the issue') is True

    def test_change_keyword_detected(self):
        """Verify 'change' keyword is detected."""
        assert _has_actionable_guidance('You should change the value') is True

    def test_update_keyword_detected(self):
        """Verify 'update' keyword is detected."""
        assert _has_actionable_guidance('Update the configuration') is True

    def test_install_keyword_detected(self):
        """Verify 'install' keyword is detected."""
        assert _has_actionable_guidance('Install the missing dependency') is True

    def test_check_keyword_detected(self):
        """Verify 'check' keyword is detected."""
        assert _has_actionable_guidance('Check the input value') is True

    def test_try_keyword_detected(self):
        """Verify 'try' keyword is detected."""
        assert _has_actionable_guidance('Try running the command again') is True

    def test_should_keyword_detected(self):
        """Verify 'should' keyword is detected."""
        assert _has_actionable_guidance('You should verify the input') is True

    def test_expected_keyword_detected(self):
        """Verify 'expected' keyword is detected."""
        assert _has_actionable_guidance('Expected string but got int') is True

    def test_no_actionable_keywords_returns_false(self):
        """Verify message without actionable keywords returns False."""
        assert _has_actionable_guidance('Error occurred') is False
        assert _has_actionable_guidance('Something went wrong') is False

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _has_actionable_guidance('') is False

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _has_actionable_guidance('PLEASE FIX THIS') is True


class TestIsVagueError:
    """Test vague error detection helper."""

    def test_something_went_wrong_detected(self):
        """Verify 'something went wrong' pattern is detected."""
        assert _is_vague_error('Something went wrong') is True
        assert _is_vague_error('Oops, something went wrong!') is True

    def test_error_occurred_detected(self):
        """Verify 'error occurred' pattern is detected."""
        assert _is_vague_error('An error occurred') is True
        assert _is_vague_error('Error occurred during processing') is True

    def test_failed_without_context_detected(self):
        """Verify 'failed' without context is detected."""
        assert _is_vague_error('Failed') is True

    def test_failed_with_context_not_detected(self):
        """Verify 'failed' with context is not vague."""
        assert _is_vague_error('Failed to connect') is False
        assert _is_vague_error('Test failed at assertion') is False

    def test_unexpected_error_detected(self):
        """Verify 'unexpected error' pattern is detected."""
        assert _is_vague_error('An unexpected error occurred') is True

    def test_unknown_error_detected(self):
        """Verify 'unknown error' pattern is detected."""
        assert _is_vague_error('Unknown error') is True

    def test_specific_error_not_vague(self):
        """Verify specific errors are not detected as vague."""
        assert _is_vague_error('TypeError: expected string') is False
        assert _is_vague_error('File not found: config.json') is False

    def test_empty_string_returns_true(self):
        """Verify empty string is considered vague."""
        assert _is_vague_error('') is True

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _is_vague_error('SOMETHING WENT WRONG') is True


class TestHasSpecificErrorType:
    """Test specific error type detection helper."""

    def test_type_error_detected(self):
        """Verify TypeError is detected."""
        assert _has_specific_error_type('TypeError: expected string') is True

    def test_value_error_detected(self):
        """Verify ValueError is detected."""
        assert _has_specific_error_type('ValueError: invalid value') is True

    def test_attribute_error_detected(self):
        """Verify AttributeError is detected."""
        assert _has_specific_error_type('AttributeError: no attribute') is True

    def test_key_error_detected(self):
        """Verify KeyError is detected."""
        assert _has_specific_error_type('KeyError: missing key') is True

    def test_file_not_found_error_detected(self):
        """Verify FileNotFoundError is detected."""
        assert _has_specific_error_type('FileNotFoundError: file.txt') is True

    def test_import_error_detected(self):
        """Verify ImportError is detected."""
        assert _has_specific_error_type('ImportError: cannot import') is True

    def test_syntax_error_detected(self):
        """Verify SyntaxError is detected."""
        assert _has_specific_error_type('SyntaxError: invalid syntax') is True

    def test_no_specific_type_returns_false(self):
        """Verify message without specific error type returns False."""
        assert _has_specific_error_type('Something went wrong') is False
        assert _has_specific_error_type('Error occurred') is False

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _has_specific_error_type('') is False

    def test_case_sensitive_detection(self):
        """Verify detection is case-sensitive (matches Python exception names)."""
        assert _has_specific_error_type('TypeError') is True
        assert _has_specific_error_type('typeerror') is False


class TestCalculateErrorQualityScore:
    """Test error quality score calculation helper."""

    def test_perfect_error_scores_high(self):
        """Verify error with all quality indicators scores 85."""
        score = _calculate_error_quality_score(
            has_location=True,
            has_actionable=True,
            is_vague=False,
            has_specific_type=True,
        )
        assert score == 85

    def test_vague_error_scores_zero(self):
        """Verify vague error without any quality indicators scores 0."""
        score = _calculate_error_quality_score(
            has_location=False,
            has_actionable=False,
            is_vague=True,
            has_specific_type=False,
        )
        assert score == 0

    def test_location_only_scores_35(self):
        """Verify error with only location scores 35."""
        score = _calculate_error_quality_score(
            has_location=True,
            has_actionable=False,
            is_vague=False,
            has_specific_type=False,
        )
        assert score == 35

    def test_actionable_only_scores_30(self):
        """Verify error with only actionable guidance scores 30."""
        score = _calculate_error_quality_score(
            has_location=False,
            has_actionable=True,
            is_vague=False,
            has_specific_type=False,
        )
        assert score == 30

    def test_specific_type_only_scores_20(self):
        """Verify error with only specific type scores 20."""
        score = _calculate_error_quality_score(
            has_location=False,
            has_actionable=False,
            is_vague=False,
            has_specific_type=True,
        )
        assert score == 20

    def test_vague_error_with_location_still_penalized(self):
        """Verify vague error is penalized even with location."""
        score = _calculate_error_quality_score(
            has_location=True,
            has_actionable=False,
            is_vague=True,
            has_specific_type=False,
        )
        assert score == 0  # 35 - 50 = -15, clamped to 0

    def test_score_clamped_to_100(self):
        """Verify score doesn't exceed 100."""
        # This shouldn't happen in practice, but test the boundary
        score = _calculate_error_quality_score(
            has_location=True,
            has_actionable=True,
            is_vague=False,
            has_specific_type=True,
        )
        assert score <= 100


class TestCalculateOverallClarityScore:
    """Test overall clarity score calculation helper."""

    def test_perfect_session_scores_100(self):
        """Verify session with all high-quality errors scores 100."""
        score = _calculate_overall_clarity_score(
            location_rate=100.0,
            actionable_rate=100.0,
            vague_rate=0.0,
            specific_type_rate=100.0,
        )
        assert score == 100.0

    def test_all_vague_errors_scores_zero(self):
        """Verify session with all vague errors scores 0."""
        score = _calculate_overall_clarity_score(
            location_rate=0.0,
            actionable_rate=0.0,
            vague_rate=100.0,
            specific_type_rate=0.0,
        )
        assert score == 0.0

    def test_mixed_quality_session(self):
        """Verify mixed quality session calculates correctly."""
        score = _calculate_overall_clarity_score(
            location_rate=50.0,
            actionable_rate=50.0,
            vague_rate=25.0,
            specific_type_rate=50.0,
        )
        # 50*0.35 + 50*0.30 + 50*0.20 + (100-25)*0.15 = 17.5 + 15 + 10 + 11.25 = 53.75
        assert score == 53.75

    def test_score_rounded_to_two_decimals(self):
        """Verify score is rounded to 2 decimal places."""
        score = _calculate_overall_clarity_score(
            location_rate=33.33,
            actionable_rate=66.67,
            vague_rate=10.0,
            specific_type_rate=50.0,
        )
        # Should have exactly 2 decimal places
        assert score == round(score, 2)


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


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_session_with_no_errors(self):
        """Verify session with no errors returns zeroed metrics."""
        result = analyze_session_error_message_clarity([])
        assert result['total_errors'] == 0
        assert result['clarity_score'] == 0.0

    def test_session_with_all_clear_errors(self):
        """Verify session with all clear errors scores highly."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'TypeError at main.py:10: Expected str, got int. Please fix the type.',
                'source': 'tool_result',
                'turn_index': 0,
            },
            {
                'message': 'ValueError at utils.py:25: Invalid input. Check the value range.',
                'source': 'tool_result',
                'turn_index': 1,
            },
        ])

        assert result['total_errors'] == 2
        assert result['location_rate'] == 100.0
        assert result['actionable_rate'] == 100.0
        assert result['vague_rate'] == 0.0
        assert result['specific_type_rate'] == 100.0
        assert result['clarity_score'] == 100.0

    def test_session_with_all_vague_errors(self):
        """Verify session with all vague errors scores poorly."""
        result = analyze_session_error_message_clarity([
            {'message': 'Something went wrong', 'source': 'agent_message', 'turn_index': 0},
            {'message': 'Error occurred', 'source': 'tool_result', 'turn_index': 1},
            {'message': 'Failed', 'source': 'tool_result', 'turn_index': 2},
        ])

        assert result['total_errors'] == 3
        assert result['vague_rate'] == 100.0
        assert result['clarity_score'] < 20.0

    def test_debugging_session_with_improving_errors(self):
        """Verify debugging session where errors improve over time."""
        result = analyze_session_error_message_clarity([
            {'message': 'Error', 'source': 'tool_result', 'turn_index': 0},
            {'message': 'Error in tests', 'source': 'tool_result', 'turn_index': 1},
            {
                'message': 'AssertionError at test_main.py:15: Expected 5, got 3. Fix the calculation.',
                'source': 'tool_result',
                'turn_index': 2,
            },
        ])

        # Last error is much better quality
        assert result['total_errors'] == 3
        assert result['examples'][2]['quality_score'] > result['examples'][0]['quality_score']

    def test_session_with_mixed_sources(self):
        """Verify session with errors from different sources."""
        result = analyze_session_error_message_clarity([
            {
                'message': 'ImportError: module not found. Install the package.',
                'source': 'tool_result',
                'tool_type': 'bash',
                'turn_index': 0,
            },
            {
                'message': 'The test failed at line 42. You should check the logic.',
                'source': 'agent_message',
                'turn_index': 1,
            },
        ])

        assert result['total_errors'] == 2
        assert result['examples'][0]['source'] == 'tool_result'
        assert result['examples'][0]['tool_type'] == 'bash'
        assert result['examples'][1]['source'] == 'agent_message'

    def test_real_world_typescript_error(self):
        """Verify real-world TypeScript error is analyzed correctly."""
        result = analyze_session_error_message_clarity([
            {
                'message': "error TS2322: Type 'string' is not assignable to type 'number' at app.ts:42. "
                           "Change the type annotation or update the value.",
                'source': 'tool_result',
                'tool_type': 'bash',
                'turn_index': 0,
            }
        ])

        assert result['errors_with_location'] == 1
        assert result['errors_with_actionable_guidance'] == 1
        assert result['vague_errors'] == 0
        assert result['clarity_score'] > 70.0

    def test_real_world_python_traceback(self):
        """Verify real-world Python traceback is analyzed correctly."""
        result = analyze_session_error_message_clarity([
            {
                'message': '''Traceback (most recent call last):
  File "main.py", line 42, in process_data
    return data['key']
KeyError: 'key'

Fix: Check if 'key' exists before accessing it.''',
                'source': 'tool_result',
                'tool_type': 'pytest',
                'turn_index': 0,
            }
        ])

        assert result['errors_with_location'] == 1
        assert result['errors_with_actionable_guidance'] == 1
        assert result['errors_with_specific_types'] == 1
        assert result['vague_errors'] == 0

    def test_correlation_between_clarity_and_length(self):
        """Verify longer messages don't automatically mean better clarity."""
        short_clear = 'TypeError at main.py:10: Fix the type'
        long_vague = 'An error occurred during the processing of your request. ' * 5

        result = analyze_session_error_message_clarity([
            {'message': short_clear, 'source': 'tool_result', 'turn_index': 0},
            {'message': long_vague, 'source': 'tool_result', 'turn_index': 1},
        ])

        # Short clear error should have better quality score
        assert result['examples'][0]['quality_score'] > result['examples'][1]['quality_score']
        # But long error has greater length
        assert result['examples'][1]['length'] > result['examples'][0]['length']
