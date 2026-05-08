"""Tests for session bash command error handling analyzer."""

import pytest

from synthesis.session_bash_command_error_handling import (
    analyze_session_bash_command_error_handling,
    _contains_error_indicators,
    _is_acknowledged,
    _percentage,
)


class TestAnalyzeSessionBashCommandErrorHandling:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_bash_command_error_handling([])

        assert result["total_commands"] == 0
        assert result["failed_commands"] == 0
        assert result["acknowledged_failures"] == 0
        assert result["unhandled_errors"] == 0
        assert result["error_acknowledgement_rate"] == 0.0
        assert result["error_types"] == {
            "non_zero_exit": 0,
            "timeout": 0,
            "command_not_found": 0,
            "permission_denied": 0,
            "stderr_output": 0,
        }
        assert result["examples"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_bash_command_error_handling(None)
        assert result["total_commands"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_bash_command_error_handling("not a list")

    def test_successful_command_not_counted_as_failure(self):
        """Verify successful commands (exit 0) are not counted as failures."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "echo hello",
                "exit_code": 0,
                "stderr": "",
                "following_response": "The command succeeded.",
                "turn_index": 0,
            }
        ])

        assert result["total_commands"] == 1
        assert result["failed_commands"] == 0
        assert result["unhandled_errors"] == 0

    def test_non_zero_exit_code_counted_as_failure(self):
        """Verify non-zero exit code is counted as failure."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "false",
                "exit_code": 1,
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["error_types"]["non_zero_exit"] == 1
        assert result["unhandled_errors"] == 1

    def test_timeout_error_counted_as_failure(self):
        """Verify timeout is counted as failure."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "sleep 1000",
                "exit_code": 124,
                "stderr": "",
                "timed_out": True,
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["error_types"]["timeout"] == 1

    def test_command_not_found_error_counted(self):
        """Verify exit code 127 (command not found) is counted correctly."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "nonexistent_command",
                "exit_code": 127,
                "stderr": "bash: nonexistent_command: command not found",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["error_types"]["command_not_found"] == 1

    def test_permission_denied_error_counted(self):
        """Verify exit code 126 (permission denied) is counted correctly."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "./script.sh",
                "exit_code": 126,
                "stderr": "bash: ./script.sh: Permission denied",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["error_types"]["permission_denied"] == 1

    def test_stderr_with_error_indicators_counted_as_failure(self):
        """Verify stderr with error content counts as failure even with exit 0."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "some_command",
                "exit_code": 0,
                "stderr": "Error: something went wrong",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["error_types"]["stderr_output"] == 1

    def test_stderr_without_error_indicators_not_counted_as_failure(self):
        """Verify stderr without error indicators is not counted as failure."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "npm install",
                "exit_code": 0,
                "stderr": "npm WARN deprecated package@1.0.0",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        # Should count stderr but not as failure
        assert result["failed_commands"] == 0
        assert result["error_types"]["stderr_output"] == 1

    def test_acknowledged_failure_counted_correctly(self):
        """Verify acknowledged failures are counted correctly."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "pytest tests/",
                "exit_code": 1,
                "stderr": "FAILED tests/test_foo.py",
                "following_response": "The test failed. Let me fix the issue.",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["acknowledged_failures"] == 1
        assert result["unhandled_errors"] == 0
        assert result["error_acknowledgement_rate"] == 100.0

    def test_unhandled_error_counted_correctly(self):
        """Verify unhandled errors are counted correctly."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "pytest tests/",
                "exit_code": 1,
                "stderr": "FAILED tests/test_foo.py",
                "following_response": "Let me continue with the next task.",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1
        assert result["acknowledged_failures"] == 0
        assert result["unhandled_errors"] == 1
        assert result["error_acknowledgement_rate"] == 0.0

    def test_empty_following_response_counted_as_unhandled(self):
        """Verify empty following response is counted as unhandled."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "false",
                "exit_code": 1,
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["unhandled_errors"] == 1

    def test_acknowledgement_terms_detected(self):
        """Verify various acknowledgement terms are detected."""
        acknowledgement_responses = [
            "I see the error occurred.",
            "The command failed, let me retry.",
            "There was an issue with the exit code.",
            "Let me fix this problem.",
            "The stderr output shows a timeout.",
            "Permission was denied, I'll check that.",
            "The command was not found.",
        ]

        for response in acknowledgement_responses:
            result = analyze_session_bash_command_error_handling([
                {
                    "command": "test",
                    "exit_code": 1,
                    "stderr": "",
                    "following_response": response,
                    "turn_index": 0,
                }
            ])
            assert result["acknowledged_failures"] == 1, f"Failed to detect: {response}"

    def test_multiple_commands_with_mixed_outcomes(self):
        """Verify multiple commands with various outcomes."""
        result = analyze_session_bash_command_error_handling([
            {"command": "echo ok", "exit_code": 0, "stderr": "", "following_response": "", "turn_index": 0},
            {"command": "false", "exit_code": 1, "stderr": "", "following_response": "Error detected, fixing.", "turn_index": 1},
            {"command": "true", "exit_code": 0, "stderr": "", "following_response": "", "turn_index": 2},
            {"command": "fail", "exit_code": 1, "stderr": "", "following_response": "Moving on.", "turn_index": 3},
        ])

        assert result["total_commands"] == 4
        assert result["failed_commands"] == 2
        assert result["acknowledged_failures"] == 1
        assert result["unhandled_errors"] == 1
        assert result["error_acknowledgement_rate"] == 50.0

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        records = [
            {
                "command": f"command_{i}",
                "exit_code": 1,
                "stderr": "",
                "following_response": "",
                "turn_index": i,
            }
            for i in range(10)
        ]

        result = analyze_session_bash_command_error_handling(records)
        assert len(result["examples"]) == 5

    def test_example_structure(self):
        """Verify example contains expected fields."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "pytest tests/",
                "exit_code": 1,
                "stderr": "FAILED tests/test_foo.py",
                "following_response": "",
                "turn_index": 5,
            }
        ])

        example = result["examples"][0]
        assert example["turn_index"] == 5
        assert example["command"] == "pytest tests/"
        assert example["exit_code"] == 1
        assert "FAILED" in example["stderr_excerpt"]
        assert example["error_type"] == "non_zero_exit"

    def test_long_command_truncated_in_example(self):
        """Verify long commands are truncated in examples."""
        long_command = "a" * 200
        result = analyze_session_bash_command_error_handling([
            {
                "command": long_command,
                "exit_code": 1,
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        example = result["examples"][0]
        assert len(example["command"]) == 100

    def test_long_stderr_truncated_in_example(self):
        """Verify long stderr is truncated in examples."""
        long_stderr = "error: " + "x" * 300
        result = analyze_session_bash_command_error_handling([
            {
                "command": "test",
                "exit_code": 1,
                "stderr": long_stderr,
                "following_response": "",
                "turn_index": 0,
            }
        ])

        example = result["examples"][0]
        assert len(example["stderr_excerpt"]) == 200

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_bash_command_error_handling([
            "not a dict",
            {"command": "test", "exit_code": 1, "stderr": "", "following_response": "", "turn_index": 0},
        ])

        assert result["total_commands"] == 1

    def test_missing_exit_code_handled(self):
        """Verify missing exit_code is handled gracefully."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "test",
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["total_commands"] == 1
        assert result["failed_commands"] == 0

    def test_exit_code_as_string_converted(self):
        """Verify exit_code as string is converted to int."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "test",
                "exit_code": "1",
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1

    def test_exit_code_as_float_converted(self):
        """Verify exit_code as float is converted to int."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "test",
                "exit_code": 1.0,
                "stderr": "",
                "following_response": "",
                "turn_index": 0,
            }
        ])

        assert result["failed_commands"] == 1

    def test_missing_turn_index_uses_record_index(self):
        """Verify missing turn_index uses record index."""
        result = analyze_session_bash_command_error_handling([
            {
                "command": "test",
                "exit_code": 1,
                "stderr": "",
                "following_response": "",
            }
        ])

        assert result["examples"][0]["turn_index"] == 0


class TestContainsErrorIndicators:
    """Test error indicator detection helper."""

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _contains_error_indicators("") is False

    def test_none_returns_false(self):
        """Verify None returns False."""
        assert _contains_error_indicators("") is False

    def test_error_keyword_detected(self):
        """Verify 'error' keyword is detected."""
        assert _contains_error_indicators("Error: something went wrong") is True

    def test_fatal_keyword_detected(self):
        """Verify 'fatal' keyword is detected."""
        assert _contains_error_indicators("Fatal: cannot continue") is True

    def test_exception_keyword_detected(self):
        """Verify 'exception' keyword is detected."""
        assert _contains_error_indicators("Exception in thread") is True

    def test_failed_keyword_detected(self):
        """Verify 'failed' keyword is detected."""
        assert _contains_error_indicators("Test failed") is True

    def test_traceback_keyword_detected(self):
        """Verify 'traceback' keyword is detected."""
        assert _contains_error_indicators("Traceback (most recent call last)") is True

    def test_abort_keyword_detected(self):
        """Verify 'abort' keyword is detected."""
        assert _contains_error_indicators("Aborted") is True

    def test_warning_not_detected_as_error(self):
        """Verify warnings without error keywords don't count."""
        assert _contains_error_indicators("Warning: deprecated feature") is False

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _contains_error_indicators("ERROR: failed") is True
        assert _contains_error_indicators("Error: Failed") is True


class TestIsAcknowledged:
    """Test acknowledgement detection helper."""

    def test_empty_string_returns_false(self):
        """Verify empty string returns False."""
        assert _is_acknowledged("") is False

    def test_error_term_detected(self):
        """Verify 'error' term is detected."""
        assert _is_acknowledged("I see the error") is True

    def test_fail_term_detected(self):
        """Verify 'fail' term is detected."""
        assert _is_acknowledged("The command failed") is True

    def test_exit_term_detected(self):
        """Verify 'exit' term is detected."""
        assert _is_acknowledged("Non-zero exit code") is True

    def test_stderr_term_detected(self):
        """Verify 'stderr' term is detected."""
        assert _is_acknowledged("Looking at stderr output") is True

    def test_timeout_term_detected(self):
        """Verify 'timeout' term is detected."""
        assert _is_acknowledged("The command timed out") is True

    def test_retry_term_detected(self):
        """Verify 'retry' term is detected."""
        assert _is_acknowledged("Let me retry that") is True

    def test_fix_term_detected(self):
        """Verify 'fix' term is detected."""
        assert _is_acknowledged("I'll fix this issue") is True

    def test_issue_term_detected(self):
        """Verify 'issue' term is detected."""
        assert _is_acknowledged("There's an issue here") is True

    def test_problem_term_detected(self):
        """Verify 'problem' term is detected."""
        assert _is_acknowledged("The problem is clear") is True

    def test_not_found_term_detected(self):
        """Verify 'not found' term is detected."""
        assert _is_acknowledged("Command not found") is True

    def test_permission_term_detected(self):
        """Verify 'permission' term is detected."""
        assert _is_acknowledged("Permission denied") is True

    def test_denied_term_detected(self):
        """Verify 'denied' term is detected."""
        assert _is_acknowledged("Access denied") is True

    def test_unrelated_text_not_acknowledged(self):
        """Verify unrelated text is not detected as acknowledgement."""
        assert _is_acknowledged("Moving on to the next task") is False

    def test_case_insensitive_detection(self):
        """Verify detection is case-insensitive."""
        assert _is_acknowledged("ERROR detected") is True
        assert _is_acknowledged("Failed to execute") is True


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

    def test_typical_debugging_session(self):
        """Simulate a debugging session with errors and fixes."""
        result = analyze_session_bash_command_error_handling([
            {"command": "pytest tests/", "exit_code": 1, "stderr": "FAILED", "following_response": "Test failed, let me fix it", "turn_index": 0},
            {"command": "pytest tests/", "exit_code": 1, "stderr": "FAILED", "following_response": "Still failing, trying another fix", "turn_index": 1},
            {"command": "pytest tests/", "exit_code": 0, "stderr": "", "following_response": "Tests pass now", "turn_index": 2},
        ])

        assert result["total_commands"] == 3
        assert result["failed_commands"] == 2
        assert result["acknowledged_failures"] == 2
        assert result["error_acknowledgement_rate"] == 100.0

    def test_session_with_ignored_errors(self):
        """Simulate session where errors are ignored."""
        result = analyze_session_bash_command_error_handling([
            {"command": "npm install", "exit_code": 0, "stderr": "", "following_response": "", "turn_index": 0},
            {"command": "npm test", "exit_code": 1, "stderr": "Test failed", "following_response": "Now let me build", "turn_index": 1},
            {"command": "npm build", "exit_code": 1, "stderr": "Build failed", "following_response": "Deploying now", "turn_index": 2},
        ])

        assert result["failed_commands"] == 2
        assert result["unhandled_errors"] == 2
        assert result["error_acknowledgement_rate"] == 0.0

    def test_timeout_scenario(self):
        """Simulate timeout errors."""
        result = analyze_session_bash_command_error_handling([
            {"command": "long_running_test", "exit_code": 124, "stderr": "", "timed_out": True, "following_response": "Command timed out, retrying", "turn_index": 0},
        ])

        assert result["error_types"]["timeout"] == 1
        assert result["acknowledged_failures"] == 1

    def test_permission_error_scenario(self):
        """Simulate permission errors."""
        result = analyze_session_bash_command_error_handling([
            {"command": "./deploy.sh", "exit_code": 126, "stderr": "Permission denied", "following_response": "Permission denied, fixing permissions", "turn_index": 0},
        ])

        assert result["error_types"]["permission_denied"] == 1
        assert result["acknowledged_failures"] == 1
