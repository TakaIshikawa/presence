"""Tests for session Bash error handling analyzer."""

import pytest

from synthesis.session_bash_error_handling import (
    BashCommand,
    BashErrorHandlingMetrics,
    Finding,
    analyze_session_bash_error_handling,
)


class TestAnalyzeSessionBashErrorHandling:
    """Test main analyzer function."""

    def test_empty_commands_returns_zero_metrics(self):
        """Verify empty commands returns zero metrics."""
        metrics, findings = analyze_session_bash_error_handling([])

        assert metrics.total_commands == 0
        assert metrics.failed_commands == 0
        assert metrics.and_operator_usage_count == 0
        assert metrics.and_operator_usage_rate == 0.0
        assert metrics.semicolon_usage_count == 0
        assert metrics.error_inspection_count == 0
        assert metrics.error_inspection_rate == 0.0
        assert metrics.retry_after_failure_count == 0
        assert metrics.retry_with_fix_count == 0
        assert metrics.retry_success_rate == 0.0
        assert metrics.destructive_commands == 0
        assert metrics.destructive_on_protected_branch == 0
        assert metrics.unquoted_space_paths == 0
        assert metrics.findings_count == 0
        assert len(findings) == 0

    def test_single_successful_command(self):
        """Verify single successful command."""
        commands = [
            BashCommand(
                turn_index=1,
                command="ls -la",
                exit_code=0,
                stderr="",
                stdout="file1.txt\nfile2.txt",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="I can see two files.",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.total_commands == 1
        assert metrics.failed_commands == 0
        assert len(findings) == 0

    def test_and_operator_usage_rate(self):
        """Verify && operator usage rate calculation."""
        commands = [
            BashCommand(
                turn_index=1,
                command="git add . && git commit -m 'msg'",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=True,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
            BashCommand(
                turn_index=2,
                command="npm install",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # 1 out of 2 use && = 50%
        assert metrics.and_operator_usage_count == 1
        assert metrics.and_operator_usage_rate == 50.0

    def test_semicolon_usage_warning(self):
        """Verify warning for semicolon usage in git commands."""
        commands = [
            BashCommand(
                turn_index=3,
                command="git add . ; git commit -m 'msg'",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=True,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.semicolon_usage_count == 1
        # Should have warning about using ; instead of &&
        chaining_findings = [f for f in findings if f.category == "command_chaining"]
        assert len(chaining_findings) >= 1
        assert chaining_findings[0].severity == "warning"

    def test_error_inspection_rate(self):
        """Verify error inspection rate calculation."""
        commands = [
            # Failed command with error inspection
            BashCommand(
                turn_index=1,
                command="npm test",
                exit_code=1,
                stderr="Test failed: authentication.test.js",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="I see the test failed with an authentication error.",
                was_retried=False,
                retry_had_fix=False,
            ),
            # Failed command without inspection
            BashCommand(
                turn_index=2,
                command="npm run build",
                exit_code=1,
                stderr="Build failed: syntax error",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="Let me try something else.",
                was_retried=False,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # 1 out of 2 failures had error inspection = 50%
        assert metrics.failed_commands == 2
        assert metrics.error_inspection_count == 1
        assert metrics.error_inspection_rate == 50.0

    def test_missing_error_inspection_critical(self):
        """Verify critical finding for missing error inspection."""
        commands = [
            BashCommand(
                turn_index=5,
                command="pytest tests/",
                exit_code=1,
                stderr="FAILED tests/test_auth.py::test_login",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="Moving on to the next task.",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # Should have critical finding
        inspection_findings = [f for f in findings if f.category == "error_inspection"]
        assert len(inspection_findings) >= 1
        assert inspection_findings[0].severity == "critical"
        assert "not acknowledged" in inspection_findings[0].message.lower()
        assert inspection_findings[0].turn_index == 5

    def test_retry_without_fix_warning(self):
        """Verify warning for retry without modification."""
        commands = [
            BashCommand(
                turn_index=7,
                command="npm install package",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=True,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        retry_findings = [f for f in findings if f.category == "retry_discipline"]
        assert len(retry_findings) >= 1
        assert retry_findings[0].severity == "warning"
        assert "without modification" in retry_findings[0].message

    def test_retry_with_fix_success_rate(self):
        """Verify retry success rate calculation."""
        commands = [
            BashCommand(
                turn_index=1,
                command="npm test",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=True,
                retry_had_fix=True,
            ),
            BashCommand(
                turn_index=2,
                command="npm build",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=True,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # 1 out of 2 retries had fix = 50%
        assert metrics.retry_after_failure_count == 2
        assert metrics.retry_with_fix_count == 1
        assert metrics.retry_success_rate == 50.0

    def test_force_push_to_main_critical(self):
        """Verify critical finding for force push to main."""
        commands = [
            BashCommand(
                turn_index=10,
                command="git push --force origin main",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="force_push",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) >= 1
        assert destructive_findings[0].severity == "critical"
        assert "main/master" in destructive_findings[0].message.lower()
        assert metrics.destructive_on_protected_branch == 1

    def test_force_push_to_feature_branch_warning(self):
        """Verify warning for force push to feature branch."""
        commands = [
            BashCommand(
                turn_index=8,
                command="git push --force origin feature/auth",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="force_push",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) >= 1
        assert destructive_findings[0].severity == "warning"
        assert metrics.destructive_on_protected_branch == 0

    def test_rm_rf_critical(self):
        """Verify critical finding for rm -rf."""
        commands = [
            BashCommand(
                turn_index=12,
                command="rm -rf /tmp/build",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="rm_rf",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) >= 1
        assert destructive_findings[0].severity == "critical"
        assert "rm -rf" in destructive_findings[0].message

    def test_reset_hard_warning(self):
        """Verify warning for git reset --hard."""
        commands = [
            BashCommand(
                turn_index=6,
                command="git reset --hard HEAD~1",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="reset_hard",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) >= 1
        assert destructive_findings[0].severity == "warning"
        assert "reset --hard" in destructive_findings[0].message.lower()

    def test_unquoted_space_paths_warning(self):
        """Verify warning for unquoted paths with spaces."""
        commands = [
            BashCommand(
                turn_index=4,
                command="cd /Users/name/My Documents",
                exit_code=1,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=True,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.unquoted_space_paths == 1
        quoting_findings = [f for f in findings if f.category == "quoting_discipline"]
        assert len(quoting_findings) >= 1
        assert quoting_findings[0].severity == "warning"
        assert "double quotes" in quoting_findings[0].message.lower()

    def test_findings_severity_counts(self):
        """Verify findings are counted by severity correctly."""
        commands = [
            # Critical: missing error inspection
            BashCommand(
                turn_index=1,
                command="npm test",
                exit_code=1,
                stderr="Tests failed",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="Let me continue.",
                was_retried=False,
                retry_had_fix=False,
            ),
            # Warning: semicolon usage
            BashCommand(
                turn_index=2,
                command="git add . ; git commit -m 'msg'",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=True,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.findings_count == len(findings)
        assert metrics.critical_findings >= 1
        assert metrics.warning_findings >= 1

    def test_finding_contains_example(self):
        """Verify findings include concrete command examples."""
        commands = [
            BashCommand(
                turn_index=3,
                command="rm -rf /dangerous/path",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="rm_rf",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # All findings should have non-empty example
        for finding in findings:
            assert finding.example
            assert "rm -rf" in finding.example

    def test_efficient_bash_usage_no_findings(self):
        """Verify efficient Bash usage produces no findings."""
        commands = [
            BashCommand(
                turn_index=1,
                command="git add . && git commit -m 'Update feature'",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=True,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
            BashCommand(
                turn_index=2,
                command='cd "/Users/name/My Project"',
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert len(findings) == 0
        assert metrics.findings_count == 0

    def test_multiple_destructive_types(self):
        """Verify different destructive command types are tracked."""
        commands = [
            BashCommand(
                turn_index=1,
                command="git push --force origin feature",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="force_push",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
            BashCommand(
                turn_index=2,
                command="rm -rf node_modules",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="rm_rf",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
            BashCommand(
                turn_index=3,
                command="git reset --hard origin/main",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="reset_hard",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            ),
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.destructive_commands == 3
        # Should have findings for each
        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) == 3


class TestValidation:
    """Test input validation."""

    def test_invalid_commands_type(self):
        """Verify non-sequence commands raises error."""
        with pytest.raises(ValueError, match="must be a list or tuple"):
            analyze_session_bash_error_handling("not a list")

    def test_invalid_command_instance(self):
        """Verify non-BashCommand instance raises error."""
        with pytest.raises(ValueError, match="BashCommand instance"):
            analyze_session_bash_error_handling([{"command": "ls"}])

    def test_invalid_turn_index_type(self):
        """Verify invalid turn_index type raises error."""
        with pytest.raises(ValueError, match="turn_index must be an integer"):
            analyze_session_bash_error_handling([
                BashCommand(
                    turn_index="not_int",
                    command="ls",
                    exit_code=0,
                    stderr="",
                    stdout="",
                    uses_and_operator=False,
                    uses_semicolon=False,
                    is_destructive=False,
                    destructive_type="",
                    has_unquoted_spaces=False,
                    following_response="",
                    was_retried=False,
                    retry_had_fix=False,
                )
            ])

    def test_invalid_turn_index_boolean(self):
        """Verify boolean turn_index raises error."""
        with pytest.raises(ValueError, match="turn_index must be an integer"):
            analyze_session_bash_error_handling([
                BashCommand(
                    turn_index=True,
                    command="ls",
                    exit_code=0,
                    stderr="",
                    stdout="",
                    uses_and_operator=False,
                    uses_semicolon=False,
                    is_destructive=False,
                    destructive_type="",
                    has_unquoted_spaces=False,
                    following_response="",
                    was_retried=False,
                    retry_had_fix=False,
                )
            ])

    def test_negative_turn_index(self):
        """Verify negative turn_index raises error."""
        with pytest.raises(ValueError, match="non-negative"):
            analyze_session_bash_error_handling([
                BashCommand(
                    turn_index=-1,
                    command="ls",
                    exit_code=0,
                    stderr="",
                    stdout="",
                    uses_and_operator=False,
                    uses_semicolon=False,
                    is_destructive=False,
                    destructive_type="",
                    has_unquoted_spaces=False,
                    following_response="",
                    was_retried=False,
                    retry_had_fix=False,
                )
            ])

    def test_invalid_command_type(self):
        """Verify non-string command raises error."""
        with pytest.raises(ValueError, match="command must be a string"):
            analyze_session_bash_error_handling([
                BashCommand(
                    turn_index=0,
                    command=123,
                    exit_code=0,
                    stderr="",
                    stdout="",
                    uses_and_operator=False,
                    uses_semicolon=False,
                    is_destructive=False,
                    destructive_type="",
                    has_unquoted_spaces=False,
                    following_response="",
                    was_retried=False,
                    retry_had_fix=False,
                )
            ])


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_command_with_empty_stderr(self):
        """Verify failed command with empty stderr."""
        commands = [
            BashCommand(
                turn_index=1,
                command="exit 1",
                exit_code=1,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="Command failed.",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # Should not trigger error inspection finding (no stderr)
        assert metrics.failed_commands == 1
        inspection_findings = [f for f in findings if f.category == "error_inspection"]
        assert len(inspection_findings) == 0

    def test_zero_exit_code_no_failure(self):
        """Verify exit code 0 not counted as failure."""
        commands = [
            BashCommand(
                turn_index=1,
                command="echo test",
                exit_code=0,
                stderr="",
                stdout="test",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        assert metrics.failed_commands == 0

    def test_empty_command_string(self):
        """Verify empty command string is handled."""
        commands = [
            BashCommand(
                turn_index=1,
                command="",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # Should handle gracefully
        assert metrics.total_commands == 1

    def test_master_branch_force_push(self):
        """Verify force push to master is also detected."""
        commands = [
            BashCommand(
                turn_index=5,
                command="git push --force origin master",
                exit_code=0,
                stderr="",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=True,
                destructive_type="force_push",
                has_unquoted_spaces=False,
                following_response="",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        destructive_findings = [f for f in findings if f.category == "destructive_command"]
        assert len(destructive_findings) >= 1
        assert destructive_findings[0].severity == "critical"
        assert metrics.destructive_on_protected_branch == 1

    def test_error_mentioned_in_response(self):
        """Verify error keyword detection in following response."""
        commands = [
            BashCommand(
                turn_index=2,
                command="npm test",
                exit_code=1,
                stderr="Test suite failed",
                stdout="",
                uses_and_operator=False,
                uses_semicolon=False,
                is_destructive=False,
                destructive_type="",
                has_unquoted_spaces=False,
                following_response="The test suite failed due to authentication issues.",
                was_retried=False,
                retry_had_fix=False,
            )
        ]

        metrics, findings = analyze_session_bash_error_handling(commands)

        # Should count as inspected
        assert metrics.error_inspection_count == 1
        # Should not have critical finding
        inspection_findings = [f for f in findings if f.category == "error_inspection"]
        assert len(inspection_findings) == 0
