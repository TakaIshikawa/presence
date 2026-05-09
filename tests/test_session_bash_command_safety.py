"""Tests for session Bash command safety analyzer."""

import pytest

from synthesis.session_bash_command_safety import analyze_session_bash_safety


class TestAnalyzeSessionBashSafety:
    """Test main analyzer function."""

    def test_empty_commands_returns_perfect_safety(self):
        """Verify empty command list returns perfect safety scores."""
        result = analyze_session_bash_safety([])

        assert result["total_commands"] == 0
        assert result["safe_commands"] == 0
        assert result["commands_with_violations"] == 0
        assert result["critical_violations"] == 0
        assert result["warning_violations"] == 0
        assert result["unquoted_path_count"] == 0
        assert result["dangerous_command_count"] == 0
        assert result["force_flag_count"] == 0
        assert result["prohibited_file_op_count"] == 0
        assert result["unnecessary_cd_count"] == 0
        assert result["safety_violation_rate"] == 0.0
        assert result["tool_preference_ratio"] == 1.0
        assert result["best_practice_adherence"] == 100.0
        assert result["overall_safety_score"] == 1.0
        assert result["violations_by_type"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_bash_safety(None)
        assert result["total_commands"] == 0
        assert result["overall_safety_score"] == 1.0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_bash_safety("not a list")

    def test_all_safe_commands(self):
        """Verify session with all safe Bash commands."""
        result = analyze_session_bash_safety([
            {
                "command": "git status",
                "turn_index": 1,
            },
            {
                "command": "pytest tests/",
                "turn_index": 2,
            },
            {
                "command": "npm install",
                "turn_index": 3,
            },
        ])

        assert result["total_commands"] == 3
        assert result["safe_commands"] == 3
        assert result["commands_with_violations"] == 0
        assert result["critical_violations"] == 0
        assert result["warning_violations"] == 0
        assert result["safety_violation_rate"] == 0.0
        assert result["best_practice_adherence"] == 100.0
        assert result["overall_safety_score"] == 1.0

    def test_dangerous_commands_detected(self):
        """Verify detection of dangerous commands."""
        result = analyze_session_bash_safety([
            {
                "command": "rm -rf /tmp/test",
                "uses_dangerous_command": True,
                "severity": "critical",
            },
            {
                "command": "git reset --hard HEAD",
                "uses_dangerous_command": True,
                "severity": "critical",
            },
        ])

        assert result["total_commands"] == 2
        assert result["dangerous_command_count"] == 2
        assert result["critical_violations"] == 2
        assert result["commands_with_violations"] == 2
        assert result["safe_commands"] == 0
        assert result["violations_by_type"]["dangerous_command"] == 2
        # Critical violations penalize safety
        # 2 critical = 0.4 penalty → critical_comp = 0.6
        # safety = 0.5 * 0.6 + 0.25 * 1.0 + 0.25 * 1.0 = 0.8
        assert result["overall_safety_score"] == 0.8

    def test_missing_path_quoting(self):
        """Verify detection of missing quotes for paths with spaces."""
        result = analyze_session_bash_safety([
            {
                "command": "cd /path with spaces/file.txt",
                "has_unquoted_paths": True,
                "severity": "warning",
            },
            {
                "command": 'cd "/path with spaces/file.txt"',
                "has_unquoted_paths": False,
            },
        ])

        assert result["total_commands"] == 2
        assert result["unquoted_path_count"] == 1
        assert result["warning_violations"] == 1
        assert result["safe_commands"] == 1
        assert result["violations_by_type"]["unquoted_paths"] == 1

    def test_force_flags_without_consent(self):
        """Verify detection of force flags."""
        result = analyze_session_bash_safety([
            {
                "command": "git push --force origin main",
                "uses_force_flag": True,
                "severity": "critical",
            },
            {
                "command": "npm install --force",
                "uses_force_flag": True,
                "severity": "warning",
            },
        ])

        assert result["total_commands"] == 2
        assert result["force_flag_count"] == 2
        assert result["critical_violations"] == 1
        assert result["warning_violations"] == 1
        assert result["violations_by_type"]["force_flag"] == 2

    def test_prohibited_file_operations(self):
        """Verify detection of bash file ops that should use specialized tools."""
        result = analyze_session_bash_safety([
            {
                "command": "grep 'pattern' file.txt",
                "uses_prohibited_file_op": "grep",
                "severity": "warning",
            },
            {
                "command": "cat README.md",
                "uses_prohibited_file_op": "cat",
                "severity": "warning",
            },
            {
                "command": "find . -name '*.py'",
                "uses_prohibited_file_op": "find",
                "severity": "warning",
            },
        ])

        assert result["total_commands"] == 3
        assert result["prohibited_file_op_count"] == 3
        assert result["warning_violations"] == 3
        assert result["violations_by_type"]["prohibited_file_op"] == 3
        # Tool preference ratio should be 0 (all commands violate)
        assert result["tool_preference_ratio"] == 0.0

    def test_unnecessary_cd_usage(self):
        """Verify detection of unnecessary cd instead of absolute paths."""
        result = analyze_session_bash_safety([
            {
                "command": "cd /foo/bar && pytest tests",
                "uses_unnecessary_cd": True,
                "severity": "warning",
            },
            {
                "command": "pytest /foo/bar/tests",
                "uses_unnecessary_cd": False,
            },
        ])

        assert result["total_commands"] == 2
        assert result["unnecessary_cd_count"] == 1
        assert result["warning_violations"] == 1
        assert result["safe_commands"] == 1
        assert result["violations_by_type"]["unnecessary_cd"] == 1

    def test_multiple_violation_types(self):
        """Verify command with multiple violation types."""
        result = analyze_session_bash_safety([
            {
                "command": "cd /bad path && rm -rf test",
                "has_unquoted_paths": True,
                "uses_dangerous_command": True,
                "uses_unnecessary_cd": True,
                "severity": "critical",
            },
        ])

        assert result["total_commands"] == 1
        assert result["unquoted_path_count"] == 1
        assert result["dangerous_command_count"] == 1
        assert result["unnecessary_cd_count"] == 1
        assert result["commands_with_violations"] == 1
        assert result["critical_violations"] == 1
        # Multiple violation types tracked
        assert "unquoted_paths" in result["violations_by_type"]
        assert "dangerous_command" in result["violations_by_type"]
        assert "unnecessary_cd" in result["violations_by_type"]

    def test_tool_preference_ratio_calculation(self):
        """Verify tool preference ratio calculation."""
        result = analyze_session_bash_safety([
            # 3 safe commands using proper tools
            {"command": "git status"},
            {"command": "pytest tests/"},
            {"command": "npm build"},
            # 1 command using prohibited file op
            {"command": "cat file.txt", "uses_prohibited_file_op": "cat"},
        ])

        assert result["total_commands"] == 4
        assert result["prohibited_file_op_count"] == 1
        # 3 out of 4 commands use proper tools = 0.75
        assert result["tool_preference_ratio"] == 0.75

    def test_safety_score_with_critical_violations(self):
        """Verify safety score calculation with critical violations."""
        result = analyze_session_bash_safety([
            {
                "command": "rm -rf /",
                "uses_dangerous_command": True,
                "severity": "critical",
            },
        ])

        # 1 critical violation = 0.2 penalty
        # critical_component = 1.0 - 0.2 = 0.8
        # warning_component = 1.0
        # tool_preference = 1.0 (no prohibited ops)
        # safety = 0.5 * 0.8 + 0.25 * 1.0 + 0.25 * 1.0 = 0.9
        assert result["overall_safety_score"] == 0.9

    def test_safety_score_with_warning_violations(self):
        """Verify safety score calculation with warning violations."""
        result = analyze_session_bash_safety([
            {
                "command": "cat file.txt",
                "uses_prohibited_file_op": "cat",
                "severity": "warning",
            },
        ])

        # critical_component = 1.0
        # 1 warning violation = 0.1 penalty → warning_component = 0.9
        # tool_preference = 0.0 (1 prohibited op out of 1 command)
        # safety = 0.5 * 1.0 + 0.25 * 0.9 + 0.25 * 0.0 = 0.725
        assert result["overall_safety_score"] == 0.72

    def test_safety_score_multiple_critical_violations(self):
        """Verify safety score with multiple critical violations."""
        result = analyze_session_bash_safety([
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
        ])

        # 3 critical violations = 0.6 penalty → critical_component = 0.4
        # warning_component = 1.0
        # tool_preference = 1.0
        # safety = 0.5 * 0.4 + 0.25 * 1.0 + 0.25 * 1.0 = 0.7
        assert result["overall_safety_score"] == 0.7

    def test_best_practice_adherence(self):
        """Verify best practice adherence percentage calculation."""
        result = analyze_session_bash_safety([
            {"command": "git status"},  # Safe
            {"command": "pytest tests/"},  # Safe
            {"command": "cat file.txt", "uses_prohibited_file_op": "cat"},  # Violation
            {"command": "grep 'x' f", "uses_prohibited_file_op": "grep"},  # Violation
        ])

        # 2 safe out of 4 = 50%
        assert result["best_practice_adherence"] == 50.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_bash_safety([
            "not a dict",
            {
                "command": "git status",
            },
            None,
        ])

        assert result["total_commands"] == 1

    def test_empty_string_prohibited_op_not_counted(self):
        """Verify empty string prohibited ops are not counted."""
        result = analyze_session_bash_safety([
            {"command": "git status", "uses_prohibited_file_op": ""},
            {"command": "pytest", "uses_prohibited_file_op": None},
        ])

        assert result["prohibited_file_op_count"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_bash_safety([
            {
                "command": "some command",
                # All violation flags missing
            },
        ])

        assert result["total_commands"] == 1
        assert result["safe_commands"] == 1
        assert result["commands_with_violations"] == 0

    def test_comprehensive_session_all_violation_types(self):
        """Verify comprehensive session with all violation types."""
        result = analyze_session_bash_safety([
            {
                "command": "git status",
            },
            {
                "command": "cd /bad path",
                "has_unquoted_paths": True,
                "severity": "warning",
            },
            {
                "command": "rm -rf /tmp/test",
                "uses_dangerous_command": True,
                "severity": "critical",
            },
            {
                "command": "git push --force",
                "uses_force_flag": True,
                "severity": "critical",
            },
            {
                "command": "cat file.txt",
                "uses_prohibited_file_op": "cat",
                "severity": "warning",
            },
            {
                "command": "cd /foo && ls",
                "uses_unnecessary_cd": True,
                "severity": "warning",
            },
        ])

        assert result["total_commands"] == 6
        assert result["safe_commands"] == 1
        assert result["commands_with_violations"] == 5
        assert result["critical_violations"] == 2
        assert result["warning_violations"] == 3
        assert result["unquoted_path_count"] == 1
        assert result["dangerous_command_count"] == 1
        assert result["force_flag_count"] == 1
        assert result["prohibited_file_op_count"] == 1
        assert result["unnecessary_cd_count"] == 1
        assert result["violations_by_type"]["unquoted_paths"] == 1
        assert result["violations_by_type"]["dangerous_command"] == 1
        assert result["violations_by_type"]["force_flag"] == 1
        assert result["violations_by_type"]["prohibited_file_op"] == 1
        assert result["violations_by_type"]["unnecessary_cd"] == 1

    def test_violation_rate_calculation(self):
        """Verify safety violation rate calculation."""
        result = analyze_session_bash_safety([
            {"command": "safe1"},
            {"command": "safe2"},
            {"command": "unsafe", "uses_dangerous_command": True},
        ])

        # 1 violation out of 3 = 33.33%
        assert result["safety_violation_rate"] == 33.33

    def test_severity_tracking(self):
        """Verify tracking of violation severity levels."""
        result = analyze_session_bash_safety([
            {"severity": "critical"},
            {"severity": "critical"},
            {"severity": "warning"},
            {"severity": "warning"},
            {"severity": "warning"},
        ])

        assert result["critical_violations"] == 2
        assert result["warning_violations"] == 3

    def test_safety_score_perfect(self):
        """Verify perfect safety score with no violations."""
        result = analyze_session_bash_safety([
            {"command": "git status"},
            {"command": "pytest tests/"},
        ])

        assert result["overall_safety_score"] == 1.0

    def test_safety_score_zero_tools_only_violations(self):
        """Verify low safety score with only tool preference violations."""
        result = analyze_session_bash_safety([
            {"uses_prohibited_file_op": "cat", "severity": "warning"},
            {"uses_prohibited_file_op": "grep", "severity": "warning"},
            {"uses_prohibited_file_op": "find", "severity": "warning"},
        ])

        # critical_component = 1.0
        # 3 warnings = 0.3 penalty → warning_component = 0.7
        # tool_preference = 0.0 (all commands are prohibited ops)
        # safety = 0.5 * 1.0 + 0.25 * 0.7 + 0.25 * 0.0 = 0.675
        assert result["overall_safety_score"] == 0.68

    def test_mixed_safe_and_unsafe(self):
        """Verify session with mix of safe and unsafe commands."""
        result = analyze_session_bash_safety([
            {"command": "git status"},
            {"command": "pytest tests/"},
            {"command": "npm build"},
            {"command": "rm -rf tmp", "uses_dangerous_command": True, "severity": "critical"},
            {"command": "cat file", "uses_prohibited_file_op": "cat", "severity": "warning"},
        ])

        assert result["total_commands"] == 5
        assert result["safe_commands"] == 3
        assert result["commands_with_violations"] == 2
        assert result["best_practice_adherence"] == 60.0
        # 4 out of 5 use proper tools (not prohibited ops)
        assert result["tool_preference_ratio"] == 0.8

    def test_no_severity_provided(self):
        """Verify commands without severity field don't count as violations."""
        result = analyze_session_bash_safety([
            {
                "command": "git status",
                # No severity field
            },
        ])

        assert result["critical_violations"] == 0
        assert result["warning_violations"] == 0

    def test_maximum_safety_penalty(self):
        """Verify safety score doesn't go below 0."""
        result = analyze_session_bash_safety([
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
            {"uses_dangerous_command": True, "severity": "critical"},
        ])

        # 6 critical violations = 1.2 penalty (capped at 1.0)
        # critical_component = 0.0
        # warning_component = 1.0
        # tool_preference = 1.0
        # safety = 0.5 * 0.0 + 0.25 * 1.0 + 0.25 * 1.0 = 0.5
        assert result["overall_safety_score"] == 0.5
