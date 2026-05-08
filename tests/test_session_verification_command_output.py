"""Tests for session verification command output analyzer."""

import pytest

from synthesis.session_verification_command_output import (
    analyze_session_verification_command_output,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_verification_command_output([])

    assert report["total_verifications"] == 0
    assert report["stderr_on_success_count"] == 0
    assert report["no_output_on_failure_count"] == 0
    assert report["truncated_output_count"] == 0
    assert report["risk_percentage"] == 0.0
    assert report["examples"] == []


def test_successful_verification_with_clean_output_does_not_flag():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 0,
            "stdout": "test_foo.py::test_alpha PASSED",
            "stderr": "",
        }
    ])

    assert report["total_verifications"] == 1
    assert report["stderr_on_success_count"] == 0
    assert report["no_output_on_failure_count"] == 0
    assert report["truncated_output_count"] == 0
    assert report["examples"] == []


def test_successful_verification_with_stderr_flags_stderr_on_success():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 0,
            "stdout": "test_foo.py::test_alpha PASSED",
            "stderr": "DeprecationWarning: old API",
        }
    ])

    assert report["stderr_on_success_count"] == 1
    assert report["risk_percentage"] == 100.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "stderr_on_success"
    assert report["examples"][0]["turn_index"] == 0


def test_failed_verification_with_output_does_not_flag():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 1,
            "stdout": "test_foo.py::test_alpha FAILED",
            "stderr": "AssertionError: expected 42",
        }
    ])

    assert report["total_verifications"] == 1
    assert report["no_output_on_failure_count"] == 0
    assert report["examples"] == []


def test_failed_verification_with_no_output_flags_no_output_on_failure():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 1,
            "stdout": "",
            "stderr": "",
        }
    ])

    assert report["no_output_on_failure_count"] == 1
    assert report["risk_percentage"] == 100.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "no_output_on_failure"


def test_truncated_output_flags_truncated_output():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/",
            "exit_code": 0,
            "stdout": "test output...",
            "stderr": "",
            "truncated": True,
        }
    ])

    assert report["truncated_output_count"] == 1
    assert report["risk_percentage"] == 100.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["reason"] == "truncated_output"


def test_non_verification_commands_are_ignored():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "git status",
            "exit_code": 0,
            "stdout": "nothing to commit",
            "stderr": "",
        },
        {
            "turn_index": 1,
            "command": "npm install",
            "exit_code": 0,
            "stdout": "added 100 packages",
            "stderr": "deprecated warning",
        }
    ])

    assert report["total_verifications"] == 0
    assert report["stderr_on_success_count"] == 0


def test_multiple_verification_commands_are_counted():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "",
        },
        {
            "turn_index": 1,
            "command": "npm test",
            "exit_code": 0,
            "stdout": "all tests passed",
            "stderr": "",
        },
        {
            "turn_index": 2,
            "command": "tsc --noEmit",
            "exit_code": 0,
            "stdout": "",
            "stderr": "",
        }
    ])

    assert report["total_verifications"] == 3


def test_verification_command_case_insensitive():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "PYTEST tests/test_foo.py",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "warning",
        }
    ])

    assert report["total_verifications"] == 1
    assert report["stderr_on_success_count"] == 1


def test_examples_capped_at_five():
    records = []
    for i in range(10):
        records.append({
            "turn_index": i,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "warning",
        })

    report = analyze_session_verification_command_output(records)

    assert report["stderr_on_success_count"] == 10
    assert len(report["examples"]) == 5


def test_mixed_issues_in_single_session():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "DeprecationWarning",
        },
        {
            "turn_index": 1,
            "command": "jest",
            "exit_code": 1,
            "stdout": "",
            "stderr": "",
        },
        {
            "turn_index": 2,
            "command": "tsc",
            "exit_code": 0,
            "stdout": "...",
            "stderr": "",
            "truncated": True,
        }
    ])

    assert report["total_verifications"] == 3
    assert report["stderr_on_success_count"] == 1
    assert report["no_output_on_failure_count"] == 1
    assert report["truncated_output_count"] == 1
    assert report["risk_percentage"] == 100.0
    assert len(report["examples"]) == 3


def test_risk_percentage_calculation():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "",
        },
        {
            "turn_index": 1,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "warning",
        }
    ])

    assert report["total_verifications"] == 2
    assert report["stderr_on_success_count"] == 1
    assert report["risk_percentage"] == 50.0


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_verification_command_output({"turn_index": 0, "command": "pytest", "exit_code": 0})


def test_missing_turn_index_raises_error():
    with pytest.raises(ValueError, match="each record must have a turn_index"):
        analyze_session_verification_command_output([
            {"command": "pytest", "exit_code": 0}
        ])


def test_missing_command_raises_error():
    with pytest.raises(ValueError, match="each record must have a command"):
        analyze_session_verification_command_output([
            {"turn_index": 0, "exit_code": 0}
        ])


def test_missing_exit_code_raises_error():
    with pytest.raises(ValueError, match="each record must have an exit_code"):
        analyze_session_verification_command_output([
            {"turn_index": 0, "command": "pytest"}
        ])


def test_empty_command_raises_error():
    with pytest.raises(ValueError, match="command must not be empty"):
        analyze_session_verification_command_output([
            {"turn_index": 0, "command": "  ", "exit_code": 0}
        ])


def test_boolean_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be an integer"):
        analyze_session_verification_command_output([
            {"turn_index": True, "command": "pytest", "exit_code": 0}
        ])


def test_negative_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be non-negative"):
        analyze_session_verification_command_output([
            {"turn_index": -1, "command": "pytest", "exit_code": 0}
        ])


def test_non_integer_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_verification_command_output([
            {"turn_index": 0, "command": "pytest", "exit_code": "0"}
        ])


def test_unordered_records_raise_error():
    with pytest.raises(ValueError, match="records must be ordered by turn_index"):
        analyze_session_verification_command_output([
            {"turn_index": 1, "command": "pytest", "exit_code": 0},
            {"turn_index": 0, "command": "jest", "exit_code": 0},
        ])


def test_non_dict_record_raises_error():
    with pytest.raises(ValueError, match="each record must be a dict"):
        analyze_session_verification_command_output([
            "not a dict"
        ])


def test_non_string_command_raises_error():
    with pytest.raises(ValueError, match="command must be a string"):
        analyze_session_verification_command_output([
            {"turn_index": 0, "command": 123, "exit_code": 0}
        ])


def test_whitespace_only_stderr_is_ignored():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "   \n  \t  ",
        }
    ])

    assert report["stderr_on_success_count"] == 0


def test_whitespace_only_output_on_failure_flags():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 1,
            "stdout": "  \n  ",
            "stderr": "\t   ",
        }
    ])

    assert report["no_output_on_failure_count"] == 1


def test_various_verification_commands_recognized():
    commands = [
        "pytest tests/",
        "jest --coverage",
        "vitest run",
        "npm test",
        "yarn test",
        "pnpm test",
        "go test ./...",
        "cargo test",
        "mvn test",
        "gradle test",
        "tsc --noEmit",
        "mypy src/",
        "pylint src/",
        "eslint .",
        "flake8 .",
        "black --check .",
        "ruff check .",
        "cargo clippy",
    ]

    records = []
    for i, cmd in enumerate(commands):
        records.append({
            "turn_index": i,
            "command": cmd,
            "exit_code": 0,
            "stdout": "OK",
            "stderr": "",
        })

    report = analyze_session_verification_command_output(records)

    assert report["total_verifications"] == len(commands)


def test_missing_stdout_and_stderr_fields_default_to_empty():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
        }
    ])

    assert report["total_verifications"] == 1
    assert report["stderr_on_success_count"] == 0


def test_missing_truncated_field_defaults_to_false():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "",
        }
    ])

    assert report["truncated_output_count"] == 0


def test_example_includes_all_required_fields():
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": "DeprecationWarning",
        }
    ])

    example = report["examples"][0]
    assert "turn_index" in example
    assert "command" in example
    assert "reason" in example
    assert "details" in example
    assert example["turn_index"] == 0
    assert example["command"] == "pytest tests/test_foo.py"


def test_stderr_details_truncated_at_100_chars():
    long_stderr = "x" * 200
    report = analyze_session_verification_command_output([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 0,
            "stdout": "PASSED",
            "stderr": long_stderr,
        }
    ])

    example = report["examples"][0]
    assert len(example["details"]) <= 120  # "stderr present: " + 100 chars
