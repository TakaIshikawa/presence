"""Tests for session command retry resolution analyzer."""

import pytest

from synthesis.session_command_retry_resolution import (
    analyze_session_command_retry_resolution,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_command_retry_resolution([])

    assert report["failed_command_count"] == 0
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 0.0
    assert report["examples"] == []


def test_successful_commands_do_not_affect_failure_metrics():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 0},
        {"turn_index": 1, "command": "npm test", "exit_code": 0},
    ])

    assert report["failed_command_count"] == 0
    assert report["retried_failure_count"] == 0
    assert report["examples"] == []


def test_failed_command_followed_by_successful_retry_counts_as_resolved():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest tests/test_foo.py", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/test_foo.py", "exit_code": 0},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 100.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["resolved"] is True
    assert report["examples"][0]["first_failure_turn"] == 0
    assert report["examples"][0]["retry_turns"] == []


def test_failed_command_with_only_additional_failures_counts_as_unresolved():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "npm build", "exit_code": 1},
        {"turn_index": 1, "command": "npm build", "exit_code": 1},
        {"turn_index": 2, "command": "npm build", "exit_code": 2},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 1
    assert report["resolution_rate"] == 0.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["resolved"] is False
    assert report["examples"][0]["first_failure_turn"] == 0
    assert report["examples"][0]["retry_turns"] == [1, 2]


def test_failed_command_with_multiple_failures_then_success_counts_as_resolved():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
        {"turn_index": 1, "command": "pytest", "exit_code": 1},
        {"turn_index": 2, "command": "pytest", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 100.0
    assert report["examples"][0]["resolved"] is True
    assert report["examples"][0]["retry_turns"] == [1]


def test_command_normalization_handles_case_differences():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "PYTEST tests/test_foo.py", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/test_foo.py", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1
    assert report["resolution_rate"] == 100.0


def test_command_normalization_handles_whitespace_differences():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest   tests/test_foo.py", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/test_foo.py", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1


def test_normalized_command_field_overrides_default_normalization():
    report = analyze_session_command_retry_resolution([
        {
            "turn_index": 0,
            "command": "pytest tests/test_foo.py::test_alpha",
            "exit_code": 1,
            "normalized_command": "pytest tests/test_foo.py",
        },
        {
            "turn_index": 1,
            "command": "pytest tests/test_foo.py::test_beta",
            "exit_code": 0,
            "normalized_command": "pytest tests/test_foo.py",
        },
    ])

    assert report["resolved_retry_count"] == 1


def test_successful_command_with_no_preceding_failure_does_not_affect_retry_metrics():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "echo hello", "exit_code": 0},
        {"turn_index": 1, "command": "pytest", "exit_code": 1},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0


def test_examples_are_capped_at_five():
    records = []
    for i in range(10):
        records.append({"turn_index": i * 2, "command": f"cmd{i}", "exit_code": 1})
        records.append({"turn_index": i * 2 + 1, "command": f"cmd{i}", "exit_code": 0})

    report = analyze_session_command_retry_resolution(records)

    assert report["retried_failure_count"] == 10
    assert len(report["examples"]) == 5


def test_examples_include_only_retried_commands():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "cmd1", "exit_code": 1},
        {"turn_index": 1, "command": "cmd2", "exit_code": 1},
        {"turn_index": 2, "command": "cmd2", "exit_code": 0},
    ])

    assert report["failed_command_count"] == 2
    assert report["retried_failure_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["command"] == "cmd2"


def test_mixed_resolved_and_unresolved_retries():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
        {"turn_index": 1, "command": "pytest", "exit_code": 0},
        {"turn_index": 2, "command": "npm build", "exit_code": 1},
        {"turn_index": 3, "command": "npm build", "exit_code": 1},
    ])

    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 1
    assert report["resolution_rate"] == 50.0
    assert len(report["examples"]) == 2


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_command_retry_resolution({"turn_index": 0, "command": "pytest", "exit_code": 1})


def test_missing_turn_index_raises_error():
    with pytest.raises(ValueError, match="each record must have a turn_index"):
        analyze_session_command_retry_resolution([
            {"command": "pytest", "exit_code": 1}
        ])


def test_missing_command_raises_error():
    with pytest.raises(ValueError, match="each record must have a command"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "exit_code": 1}
        ])


def test_missing_exit_code_raises_error():
    with pytest.raises(ValueError, match="each record must have an exit_code"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "pytest"}
        ])


def test_empty_command_raises_error():
    with pytest.raises(ValueError, match="command must not be empty"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "  ", "exit_code": 1}
        ])


def test_boolean_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be an integer"):
        analyze_session_command_retry_resolution([
            {"turn_index": True, "command": "pytest", "exit_code": 1}
        ])


def test_negative_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be non-negative"):
        analyze_session_command_retry_resolution([
            {"turn_index": -1, "command": "pytest", "exit_code": 1}
        ])


def test_non_integer_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "pytest", "exit_code": "1"}
        ])


def test_unordered_records_raise_error():
    with pytest.raises(ValueError, match="records must be ordered by turn_index"):
        analyze_session_command_retry_resolution([
            {"turn_index": 1, "command": "pytest", "exit_code": 1},
            {"turn_index": 0, "command": "npm test", "exit_code": 0},
        ])


def test_non_dict_record_raises_error():
    with pytest.raises(ValueError, match="each record must be a dict"):
        analyze_session_command_retry_resolution([
            "not a dict"
        ])


def test_non_string_command_raises_error():
    with pytest.raises(ValueError, match="command must be a string"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": 123, "exit_code": 1}
        ])
