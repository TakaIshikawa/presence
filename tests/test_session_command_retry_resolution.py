"""Tests for session command retry resolution analysis."""

import pytest

from synthesis.session_command_retry_resolution import analyze_session_command_retry_resolution


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_command_retry_resolution([])

    assert report["failed_command_count"] == 0
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 0.0
    assert report["examples"] == []


def test_failed_command_followed_by_successful_retry_counts_as_resolved():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest tests/", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/", "exit_code": 0},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 100.0


def test_failed_command_followed_only_by_additional_failures_counts_as_unresolved():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest tests/", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/", "exit_code": 1},
        {"turn_index": 2, "command": "pytest tests/", "exit_code": 1},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 1
    assert len(report["examples"]) == 1
    assert report["examples"][0]["first_failure_turn"] == 0
    assert report["examples"][0]["retry_turns"] == [1, 2]


def test_successful_commands_with_no_preceding_failure_do_not_affect_metrics():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest tests/", "exit_code": 0},
        {"turn_index": 1, "command": "npm test", "exit_code": 0},
    ])

    assert report["failed_command_count"] == 0
    assert report["retried_failure_count"] == 0


def test_command_normalization_handles_case():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "PYTEST tests/", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1


def test_command_normalization_handles_repeated_whitespace():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest   tests/", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1


def test_normalized_command_field_is_used_when_provided():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest tests/test_foo.py", "normalized_command": "pytest", "exit_code": 1},
        {"turn_index": 1, "command": "pytest tests/test_bar.py", "normalized_command": "pytest", "exit_code": 0},
    ])

    assert report["resolved_retry_count"] == 1


def test_empty_command_raises_value_error():
    with pytest.raises(ValueError, match="empty command"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "", "exit_code": 1}
        ])


def test_whitespace_only_command_raises_value_error():
    with pytest.raises(ValueError, match="empty command"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "   ", "exit_code": 1}
        ])


def test_boolean_turn_index_raises_value_error():
    with pytest.raises(ValueError, match="invalid turn_index"):
        analyze_session_command_retry_resolution([
            {"turn_index": True, "command": "pytest", "exit_code": 1}
        ])


def test_negative_turn_index_raises_value_error():
    with pytest.raises(ValueError, match="negative turn_index"):
        analyze_session_command_retry_resolution([
            {"turn_index": -1, "command": "pytest", "exit_code": 1}
        ])


def test_non_integer_exit_code_raises_value_error():
    with pytest.raises(ValueError, match="invalid exit_code"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "pytest", "exit_code": "1"}
        ])


def test_boolean_exit_code_raises_value_error():
    with pytest.raises(ValueError, match="invalid exit_code"):
        analyze_session_command_retry_resolution([
            {"turn_index": 0, "command": "pytest", "exit_code": False}
        ])


def test_unordered_turn_indexes_raise_value_error():
    with pytest.raises(ValueError, match="unordered turn_index"):
        analyze_session_command_retry_resolution([
            {"turn_index": 5, "command": "pytest", "exit_code": 1},
            {"turn_index": 3, "command": "pytest", "exit_code": 0},
        ])


def test_non_mapping_record_raises_value_error():
    with pytest.raises(ValueError, match="not a dictionary"):
        analyze_session_command_retry_resolution(["not a dict"])


def test_non_list_input_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_command_retry_resolution({"turn_index": 0})


def test_examples_capped_at_five():
    records = []
    for i in range(7):
        records.append({"turn_index": i * 2, "command": f"pytest test{i}.py", "exit_code": 1})
        records.append({"turn_index": i * 2 + 1, "command": f"pytest test{i}.py", "exit_code": 1})

    report = analyze_session_command_retry_resolution(records)

    assert len(report["examples"]) == 5


def test_multiple_different_commands():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
        {"turn_index": 1, "command": "npm test", "exit_code": 1},
        {"turn_index": 2, "command": "pytest", "exit_code": 0},
        {"turn_index": 3, "command": "npm test", "exit_code": 1},
    ])

    assert report["failed_command_count"] == 2
    assert report["retried_failure_count"] == 2
    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 1


def test_resolution_rate_calculation():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "cmd1", "exit_code": 1},
        {"turn_index": 1, "command": "cmd1", "exit_code": 0},
        {"turn_index": 2, "command": "cmd2", "exit_code": 1},
        {"turn_index": 3, "command": "cmd2", "exit_code": 1},
        {"turn_index": 4, "command": "cmd3", "exit_code": 1},
        {"turn_index": 5, "command": "cmd3", "exit_code": 0},
    ])

    assert report["retried_failure_count"] == 3
    assert report["resolved_retry_count"] == 2
    assert report["resolution_rate"] == 66.67


def test_single_failure_without_retry():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
    ])

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0


def test_failure_then_success_then_failure_again():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
        {"turn_index": 1, "command": "pytest", "exit_code": 0},
        {"turn_index": 2, "command": "pytest", "exit_code": 1},
    ])

    # First failure is resolved by turn 1
    # Turn 2 creates a new failure that wasn't retried
    assert report["failed_command_count"] == 2
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 1


def test_retry_turns_are_recorded_correctly():
    report = analyze_session_command_retry_resolution([
        {"turn_index": 0, "command": "pytest", "exit_code": 1},
        {"turn_index": 2, "command": "pytest", "exit_code": 1},
        {"turn_index": 5, "command": "pytest", "exit_code": 1},
    ])

    assert report["examples"][0]["first_failure_turn"] == 0
    assert report["examples"][0]["retry_turns"] == [2, 5]
