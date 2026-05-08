"""Tests for session command retry resolution analysis."""

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


def test_failed_command_followed_by_success_counts_as_resolved():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/test_main.py",
                "exit_code": 1,
            },
            {
                "turn_index": 2,
                "command": "pytest tests/test_main.py",
                "exit_code": 0,
            },
        ]
    )

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 0
    assert report["resolution_rate"] == 100.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["first_failure_turn"] == 1
    assert report["examples"][0]["status"] == "resolved"


def test_failed_command_with_only_additional_failures_counts_as_unresolved():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/test_main.py",
                "exit_code": 1,
            },
            {
                "turn_index": 2,
                "command": "pytest tests/test_main.py",
                "exit_code": 2,
            },
            {
                "turn_index": 3,
                "command": "pytest tests/test_main.py",
                "exit_code": 1,
            },
        ]
    )

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 1
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 1
    assert report["resolution_rate"] == 0.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["first_failure_turn"] == 1
    assert report["examples"][0]["retry_turns"] == [2, 3]
    assert report["examples"][0]["status"] == "unresolved"


def test_successful_command_with_no_preceding_failure_does_not_affect_retry_metrics():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/test_main.py",
                "exit_code": 0,
            }
        ]
    )

    assert report["failed_command_count"] == 0
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 0


def test_command_normalization_handles_case_and_whitespace():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "PYTEST   tests/test_main.py",
                "exit_code": 1,
            },
            {
                "turn_index": 2,
                "command": "pytest tests/test_main.py",
                "exit_code": 0,
            },
        ]
    )

    assert report["failed_command_count"] == 1
    assert report["resolved_retry_count"] == 1


def test_custom_normalized_command_field():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/test_main.py --verbose",
                "exit_code": 1,
                "normalized_command": "pytest tests/test_main.py",
            },
            {
                "turn_index": 2,
                "command": "pytest tests/test_main.py",
                "exit_code": 0,
                "normalized_command": "pytest tests/test_main.py",
            },
        ]
    )

    assert report["failed_command_count"] == 1
    assert report["resolved_retry_count"] == 1


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list of command record dictionaries"):
        analyze_session_command_retry_resolution({"command": "pytest"})


def test_non_mapping_record_raises_error():
    with pytest.raises(ValueError, match="records must be a list of command record dictionaries"):
        analyze_session_command_retry_resolution(["not a dict"])


def test_empty_command_raises_error():
    with pytest.raises(ValueError, match="command must be a non-empty string"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": 1,
                    "command": "",
                    "exit_code": 0,
                }
            ]
        )


def test_boolean_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be an integer"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": True,
                    "command": "pytest",
                    "exit_code": 0,
                }
            ]
        )


def test_negative_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be non-negative"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": -1,
                    "command": "pytest",
                    "exit_code": 0,
                }
            ]
        )


def test_non_integer_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": "0",
                }
            ]
        )


def test_boolean_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": False,
                }
            ]
        )


def test_unordered_turns_raise_error():
    with pytest.raises(ValueError, match="records must be ordered by turn_index"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": 2,
                    "command": "pytest",
                    "exit_code": 0,
                },
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": 0,
                },
            ]
        )


def test_examples_capped_at_five():
    records = [
        {
            "turn_index": i * 2,
            "command": f"pytest test{i}.py",
            "exit_code": 1,
        }
        for i in range(7)
    ] + [
        {
            "turn_index": i * 2 + 1,
            "command": f"pytest test{i}.py",
            "exit_code": 1,
        }
        for i in range(7)
    ]

    # Sort by turn_index to ensure ordering
    records.sort(key=lambda r: r["turn_index"])

    report = analyze_session_command_retry_resolution(records)

    assert report["retried_failure_count"] == 7
    assert len(report["examples"]) == 5


def test_single_failure_not_counted_as_retry():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/test_main.py",
                "exit_code": 1,
            }
        ]
    )

    assert report["failed_command_count"] == 1
    assert report["retried_failure_count"] == 0
    assert report["resolved_retry_count"] == 0
    assert report["unresolved_retry_count"] == 0
    assert report["examples"] == []


def test_multiple_different_failed_commands():
    report = analyze_session_command_retry_resolution(
        [
            {
                "turn_index": 1,
                "command": "pytest test_a.py",
                "exit_code": 1,
            },
            {
                "turn_index": 2,
                "command": "pytest test_b.py",
                "exit_code": 1,
            },
            {
                "turn_index": 3,
                "command": "pytest test_a.py",
                "exit_code": 0,
            },
        ]
    )

    assert report["failed_command_count"] == 2
    assert report["retried_failure_count"] == 1  # Only test_a was retried
    assert report["resolved_retry_count"] == 1
    assert report["unresolved_retry_count"] == 0


def test_invalid_normalized_command_type_raises_error():
    with pytest.raises(ValueError, match="normalized_command must be a string"):
        analyze_session_command_retry_resolution(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": 0,
                    "normalized_command": 123,
                }
            ]
        )
