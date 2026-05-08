"""Tests for session command failure acknowledgement analysis."""

import pytest

from synthesis.session_command_failure_acknowledgement import (
    analyze_session_command_failure_acknowledgement,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_command_failure_acknowledgement([])

    assert report["total_failures"] == 0
    assert report["acknowledged_failures"] == 0
    assert report["unacknowledged_failures"] == 0
    assert report["acknowledgement_rate"] == 0.0
    assert report["examples"] == []


def test_successful_commands_do_not_affect_failure_metrics():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/",
                "exit_code": 0,
                "output_excerpt": "All tests passed",
                "following_response": "Great, tests are passing",
            }
        ]
    )

    assert report["total_failures"] == 0
    assert report["acknowledged_failures"] == 0
    assert report["unacknowledged_failures"] == 0


def test_failed_command_with_explicit_acknowledgement():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/",
                "exit_code": 1,
                "output_excerpt": "FAILED tests/test_main.py::test_function",
                "following_response": "The test failed due to a missing import. Let me fix that.",
            }
        ]
    )

    assert report["total_failures"] == 1
    assert report["acknowledged_failures"] == 1
    assert report["unacknowledged_failures"] == 0
    assert report["acknowledgement_rate"] == 100.0
    assert report["examples"] == []


def test_failed_command_with_empty_following_response():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/",
                "exit_code": 1,
                "output_excerpt": "FAILED tests/test_main.py::test_function",
                "following_response": "",
            }
        ]
    )

    assert report["total_failures"] == 1
    assert report["acknowledged_failures"] == 0
    assert report["unacknowledged_failures"] == 1
    assert report["acknowledgement_rate"] == 0.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["turn_index"] == 1
    assert report["examples"][0]["exit_code"] == 1


def test_failed_command_with_unrelated_following_response():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest tests/",
                "exit_code": 1,
                "output_excerpt": "FAILED tests/test_main.py::test_function",
                "following_response": "Now let me work on something completely different.",
            }
        ]
    )

    assert report["total_failures"] == 1
    assert report["acknowledged_failures"] == 0
    assert report["unacknowledged_failures"] == 1
    assert len(report["examples"]) == 1


def test_acknowledgement_terms_recognized():
    acknowledgement_responses = [
        "The error occurred because of a typo",
        "I see the failure in the output",
        "The exit code 1 indicates a problem",
        "Looking at the traceback, the issue is clear",
        "Let me retry the command",
        "I need to fix the broken test",
        "There's an issue with the import",
        "The problem is in the configuration",
    ]

    for response in acknowledgement_responses:
        report = analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": 1,
                    "output_excerpt": "Test failed",
                    "following_response": response,
                }
            ]
        )

        assert report["acknowledged_failures"] == 1, f"Failed to recognize: {response}"


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list of command event dictionaries"):
        analyze_session_command_failure_acknowledgement({"command": "pytest"})


def test_non_mapping_record_raises_error():
    with pytest.raises(ValueError, match="records must be a list of command event dictionaries"):
        analyze_session_command_failure_acknowledgement(["not a dict"])


def test_negative_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be non-negative"):
        analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": -1,
                    "command": "pytest",
                    "exit_code": 1,
                    "output_excerpt": "",
                    "following_response": "",
                }
            ]
        )


def test_boolean_turn_index_raises_error():
    with pytest.raises(ValueError, match="turn_index must be an integer"):
        analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": True,
                    "command": "pytest",
                    "exit_code": 1,
                    "output_excerpt": "",
                    "following_response": "",
                }
            ]
        )


def test_non_integer_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": "1",
                    "output_excerpt": "",
                    "following_response": "",
                }
            ]
        )


def test_boolean_exit_code_raises_error():
    with pytest.raises(ValueError, match="exit_code must be an integer"):
        analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": False,
                    "output_excerpt": "",
                    "following_response": "",
                }
            ]
        )


def test_unordered_records_raise_error():
    with pytest.raises(ValueError, match="records must be ordered by turn_index"):
        analyze_session_command_failure_acknowledgement(
            [
                {
                    "turn_index": 2,
                    "command": "pytest",
                    "exit_code": 1,
                    "output_excerpt": "",
                    "following_response": "",
                },
                {
                    "turn_index": 1,
                    "command": "pytest",
                    "exit_code": 1,
                    "output_excerpt": "",
                    "following_response": "",
                },
            ]
        )


def test_examples_capped_at_five():
    records = [
        {
            "turn_index": i,
            "command": f"pytest test{i}.py",
            "exit_code": 1,
            "output_excerpt": f"Test {i} failed",
            "following_response": "Unrelated response",
        }
        for i in range(7)
    ]

    report = analyze_session_command_failure_acknowledgement(records)

    assert report["unacknowledged_failures"] == 7
    assert len(report["examples"]) == 5


def test_mixed_acknowledged_and_unacknowledged():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest test1.py",
                "exit_code": 1,
                "output_excerpt": "Test failed",
                "following_response": "I see the error, let me fix it",
            },
            {
                "turn_index": 2,
                "command": "pytest test2.py",
                "exit_code": 1,
                "output_excerpt": "Test failed",
                "following_response": "Moving on to something else",
            },
            {
                "turn_index": 3,
                "command": "pytest test3.py",
                "exit_code": 1,
                "output_excerpt": "Test failed",
                "following_response": "The traceback shows the problem",
            },
        ]
    )

    assert report["total_failures"] == 3
    assert report["acknowledged_failures"] == 2
    assert report["unacknowledged_failures"] == 1
    assert report["acknowledgement_rate"] == 66.67
    assert len(report["examples"]) == 1
    assert report["examples"][0]["turn_index"] == 2


def test_case_insensitive_acknowledgement():
    report = analyze_session_command_failure_acknowledgement(
        [
            {
                "turn_index": 1,
                "command": "pytest",
                "exit_code": 1,
                "output_excerpt": "Test failed",
                "following_response": "The FAILURE was due to missing imports",
            }
        ]
    )

    assert report["acknowledged_failures"] == 1
