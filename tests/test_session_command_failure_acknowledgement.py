"""Tests for session command failure acknowledgement analysis."""

import pytest

<<<<<<< HEAD
from synthesis.session_command_failure_acknowledgement import (
    analyze_session_command_failure_acknowledgement,
)
=======
from synthesis.session_command_failure_acknowledgement import analyze_session_command_failure_acknowledgement
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_command_failure_acknowledgement([])

    assert report["total_failures"] == 0
    assert report["acknowledged_failures"] == 0
    assert report["unacknowledged_failures"] == 0
    assert report["acknowledgement_rate"] == 0.0
    assert report["examples"] == []


def test_successful_commands_do_not_affect_failure_metrics():
<<<<<<< HEAD
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
=======
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest tests/",
            "exit_code": 0,
            "output_excerpt": "All tests passed",
            "following_response": "Great, tests pass!",
        }
    ])

    assert report["total_failures"] == 0


def test_failed_command_with_explicit_acknowledgement_counts_as_acknowledged():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest tests/",
            "exit_code": 1,
            "output_excerpt": "FAILED tests/test_foo.py",
            "following_response": "I see the test failed. Let me fix the issue.",
        }
    ])
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    assert report["total_failures"] == 1
    assert report["acknowledged_failures"] == 1
    assert report["unacknowledged_failures"] == 0
    assert report["acknowledgement_rate"] == 100.0
<<<<<<< HEAD
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
=======


def test_failed_command_with_empty_following_response_is_unacknowledged():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest tests/",
            "exit_code": 1,
            "output_excerpt": "FAILED tests/test_foo.py",
            "following_response": "",
        }
    ])

    assert report["unacknowledged_failures"] == 1
    assert report["acknowledged_failures"] == 0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["turn_index"] == 0


def test_failed_command_with_unrelated_following_response_is_unacknowledged():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest tests/",
            "exit_code": 1,
            "output_excerpt": "FAILED tests/test_foo.py",
            "following_response": "Let me continue with the next task.",
        }
    ])

    assert report["unacknowledged_failures"] == 1


def test_acknowledgement_term_failure_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "npm test",
            "exit_code": 1,
            "output_excerpt": "Test suite failed",
            "following_response": "The tests failed due to a missing import.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_error_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "npm build",
            "exit_code": 1,
            "output_excerpt": "Build error",
            "following_response": "I see an error in the build output.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_exit_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "script.sh",
            "exit_code": 127,
            "output_excerpt": "Command not found",
            "following_response": "The command exited with code 127.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_traceback_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "python script.py",
            "exit_code": 1,
            "output_excerpt": "Traceback (most recent call last)",
            "following_response": "Looking at the traceback, the issue is in line 42.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_retry_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "npm install",
            "exit_code": 1,
            "output_excerpt": "Network error",
            "following_response": "Let me retry the installation.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_fix_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 1,
            "output_excerpt": "Test failed",
            "following_response": "I'll fix this test now.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_issue_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "make",
            "exit_code": 2,
            "output_excerpt": "Compilation failed",
            "following_response": "The issue appears to be a missing header file.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_acknowledgement_term_problem_is_detected():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "cargo build",
            "exit_code": 101,
            "output_excerpt": "Build failed",
            "following_response": "The problem is a type mismatch.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_case_insensitive_acknowledgement_detection():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 1,
            "output_excerpt": "Test failed",
            "following_response": "The test FAILED because of a configuration ERROR.",
        }
    ])

    assert report["acknowledged_failures"] == 1


def test_invalid_turn_index_raises_value_error():
    with pytest.raises(ValueError, match="invalid turn_index"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": "not an int",
                "command": "pytest",
                "exit_code": 1,
            }
        ])


def test_negative_turn_index_raises_value_error():
    with pytest.raises(ValueError, match="negative turn_index"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": -1,
                "command": "pytest",
                "exit_code": 1,
            }
        ])


def test_boolean_turn_index_raises_value_error():
    with pytest.raises(ValueError, match="invalid turn_index"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": True,
                "command": "pytest",
                "exit_code": 1,
            }
        ])


def test_non_integer_exit_code_raises_value_error():
    with pytest.raises(ValueError, match="invalid exit_code"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": 0,
                "command": "pytest",
                "exit_code": "1",
            }
        ])


def test_boolean_exit_code_raises_value_error():
    with pytest.raises(ValueError, match="invalid exit_code"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": 0,
                "command": "pytest",
                "exit_code": False,
            }
        ])


def test_unordered_records_raise_value_error():
    with pytest.raises(ValueError, match="unordered turn_index"):
        analyze_session_command_failure_acknowledgement([
            {
                "turn_index": 5,
                "command": "pytest",
                "exit_code": 1,
            },
            {
                "turn_index": 3,
                "command": "pytest",
                "exit_code": 1,
            },
        ])


def test_non_mapping_record_raises_value_error():
    with pytest.raises(ValueError, match="not a dictionary"):
        analyze_session_command_failure_acknowledgement(["not a dict"])


def test_non_list_input_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_command_failure_acknowledgement({"turn_index": 0})
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def test_examples_capped_at_five():
    records = [
        {
            "turn_index": i,
            "command": f"pytest test{i}.py",
            "exit_code": 1,
<<<<<<< HEAD
            "output_excerpt": f"Test {i} failed",
            "following_response": "Unrelated response",
=======
            "output_excerpt": "Failed",
            "following_response": "Moving on",
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
        }
        for i in range(7)
    ]

    report = analyze_session_command_failure_acknowledgement(records)

    assert report["unacknowledged_failures"] == 7
    assert len(report["examples"]) == 5


<<<<<<< HEAD
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
=======
def test_mixed_acknowledged_and_unacknowledged_failures():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest test1.py",
            "exit_code": 1,
            "output_excerpt": "Failed",
            "following_response": "I see the failure, let me fix it.",
        },
        {
            "turn_index": 1,
            "command": "pytest test2.py",
            "exit_code": 1,
            "output_excerpt": "Failed",
            "following_response": "Next step.",
        },
        {
            "turn_index": 2,
            "command": "pytest test3.py",
            "exit_code": 1,
            "output_excerpt": "Failed",
            "following_response": "The error suggests a missing module.",
        },
    ])
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    assert report["total_failures"] == 3
    assert report["acknowledged_failures"] == 2
    assert report["unacknowledged_failures"] == 1
    assert report["acknowledgement_rate"] == 66.67
<<<<<<< HEAD
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
=======


def test_none_following_response_is_unacknowledged():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 0,
            "command": "pytest",
            "exit_code": 1,
            "output_excerpt": "Failed",
            "following_response": None,
        }
    ])

    assert report["unacknowledged_failures"] == 1


def test_example_includes_all_fields():
    report = analyze_session_command_failure_acknowledgement([
        {
            "turn_index": 5,
            "command": "npm test",
            "exit_code": 127,
            "output_excerpt": "Command not found: jest",
            "following_response": "Continuing with next step",
        }
    ])

    example = report["examples"][0]
    assert example["turn_index"] == 5
    assert example["command"] == "npm test"
    assert example["exit_code"] == 127
    assert example["output_excerpt"] == "Command not found: jest"
    assert example["following_response"] == "Continuing with next step"
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
