"""Tests for verification command coverage analysis."""

import pytest

from synthesis.verification_command_coverage import analyze_verification_command_coverage


def test_empty_input_returns_zero_report():
    report = analyze_verification_command_coverage([])

    assert report["total_records"] == 0
    assert report["records_with_verification"] == 0
    assert report["coverage_percentage"] == 0.0
    assert report["command_class_counts"]["missing"] == 0


def test_malformed_records_do_not_raise():
    report = analyze_verification_command_coverage([None, "bad", {"id": "ok", "commands": [42]}])

    assert report["total_records"] == 3
    assert report["records_missing_verification"] == 3
    assert report["command_class_counts"]["missing"] == 3
    assert len(report["weak_or_missing_examples"]) == 3


def test_targeted_pytest_command_counts_as_meaningful_verification():
    report = analyze_verification_command_coverage(
        [{"task_id": "a", "verification_commands": ["pytest tests/test_widget.py::test_renders"]}]
    )

    assert report["records_with_verification"] == 1
    assert report["coverage_percentage"] == 100.0
    assert report["command_class_counts"]["targeted"] == 1


def test_broad_suite_commands_are_classified():
    report = analyze_verification_command_coverage(
        [
            {"id": "pytest-all", "test_commands": "pytest"},
            {"id": "npm-all", "verification": "npm test"},
        ]
    )

    assert report["command_class_counts"]["broad"] == 2
    assert report["records_with_verification"] == 2
    assert report["coverage_percentage"] == 100.0


def test_typecheck_and_build_commands_are_classified():
    report = analyze_verification_command_coverage(
        [
            {"id": "types", "verification_commands": ["mypy src"]},
            {"id": "build", "verification_commands": ["npm run build"]},
        ]
    )

    assert report["command_class_counts"]["typecheck"] == 1
    assert report["command_class_counts"]["build"] == 1
    assert report["records_with_verification"] == 2


def test_exec_style_tool_calls_extract_dict_cmd_arguments():
    report = analyze_verification_command_coverage(
        [
            {
                "id": "exec-cmd",
                "tool_calls": [
                    {"name": "exec_command", "args": {"cmd": "pytest tests/test_widget.py"}}
                ],
            },
            {
                "id": "bash-cmd",
                "tool_calls": [{"name": "bash", "args": {"cmd": "mypy src"}}],
            },
            {
                "id": "shell-cmd",
                "tool_calls": [{"name": "shell", "args": {"cmd": "npm run build"}}],
            },
        ]
    )

    assert report["records_with_verification"] == 3
    assert report["command_class_counts"]["targeted"] == 1
    assert report["command_class_counts"]["typecheck"] == 1
    assert report["command_class_counts"]["build"] == 1


def test_exec_style_tool_calls_extract_string_inputs_and_iterable_commands():
    report = analyze_verification_command_coverage(
        [
            {
                "id": "string-input",
                "tool_calls": [{"name": "exec_command", "input": "pytest"}],
            },
            {
                "id": "iterable-commands",
                "tool_calls": [
                    {
                        "name": "shell",
                        "args": [
                            "pytest tests/test_widget.py::test_renders",
                            {"cmd": "npm run build"},
                        ],
                    }
                ],
            },
        ]
    )

    assert report["records_with_verification"] == 2
    assert report["command_class_counts"]["broad"] == 1
    assert report["command_class_counts"]["targeted"] == 1
    assert report["command_class_counts"]["build"] == 1
    assert report["total_commands"] == 3


def test_unknown_tool_calls_are_ignored_and_duplicate_commands_are_stable():
    report = analyze_verification_command_coverage(
        [
            {
                "id": "duplicates",
                "verification_commands": ["pytest tests/test_widget.py::test_renders"],
                "commands": ["pytest tests/test_widget.py::test_renders"],
                "tool_calls": [
                    {
                        "name": "exec_command",
                        "args": {"cmd": "pytest tests/test_widget.py::test_renders"},
                    },
                    {"name": "read_file", "args": {"cmd": "pytest tests/test_other.py"}},
                ],
            }
        ]
    )

    assert report["records_with_verification"] == 1
    assert report["total_commands"] == 1
    assert report["command_class_counts"]["targeted"] == 1
    assert report["command_class_counts"]["unknown"] == 0


def test_records_with_no_verification_are_reported_as_missing_examples():
    report = analyze_verification_command_coverage(
        [
            {"task_id": "verified", "commands": ["pytest tests/test_one.py"]},
            {"task_id": "missing", "status": "completed"},
            {"task_id": "unknown", "commands": ["echo done"]},
        ]
    )

    assert report["total_records"] == 3
    assert report["records_with_verification"] == 1
    assert report["records_missing_verification"] == 2
    assert report["coverage_percentage"] == pytest.approx(33.33)
    assert report["command_class_counts"]["missing"] == 1
    assert report["command_class_counts"]["unknown"] == 1
    assert [example["record_id"] for example in report["weak_or_missing_examples"]] == [
        "missing",
        "unknown",
    ]


def test_non_list_input_raises_clear_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_verification_command_coverage({"commands": ["pytest"]})
