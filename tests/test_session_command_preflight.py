"""Tests for session command preflight analysis."""

import pytest

from synthesis.session_command_preflight import (
    SessionCommandEvent,
    analyze_session_command_preflight,
)


def test_empty_input_returns_clean_report():
    report = analyze_session_command_preflight([])

    assert report.total_commands == 0
    assert report.pre_edit_commands == 0
    assert report.post_edit_commands == 0
    assert report.preflight_quality == "none"


def test_read_and_list_commands_before_edits_are_strong_preflight():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", "ls src/synthesis"),
            SessionCommandEvent(1, "command", "rg preflight src tests"),
            SessionCommandEvent(2, "edit", file_path="src/synthesis/example.py"),
        ]
    )

    assert report.pre_edit_commands == 2
    assert report.preflight_commands == 2
    assert report.preflight_quality == "strong"


def test_edit_first_session_is_none_and_mentions_missing_preflight():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "edit", file_path="src/synthesis/example.py"),
            SessionCommandEvent(1, "command", "pytest tests/test_example.py"),
        ]
    )

    assert report.pre_edit_commands == 0
    assert report.post_edit_commands == 1
    assert report.preflight_quality == "none"
    assert "before any read/list/test preflight command" in report.insights[0]


def test_mixed_command_timing_counts_pre_and_post_edit_commands():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", "pwd"),
            SessionCommandEvent(1, "command", "sed -n '1,80p' src/app.py"),
            SessionCommandEvent(2, "edit", file_path="src/app.py"),
            SessionCommandEvent(3, "command", "uv run pytest tests/test_app.py"),
        ]
    )

    assert report.total_commands == 3
    assert report.pre_edit_commands == 2
    assert report.post_edit_commands == 1
    assert report.preflight_quality == "strong"


def test_single_preflight_command_is_thin():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", "cat src/app.py"),
            SessionCommandEvent(1, "edit", file_path="src/app.py"),
        ]
    )

    assert report.preflight_quality == "thin"


def test_chained_read_command_counts_as_preflight():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", "cd repo && rg pattern src"),
            SessionCommandEvent(1, "edit", file_path="src/app.py"),
        ]
    )

    assert report.preflight_commands == 1
    assert report.preflight_quality == "thin"


@pytest.mark.parametrize(
    "command",
    [
        "python -m pytest tests/test_app.py",
        "uv run python -m pytest tests/test_app.py",
    ],
)
def test_python_module_pytest_counts_as_test_preflight(command):
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", command),
            SessionCommandEvent(1, "edit", file_path="src/app.py"),
        ]
    )

    assert report.preflight_commands == 1


def test_git_show_counts_as_read_preflight():
    report = analyze_session_command_preflight(
        [
            SessionCommandEvent(0, "command", "git show --stat"),
            SessionCommandEvent(1, "edit", file_path="src/app.py"),
        ]
    )

    assert report.preflight_commands == 1


def test_invalid_records_raise_value_error():
    with pytest.raises(ValueError, match="SessionCommandEvent"):
        analyze_session_command_preflight([{"turn_index": 0}])


def test_unordered_turn_indexes_raise_value_error():
    with pytest.raises(ValueError, match="ordered by turn_index"):
        analyze_session_command_preflight(
            [
                SessionCommandEvent(2, "command", "ls"),
                SessionCommandEvent(1, "edit", file_path="src/app.py"),
            ]
        )
