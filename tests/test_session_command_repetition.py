"""Tests for session command repetition analysis."""

import pytest

from synthesis.session_command_repetition import (
    SessionCommandRecord,
    analyze_session_command_repetition,
)


def test_empty_input_returns_zeroed_metrics_and_no_examples():
    report = analyze_session_command_repetition([])

    assert report.metrics.total_commands == 0
    assert report.metrics.unique_commands == 0
    assert report.metrics.repeated_commands == 0
    assert report.metrics.repeat_rate == 0.0
    assert report.repeated_examples == ()


def test_no_repeats_reports_zero_repeat_rate():
    report = analyze_session_command_repetition(
        [
            SessionCommandRecord(0, "pwd"),
            SessionCommandRecord(1, "rg analyzer src"),
            SessionCommandRecord(2, "uv run pytest tests/test_example.py"),
        ]
    )

    assert report.metrics.total_commands == 3
    assert report.metrics.unique_commands == 3
    assert report.metrics.repeated_commands == 0
    assert report.metrics.repeat_rate == 0.0
    assert report.repeated_examples == ()


def test_whitespace_and_case_normalization_counts_repeats():
    report = analyze_session_command_repetition(
        [
            SessionCommandRecord(0, "UV   RUN PYTEST tests/test_app.py"),
            SessionCommandRecord(2, "uv run pytest   tests/test_app.py"),
            SessionCommandRecord(4, "uv run pytest tests/test_app.py"),
        ]
    )

    assert report.metrics.unique_commands == 1
    assert report.metrics.repeated_commands == 2
    assert report.metrics.repeat_rate == 0.667
    assert report.repeated_examples[0].command == "uv run pytest tests/test_app.py"
    assert report.repeated_examples[0].first_turn == 0
    assert report.repeated_examples[0].repeated_turn == 2
    assert report.repeated_examples[0].repeat_count == 2
    assert report.repeated_examples[1].repeat_count == 3


def test_examples_are_capped_at_five():
    records = [
        SessionCommandRecord(0, "pwd"),
        SessionCommandRecord(1, "ls"),
        SessionCommandRecord(2, "rg src"),
        SessionCommandRecord(3, "git status"),
        SessionCommandRecord(4, "pytest"),
        SessionCommandRecord(5, "git diff"),
        SessionCommandRecord(6, "PWD"),
        SessionCommandRecord(7, "LS"),
        SessionCommandRecord(8, "RG   src"),
        SessionCommandRecord(9, "GIT STATUS"),
        SessionCommandRecord(10, "PYTEST"),
        SessionCommandRecord(11, "GIT DIFF"),
    ]

    report = analyze_session_command_repetition(records)

    assert report.metrics.repeated_commands == 6
    assert len(report.repeated_examples) == 5
    assert [example.command for example in report.repeated_examples] == [
        "pwd",
        "ls",
        "rg src",
        "git status",
        "pytest",
    ]


def test_repeat_rate_is_rounded_to_three_decimals():
    report = analyze_session_command_repetition(
        [
            SessionCommandRecord(0, "pwd"),
            SessionCommandRecord(1, "ls"),
            SessionCommandRecord(2, "PWD"),
        ]
    )

    assert report.metrics.repeat_rate == 0.333


@pytest.mark.parametrize(
    "records",
    [
        "bad",
        [{"turn_index": 0, "command": "pwd"}],
        [SessionCommandRecord(-1, "pwd")],
        [SessionCommandRecord(True, "pwd")],
        [SessionCommandRecord(1, "pwd"), SessionCommandRecord(0, "ls")],
        [SessionCommandRecord(0, "")],
        [SessionCommandRecord(0, "   ")],
        [SessionCommandRecord(0, 1)],
    ],
)
def test_invalid_inputs_raise_value_error(records):
    with pytest.raises(ValueError, match="records|turn_index|ordered|command"):
        analyze_session_command_repetition(records)
