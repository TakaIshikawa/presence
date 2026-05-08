"""Tests for command output truncation risk analysis."""

from __future__ import annotations

from dataclasses import dataclass

import pytest

from synthesis.command_output_truncation import analyze_command_output_truncation


@dataclass
class CommandRecord:
    command: str
    output: str
    turn_index: int = 0


def test_empty_input_returns_zeroed_metrics():
    report = analyze_command_output_truncation([])

    assert report["total_records"] == 0
    assert report["total_commands"] == 0
    assert report["risky_commands"] == 0
    assert report["risk_percentage"] == 0.0
    assert report["command_category_counts"] == {"test": 0, "lint": 0, "git": 0, "other": 0}
    assert report["weak_examples"] == []


def test_no_truncation_counts_clean_commands():
    report = analyze_command_output_truncation(
        [
            {"command": "pytest tests/test_widget.py", "output": "1 passed"},
            CommandRecord("git status --short", "clean", turn_index=4),
        ]
    )

    assert report["total_commands"] == 2
    assert report["clean_commands"] == 2
    assert report["risky_commands"] == 0
    assert report["risk_percentage"] == 0.0
    assert report["weak_examples"] == []


def test_explicit_truncation_metadata_flags_risk():
    report = analyze_command_output_truncation(
        [
            {
                "command": "pytest tests/test_widget.py",
                "turn_index": 7,
                "truncated": True,
                "max_output_tokens": 12000,
            },
            {
                "command": "ruff check src",
                "metadata": {"is_truncated": "exceeded result budget"},
            },
        ]
    )

    assert report["risky_commands"] == 2
    assert report["truncation_reason_counts"]["truncated"] == 2
    assert report["truncation_reason_counts"]["max_output_tokens"] == 1
    assert report["weak_examples"][0] == {
        "command": "pytest tests/test_widget.py",
        "turn_index": 7,
        "category": "test",
        "reason": "truncated",
    }


def test_textual_truncation_markers_flag_risk():
    report = analyze_command_output_truncation(
        [
            {"command": "npm test", "stderr": "Output truncated after 5000 lines"},
            {"command": "python script.py", "output": "diagnostic output omitted for brevity"},
            {"command": "git diff", "summary": "first hunk\n..."},
            {"command": "pytest", "message": "stopped because max_output_tokens was reached"},
        ]
    )

    assert report["risky_commands"] == 4
    assert report["truncation_reason_counts"]["truncated"] == 1
    assert report["truncation_reason_counts"]["output_omitted"] == 1
    assert report["truncation_reason_counts"]["ellipsis"] == 1
    assert report["truncation_reason_counts"]["max_output_tokens"] == 1


def test_category_grouping_counts_commands_and_risks():
    report = analyze_command_output_truncation(
        [
            {"command": "pytest tests/test_widget.py", "output": "truncated"},
            {"command": "ruff check src", "output": "truncated"},
            {"command": "git diff --stat", "output": "truncated"},
            {"command": "python manage.py migrate", "output": "truncated"},
            {"command": "git status", "output": "clean"},
        ]
    )

    assert report["command_category_counts"] == {"test": 1, "lint": 1, "git": 2, "other": 1}
    assert report["risk_category_counts"] == {"test": 1, "lint": 1, "git": 1, "other": 1}
    assert report["risk_percentage"] == 80.0


def test_examples_are_limited_to_five_representative_risks():
    records = [
        {"command": f"pytest tests/test_{index}.py", "turn_index": index, "output": "truncated"}
        for index in range(7)
    ]

    report = analyze_command_output_truncation(records)

    assert report["risky_commands"] == 7
    assert len(report["weak_examples"]) == 5
    assert [example["turn_index"] for example in report["weak_examples"]] == [0, 1, 2, 3, 4]


def test_malformed_records_are_ignored_but_non_list_input_raises():
    report = analyze_command_output_truncation(
        [None, "bad", {"output": "truncated"}, {"args": {"cmd": "pytest"}, "output": "ok"}]
    )

    assert report["total_records"] == 4
    assert report["total_commands"] == 1
    assert report["risky_commands"] == 0

    with pytest.raises(ValueError, match="records must be a list"):
        analyze_command_output_truncation({"command": "pytest"})
