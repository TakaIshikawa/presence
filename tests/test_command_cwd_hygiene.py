"""Tests for command working-directory hygiene analysis."""

import pytest

from synthesis.command_cwd_hygiene import analyze_command_cwd_hygiene


def test_empty_input_returns_zeroed_metrics():
    report = analyze_command_cwd_hygiene([])

    assert report["total_commands"] == 0
    assert report["valid_cwd_count"] == 0
    assert report["hygiene_percentage"] == 0.0
    assert report["examples"] == []


def test_valid_project_root_commands_are_counted_clean():
    report = analyze_command_cwd_hygiene(
        [
            {"command": "pytest", "cwd": "/repo/project", "project_path": "/repo/project"},
            {"command": "ruff check", "cwd": "/repo/project/src", "project_path": "/repo/project"},
        ]
    )

    assert report["valid_cwd_count"] == 2
    assert report["missing_cwd_count"] == 0
    assert report["outside_project_count"] == 0
    assert report["hygiene_percentage"] == 100.0


def test_missing_cwd_and_workdir_are_counted_separately():
    report = analyze_command_cwd_hygiene([{"command": "pytest", "project_path": "/repo/project"}])

    assert report["missing_cwd_count"] == 1
    assert report["outside_project_count"] == 0
    assert report["examples"][0]["reason"] == "missing_cwd"


def test_outside_project_paths_are_flagged():
    report = analyze_command_cwd_hygiene(
        [{"command": "pytest", "cwd": "/tmp/other", "project_path": "/repo/project"}]
    )

    assert report["outside_project_count"] == 1
    assert report["missing_cwd_count"] == 0
    assert report["examples"][0]["reason"] == "outside_project_path"


def test_mixed_cwd_field_names_are_supported():
    report = analyze_command_cwd_hygiene(
        [
            {"cmd": "pytest", "workdir": "/repo/project", "project_path": "/repo/project"},
            {"cmd": "npm test", "cwd": "/repo/project/web", "project_path": "/repo/project"},
        ]
    )

    assert report["valid_cwd_count"] == 2


def test_examples_are_capped_deterministically():
    records = [{"command": f"pytest {index}", "project_path": "/repo/project"} for index in range(7)]

    report = analyze_command_cwd_hygiene(records)

    assert report["missing_cwd_count"] == 7
    assert len(report["examples"]) == 5
    assert [example["index"] for example in report["examples"]] == [0, 1, 2, 3, 4]


def test_non_list_input_raises_clear_error():
    with pytest.raises(ValueError, match="records must be a list of command record dictionaries"):
        analyze_command_cwd_hygiene({"command": "pytest"})
