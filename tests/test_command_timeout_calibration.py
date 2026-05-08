"""Tests for command timeout calibration analysis."""

import pytest

from synthesis.command_timeout_calibration import analyze_command_timeout_calibration


def test_empty_input_returns_stable_zero_metrics():
    report = analyze_command_timeout_calibration([])

    assert report["total_commands"] == 0
    assert report["risk_percentage"] == 0.0
    assert report["missing_timeout_count"] == 0


def test_timed_out_commands_are_counted():
    report = analyze_command_timeout_calibration(
        [{"command": "pytest", "duration_seconds": 60, "timeout_seconds": 60, "exit_code": 124}]
    )

    assert report["timed_out_count"] == 1
    assert report["near_timeout_count"] == 1
    assert report["examples"][0]["reason"] == "timed_out"


def test_commands_within_ten_percent_are_near_timeout():
    report = analyze_command_timeout_calibration(
        [{"command": "npm test", "duration_seconds": 91, "timeout_seconds": 100}]
    )

    assert report["near_timeout_count"] == 1
    assert report["risk_percentage"] == 100.0


def test_excessive_timeout_requires_five_times_duration():
    report = analyze_command_timeout_calibration(
        [
            {"command": "git status", "duration_seconds": 2, "timeout_seconds": 10},
            {"command": "git diff", "duration_seconds": 3, "timeout_seconds": 14},
        ]
    )

    assert report["excessive_timeout_count"] == 1


def test_category_distribution_is_reported():
    report = analyze_command_timeout_calibration(
        [
            {"command": "pytest", "timeout_seconds": 30},
            {"command": "npm install", "timeout_seconds": 30},
            {"command": "npm run build", "timeout_seconds": 30},
            {"command": "git status", "timeout_seconds": 30},
            {"command": "echo ok", "timeout_seconds": 30},
        ]
    )

    assert report["category_counts"] == {"test": 1, "install": 1, "build": 1, "git": 1, "other": 1}


def test_malformed_records_are_missing_quality_examples():
    report = analyze_command_timeout_calibration([None, {"command": "pytest"}])

    assert report["missing_timeout_count"] == 2
    assert [example["reason"] for example in report["examples"]] == ["malformed_record", "missing_timeout"]


def test_non_list_input_raises_clear_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_command_timeout_calibration({"command": "pytest"})
