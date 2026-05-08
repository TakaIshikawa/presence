"""Tests for task acceptance criteria measurability analysis."""

import pytest

from synthesis.task_acceptance_measurability import analyze_task_acceptance_measurability


def test_strong_criteria_are_measurable():
    report = analyze_task_acceptance_measurability(
        [{"title": "Add report", "acceptanceCriteria": ["Report includes command counts"], "testCommand": "pytest"}]
    )

    assert report["measurable_criteria_count"] == 1
    assert report["vague_criteria_count"] == 0


def test_vague_criteria_are_flagged():
    report = analyze_task_acceptance_measurability(
        [{"title": "Polish", "acceptanceCriteria": ["Improve output quality"], "testCommand": "pytest"}]
    )

    assert report["vague_criteria_count"] == 1
    assert report["examples"][0]["reason"] == "vague_criterion"


def test_missing_criteria_are_counted():
    report = analyze_task_acceptance_measurability([{"title": "Empty", "testCommand": "pytest"}])

    assert report["missing_criteria_count"] == 1


def test_missing_test_command_has_dedicated_count():
    report = analyze_task_acceptance_measurability(
        [{"title": "No test", "acceptanceCriteria": ["Returns stable counts"], "testCommand": ""}]
    )

    assert report["missing_test_command_count"] == 1


def test_mixed_tasks_report_percentages_and_examples():
    report = analyze_task_acceptance_measurability(
        [
            {"title": "Strong", "acceptanceCriteria": ["Raises ValueError for non-list input"], "testCommand": "pytest"},
            {"title": "Weak", "acceptanceCriteria": ["Make it better"], "testCommand": ""},
        ]
    )

    assert report["measurable_percentage"] == 50.0
    assert {example["title"] for example in report["examples"]} == {"Weak"}


def test_malformed_criteria_values_do_not_crash():
    report = analyze_task_acceptance_measurability([{"title": "Bad", "acceptanceCriteria": 42}])

    assert report["missing_criteria_count"] == 1
    assert report["missing_test_command_count"] == 1


def test_non_list_input_validation():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_task_acceptance_measurability({"title": "Bad"})
