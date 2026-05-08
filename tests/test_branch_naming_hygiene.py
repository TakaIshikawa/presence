"""Tests for branch naming hygiene analysis."""

import pytest

from synthesis.branch_naming_hygiene import analyze_branch_naming_hygiene


def test_valid_relay_branches_are_counted_by_agent_prefix():
    report = analyze_branch_naming_hygiene(
        [
            {"branch": "relay/codex/add-command-output-truncation-01kr39"},
            {"branch": "relay/claude-code/add-timeout-calibration-abcdef"},
        ]
    )

    assert report["valid_branches"] == 2
    assert report["agent_prefix_counts"]["relay/codex"] == 1
    assert report["agent_prefix_counts"]["relay/claude-code"] == 1


def test_missing_prefix_is_invalid():
    report = analyze_branch_naming_hygiene([{"branch": "feature/add-command-output-01kr39"}])

    assert report["issue_counts"]["missing_agent_prefix"] == 1


def test_missing_unique_suffix_is_invalid():
    report = analyze_branch_naming_hygiene([{"branch": "relay/codex/add-command-output"}])

    assert report["issue_counts"]["missing_unique_suffix"] == 1


def test_uppercase_and_whitespace_are_reported():
    report = analyze_branch_naming_hygiene([{"branch": "relay/codex/Add Command Output-01kr39"}])

    assert report["issue_counts"]["uppercase"] == 1
    assert report["issue_counts"]["whitespace"] == 1


def test_overly_long_branch_names_are_reported():
    report = analyze_branch_naming_hygiene([{"branch": "relay/codex/" + "very-long-" * 12 + "01kr39"}])

    assert report["issue_counts"]["too_long"] == 1


def test_alternate_field_names_are_supported():
    report = analyze_branch_naming_hygiene(
        [
            {"branch_name": "relay/codex/add-command-output-01kr39"},
            {"ref": "relay/codex/add-other-task-abcdef"},
        ]
    )

    assert report["valid_branches"] == 2


def test_empty_input_has_stable_percentage():
    report = analyze_branch_naming_hygiene([])

    assert report["valid_percentage"] == 0.0
    assert report["valid_branches"] == 0


def test_non_list_input_validation():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_branch_naming_hygiene({"branch": "relay/codex/add-command-output-01kr39"})
