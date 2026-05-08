"""Tests for prompt constraint conflict analysis."""

from synthesis.prompt_constraint_conflicts import analyze_prompt_constraint_conflicts


def test_empty_input_returns_zero_conflicts():
    report = analyze_prompt_constraint_conflicts([])

    assert report["prompt_count"] == 0
    assert report["conflict_count"] == 0
    assert report["conflicts"] == []


def test_detects_testing_conflict():
    report = analyze_prompt_constraint_conflicts(
        [{"text": "Do not add tests, but add tests for this change."}]
    )

    assert report["has_conflicts"]
    assert report["conflicts"][0]["conflict_type"] == "testing_conflict"


def test_string_prompt_is_accepted():
    report = analyze_prompt_constraint_conflicts("Be concise and comprehensive.")

    assert report["prompt_count"] == 1
    assert report["conflict_count"] == 1
