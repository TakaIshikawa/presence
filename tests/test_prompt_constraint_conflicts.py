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


def test_detects_high_and_low_priority_conflict_in_string_prompt():
    report = analyze_prompt_constraint_conflicts(
        "Use highest priority for speed, but treat speed as low priority."
    )

    assert report["has_conflicts"]
    assert report["conflicts"][0]["conflict_type"] == "priority_conflict"


def test_detects_must_and_optional_conflict_in_prompt_dict():
    report = analyze_prompt_constraint_conflicts(
        [{"prompt": "You must update docs, but the docs update is optional."}]
    )

    assert report["has_conflicts"]
    assert report["conflicts"][0]["conflict_type"] == "priority_conflict"


def test_detects_required_and_nice_to_have_conflict_in_mixed_prompts():
    report = analyze_prompt_constraint_conflicts(
        [
            "Be concise.",
            {"prompt": "Verification is required, but verification is nice to have."},
        ]
    )

    assert report["prompt_count"] == 2
    assert report["conflict_count"] == 1
    assert report["conflicts"][0]["index"] == 1
    assert report["conflicts"][0]["conflict_type"] == "priority_conflict"
