"""Tests for prompt revision churn analyzer."""

import pytest

from synthesis.prompt_revision_churn import (
    PromptRevision,
    SEVERITY_HIGH,
    SEVERITY_MODERATE,
    SEVERITY_STABLE,
    analyze_prompt_revision_churn,
)


def test_empty_revisions_are_stable_zero_state():
    result = analyze_prompt_revision_churn([])

    assert result.metrics.revision_count == 0
    assert result.severity == SEVERITY_STABLE


def test_single_stable_prompt_has_no_churn():
    result = analyze_prompt_revision_churn(
        [PromptRevision("r1", "handoff", "Summarize the session clearly.")]
    )

    assert result.metrics.average_token_delta == 0.0
    assert result.metrics.average_edit_distance == 0.0
    assert result.severity == SEVERITY_STABLE


def test_minor_repeated_edit_is_moderate_churn():
    result = analyze_prompt_revision_churn(
        [
            PromptRevision("r1", "handoff", "Summarize session clearly."),
            PromptRevision("r2", "handoff", "Summarize the session clearly."),
        ]
    )

    assert result.metrics.repeated_topic_churns == 1
    assert result.repeated_topics == ("handoff",)
    assert result.severity == SEVERITY_MODERATE


def test_major_rewrite_is_high_churn():
    result = analyze_prompt_revision_churn(
        [
            PromptRevision("r1", "task", "Write a brief status update."),
            PromptRevision(
                "r2",
                "task",
                "Build a detailed analyzer with validation metrics dataclasses and tests.",
            ),
        ]
    )

    assert result.metrics.average_edit_distance >= 0.55
    assert result.severity == SEVERITY_HIGH


def test_repeated_revision_loop_is_high_churn():
    result = analyze_prompt_revision_churn(
        [
            PromptRevision("r1", "planner", "Draft plan."),
            PromptRevision("r2", "planner", "Draft better plan."),
            PromptRevision("r3", "planner", "Draft concise plan."),
            PromptRevision("r4", "planner", "Draft final plan."),
        ]
    )

    assert result.metrics.repeated_topic_churns == 3
    assert result.severity == SEVERITY_HIGH
    assert "planner" in result.insights[1]


@pytest.mark.parametrize(
    "revisions",
    [
        "bad",
        [{"revision_id": "r1"}],
        [PromptRevision("", "topic", "prompt")],
        [PromptRevision("r1", "", "prompt")],
        [PromptRevision("r1", "topic", "a"), PromptRevision("r1", "topic", "b")],
    ],
)
def test_invalid_revision_records_raise_value_error(revisions):
    with pytest.raises(ValueError):
        analyze_prompt_revision_churn(revisions)
