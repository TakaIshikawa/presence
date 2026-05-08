"""Tests for session prompt drift analyzer."""

import pytest

from synthesis.session_prompt_drift import (
    DRIFT_INSTRUCTION_CHANGE,
    DRIFT_TOPIC_CHANGE,
    PromptInstructionRecord,
    analyze_session_prompt_drift,
)


def test_empty_input_returns_stable_zero_metrics():
    result = analyze_session_prompt_drift([])

    assert result.metrics.total_records == 0
    assert result.metrics.drift_events == 0
    assert result.metrics.drift_rate == 0.0
    assert result.examples == ()
    assert result.most_frequent_drift_labels == ()


def test_repeated_same_topic_and_instruction_are_not_drift():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("exports", "Add analyzer tests", 0),
            PromptInstructionRecord(" exports ", "add analyzer tests", 1),
        ]
    )

    assert result.metrics.comparable_transitions == 1
    assert result.metrics.drift_events == 0
    assert result.metrics.topic_changes == 0
    assert result.metrics.instruction_changes == 0
    assert result.examples == ()


def test_topic_change_counts_as_prompt_drift():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("exports", "Add analyzer tests", 0),
            PromptInstructionRecord("cli", "Add analyzer tests", 1),
        ]
    )

    assert result.metrics.drift_events == 1
    assert result.metrics.topic_changes == 1
    assert result.metrics.instruction_changes == 0
    assert result.metrics.drift_rate == 1.0
    assert result.most_frequent_drift_labels == (DRIFT_TOPIC_CHANGE,)
    assert result.examples[0].labels == (DRIFT_TOPIC_CHANGE,)


def test_instruction_change_counts_as_prompt_drift():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("exports", "Add analyzer tests", 0),
            PromptInstructionRecord("exports", "Add analyzer and CLI tests", 1),
        ]
    )

    assert result.metrics.drift_events == 1
    assert result.metrics.topic_changes == 0
    assert result.metrics.instruction_changes == 1
    assert result.most_frequent_drift_labels == (DRIFT_INSTRUCTION_CHANGE,)


def test_topic_and_instruction_changes_are_summarized_by_frequency():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("exports", "Add analyzer", 0),
            PromptInstructionRecord("reports", "Add analyzer", 1),
            PromptInstructionRecord("reports", "Add analyzer tests", 2),
            PromptInstructionRecord("cli", "Add CLI tests", 3),
        ]
    )

    assert result.metrics.drift_events == 3
    assert result.metrics.topic_changes == 2
    assert result.metrics.instruction_changes == 2
    assert result.metrics.drift_rate == 1.0
    assert result.most_frequent_drift_labels == (
        DRIFT_INSTRUCTION_CHANGE,
        DRIFT_TOPIC_CHANGE,
    )
    assert len(result.examples) == 3


def test_drift_rate_rounds_for_mixed_stable_and_changed_transitions():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("a", "one", 0),
            PromptInstructionRecord("a", "one", 1),
            PromptInstructionRecord("b", "one", 2),
            PromptInstructionRecord("b", "two", 3),
        ]
    )

    assert result.metrics.comparable_transitions == 3
    assert result.metrics.drift_events == 2
    assert result.metrics.drift_rate == 0.667


def test_drift_rate_half_boundary_is_reported_exactly():
    result = analyze_session_prompt_drift(
        [
            PromptInstructionRecord("a", "one", 0),
            PromptInstructionRecord("a", "one", 1),
            PromptInstructionRecord("b", "one", 2),
        ]
    )

    assert result.metrics.comparable_transitions == 2
    assert result.metrics.drift_events == 1
    assert result.metrics.drift_rate == 0.5


def test_examples_are_capped_at_five_entries():
    records = [
        PromptInstructionRecord(f"topic-{index}", f"instruction-{index}", index)
        for index in range(8)
    ]

    result = analyze_session_prompt_drift(records)

    assert result.metrics.drift_events == 7
    assert len(result.examples) == 5


@pytest.mark.parametrize(
    ("records", "message"),
    [
        ("bad", "records"),
        ([{"topic": "exports"}], "PromptInstructionRecord"),
        ([PromptInstructionRecord("", "do work", 0)], "topic"),
        ([PromptInstructionRecord("exports", "", 0)], "instruction"),
        ([PromptInstructionRecord("exports", "do work", -1)], "turn_index"),
        ([PromptInstructionRecord("exports", "do work", True)], "turn_index"),
        ([PromptInstructionRecord("exports", "do work", 0, "")], "prompt_id"),
        (
            [
                PromptInstructionRecord("exports", "do work", 0, "p1"),
                PromptInstructionRecord("exports", "do more", 1, "p1"),
            ],
            "prompt_id",
        ),
        (
            [
                PromptInstructionRecord("exports", "do work", 2),
                PromptInstructionRecord("exports", "do more", 1),
            ],
            "turn_index",
        ),
    ],
)
def test_invalid_prompt_records_raise_value_error(records, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_prompt_drift(records)
