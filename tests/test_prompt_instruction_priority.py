"""Tests for prompt instruction priority analysis."""

import pytest

from synthesis.prompt_instruction_priority import PromptInstructionRecord, analyze_prompt_instruction_priority


def test_empty_prompts_are_clear():
    report = analyze_prompt_instruction_priority([])

    assert report.total_prompts == 0
    assert report.priority_quality == "clear"


def test_hard_only_prompt_counts_constraints():
    report = analyze_prompt_instruction_priority(
        [PromptInstructionRecord("p1", "You must run tests and never skip review.")]
    )

    assert report.hard_constraints == 2
    assert report.soft_preferences == 0
    assert report.priority_quality == "clear"


def test_balanced_hard_and_soft_prompt_is_mixed():
    report = analyze_prompt_instruction_priority(
        [PromptInstructionRecord("p1", "Must validate output. Prefer targeted tests. Optional docs update.")]
    )

    assert report.hard_constraints == 1
    assert report.soft_preferences == 2
    assert report.priority_quality == "mixed"


def test_ambiguous_unranked_constraints_are_overloaded():
    report = analyze_prompt_instruction_priority(
        [PromptInstructionRecord("p1", "Must test. Never push. Avoid broad refactors. Must commit.")]
    )

    assert report.priority_quality == "overloaded"
    assert report.overloaded_prompts == ("p1",)
    assert "no ordering language" in report.insights[0]


def test_case_normalization_is_deterministic():
    report = analyze_prompt_instruction_priority(
        [PromptInstructionRecord("p1", "MUST do this. Should do that. PREFER this.")]
    )

    assert report.hard_constraints == 1
    assert report.soft_preferences == 2


def test_invalid_prompt_records_raise_value_error():
    with pytest.raises(ValueError, match="PromptInstructionRecord"):
        analyze_prompt_instruction_priority([{"prompt_id": "p1"}])
