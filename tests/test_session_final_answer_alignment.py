"""Tests for session final answer alignment analyzer."""

import pytest

from synthesis.session_final_answer_alignment import (
    QUALITY_ALIGNED,
    QUALITY_INCOMPLETE,
    QUALITY_MISLEADING,
    STATUS_COMPLETED,
    STATUS_DEFERRED,
    STATUS_FAILED,
    STATUS_SKIPPED,
    FinalAnswerOutcome,
    analyze_session_final_answer_alignment,
)


def test_empty_input_returns_no_items():
    result = analyze_session_final_answer_alignment([])

    assert result.metrics.completed_items == 0
    assert result.alignment_quality == "no_items"
    assert "No tracked" in result.insights[0]


def test_perfect_alignment():
    result = analyze_session_final_answer_alignment(
        [
            FinalAnswerOutcome("a", "test", STATUS_COMPLETED, True),
            FinalAnswerOutcome("b", "cleanup", STATUS_SKIPPED, False),
        ]
    )

    assert result.metrics.mentioned_completed_count == 1
    assert result.alignment_quality == QUALITY_ALIGNED


def test_omitted_completed_work_is_incomplete():
    result = analyze_session_final_answer_alignment(
        [FinalAnswerOutcome("a", "module", STATUS_COMPLETED, False)]
    )

    assert result.metrics.omitted_completed_count == 1
    assert result.alignment_quality == QUALITY_INCOMPLETE


def test_overstated_failed_work_is_misleading():
    result = analyze_session_final_answer_alignment(
        [FinalAnswerOutcome("a", "test", STATUS_FAILED, True)]
    )

    assert result.metrics.overstated_failed_count == 1
    assert result.alignment_quality == QUALITY_MISLEADING


def test_deferred_work_called_out_honestly_is_aligned():
    result = analyze_session_final_answer_alignment(
        [FinalAnswerOutcome("a", "followup", STATUS_DEFERRED, True)]
    )

    assert result.metrics.deferred_items == 1
    assert result.metrics.unmentioned_deferred_count == 0
    assert result.alignment_quality == QUALITY_ALIGNED


def test_mixed_outcomes_prioritize_misleading_insight():
    result = analyze_session_final_answer_alignment(
        [
            FinalAnswerOutcome("a", "module", STATUS_COMPLETED, False),
            FinalAnswerOutcome("b", "test", STATUS_FAILED, True),
            FinalAnswerOutcome("c", "followup", STATUS_DEFERRED, False),
        ]
    )

    assert result.alignment_quality == QUALITY_MISLEADING
    assert "failed" in result.insights[0]


@pytest.mark.parametrize(
    ("outcomes", "message"),
    [
        ("bad", "list or tuple"),
        ([{"item_id": "a"}], "FinalAnswerOutcome"),
        ([FinalAnswerOutcome("", "test", STATUS_COMPLETED, True)], "item_id"),
        ([FinalAnswerOutcome("a", "", STATUS_COMPLETED, True)], "item_type"),
        ([FinalAnswerOutcome("a", "test", "done", True)], "unsupported status"),
        ([FinalAnswerOutcome("a", "test", STATUS_COMPLETED, "yes")], "boolean"),
    ],
)
def test_invalid_records_raise_clear_errors(outcomes, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_final_answer_alignment(outcomes)
