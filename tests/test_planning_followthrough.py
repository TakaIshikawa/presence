"""Tests for planning followthrough analyzer."""

import pytest

from synthesis.planning_followthrough import (
    ExecutionEvent,
    PlanItem,
    analyze_planning_followthrough,
)


def test_empty_plan_returns_useful_zero_state():
    result = analyze_planning_followthrough([], [])

    assert result.metrics.planned_steps == 0
    assert result.metrics.completion_rate == 1.0
    assert result.quality == "no_plan"


def test_full_completion_exact_matching():
    result = analyze_planning_followthrough(
        [PlanItem("1", "Run tests"), PlanItem("2", "Commit changes")],
        [ExecutionEvent("a", "Run tests"), ExecutionEvent("b", "Commit changes")],
    )

    assert result.metrics.completed_steps == 2
    assert result.metrics.skipped_steps == 0
    assert result.quality == "complete"


def test_case_insensitive_and_partial_matching():
    result = analyze_planning_followthrough(
        [PlanItem("1", "Run focused pytest validation")],
        [ExecutionEvent("a", "Finished RUN focused pytest validation successfully")],
    )

    assert result.outcomes[0].completed is True


def test_partial_completion_counts_skipped_steps_and_rounds_rate():
    result = analyze_planning_followthrough(
        [PlanItem("1", "Add module"), PlanItem("2", "Run tests"), PlanItem("3", "Commit")],
        [ExecutionEvent("a", "Add module")],
    )

    assert result.metrics.completed_steps == 1
    assert result.metrics.skipped_steps == 2
    assert result.metrics.completion_rate == 0.333
    assert result.quality == "poor"


def test_out_of_order_execution_counts_reordered_step():
    result = analyze_planning_followthrough(
        [PlanItem("1", "First step"), PlanItem("2", "Second step")],
        [ExecutionEvent("a", "Second step"), ExecutionEvent("b", "First step")],
    )

    assert result.metrics.completed_steps == 2
    assert result.metrics.reordered_steps == 1
    assert result.quality == "partial"


def test_duplicate_plan_items_are_invalid():
    with pytest.raises(ValueError, match="unique"):
        analyze_planning_followthrough(
            [PlanItem("1", "A"), PlanItem("1", "B")],
            [],
        )


@pytest.mark.parametrize(
    ("plan_items", "events"),
    [
        ("bad", []),
        ([], "bad"),
        ([{"step_id": "1"}], []),
        ([PlanItem("", "text")], []),
        ([PlanItem("1", "")], []),
        ([], [{"event_id": "e"}]),
        ([], [ExecutionEvent("", "text")]),
        ([], [ExecutionEvent("e", "")]),
    ],
)
def test_invalid_inputs_raise_value_error(plan_items, events):
    with pytest.raises(ValueError):
        analyze_planning_followthrough(plan_items, events)
