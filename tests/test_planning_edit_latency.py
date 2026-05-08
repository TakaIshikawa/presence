"""Tests for planning edit latency analyzer."""

import pytest

from synthesis.planning_edit_latency import (
    KIND_EDIT,
    KIND_PLAN,
    QUALITY_ABANDONED,
    QUALITY_FAST,
    QUALITY_NO_PLANS,
    QUALITY_UNEVEN,
    PlanningEditTurn,
    analyze_planning_edit_latency,
)


def test_no_plans_returns_zero_state():
    result = analyze_planning_edit_latency([])

    assert result.metrics.plan_count == 0
    assert result.metrics.average_edit_latency_turns == 0.0
    assert result.outcomes == ()
    assert result.quality == QUALITY_NO_PLANS
    assert "No implementation plans" in result.insights[0]


def test_immediate_edit_counts_fast_followthrough():
    result = analyze_planning_edit_latency(
        [
            PlanningEditTurn(0, KIND_PLAN, "synthesis"),
            PlanningEditTurn(1, KIND_EDIT, "synthesis", 2),
        ]
    )

    assert result.metrics.plan_count == 1
    assert result.metrics.plans_with_edits == 1
    assert result.metrics.abandoned_plans == 0
    assert result.metrics.immediate_edit_count == 1
    assert result.metrics.delayed_edit_count == 0
    assert result.metrics.average_edit_latency_turns == 1.0
    assert result.quality == QUALITY_FAST


def test_delayed_edit_degrades_quality_and_rounds_latency():
    result = analyze_planning_edit_latency(
        [
            PlanningEditTurn(0, KIND_PLAN, "tests"),
            PlanningEditTurn(1, KIND_PLAN, "docs"),
            PlanningEditTurn(3, KIND_EDIT, "tests", 1),
            PlanningEditTurn(6, KIND_EDIT, "docs", 1),
        ]
    )

    assert result.metrics.plans_with_edits == 2
    assert result.metrics.immediate_edit_count == 0
    assert result.metrics.delayed_edit_count == 2
    assert result.metrics.average_edit_latency_turns == 4.0
    assert result.quality == QUALITY_UNEVEN
    assert any("waited" in insight for insight in result.insights)


def test_abandoned_plan_has_no_same_scope_edit():
    result = analyze_planning_edit_latency(
        [
            PlanningEditTurn(0, KIND_PLAN, "api"),
            PlanningEditTurn(1, KIND_EDIT, "ui", 3),
        ]
    )

    assert result.metrics.abandoned_plans == 1
    assert result.outcomes[0].edit_turn_index is None
    assert result.quality == QUALITY_ABANDONED


def test_multiple_scopes_match_first_later_same_scope_edit():
    result = analyze_planning_edit_latency(
        [
            PlanningEditTurn(0, KIND_PLAN, "api"),
            PlanningEditTurn(1, KIND_PLAN, "ui"),
            PlanningEditTurn(2, KIND_EDIT, "api", 1),
            PlanningEditTurn(3, KIND_EDIT, "ui", 2),
            PlanningEditTurn(4, KIND_EDIT, "api", 5),
        ]
    )

    assert result.outcomes[0].edit_turn_index == 2
    assert result.outcomes[0].file_count == 1
    assert result.outcomes[1].edit_turn_index == 3
    assert result.metrics.scope_distribution == (("api", 1), ("ui", 1))


@pytest.mark.parametrize(
    ("turns", "message"),
    [
        ("bad", "list or tuple"),
        ([{"turn_index": 0}], "PlanningEditTurn"),
        ([PlanningEditTurn(-1, KIND_PLAN, "api")], "turn_index"),
        (
            [PlanningEditTurn(1, KIND_PLAN, "api"), PlanningEditTurn(1, KIND_EDIT, "api", 1)],
            "strictly increasing",
        ),
        ([PlanningEditTurn(0, "note", "api")], "unsupported event_type"),
        ([PlanningEditTurn(0, KIND_PLAN, "")], "scope"),
        ([PlanningEditTurn(0, KIND_EDIT, "api", 0)], "greater than 0"),
        ([PlanningEditTurn(0, KIND_PLAN, "api", 1)], "file_count 0"),
    ],
)
def test_invalid_inputs_raise_clear_value_errors(turns, message):
    with pytest.raises(ValueError, match=message):
        analyze_planning_edit_latency(turns)
