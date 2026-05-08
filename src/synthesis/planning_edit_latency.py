"""Planning edit latency analyzer.

Measures how quickly explicit implementation plans are followed by concrete
edits in the same scope.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence


KIND_PLAN = "plan"
KIND_EDIT = "edit"

IMMEDIATE_EDIT_LATENCY_TURNS = 1
DELAYED_EDIT_LATENCY_TURNS = 3

QUALITY_NO_PLANS = "no_plans"
QUALITY_FAST = "fast"
QUALITY_UNEVEN = "uneven"
QUALITY_ABANDONED = "abandoned"


@dataclass(frozen=True)
class PlanningEditTurn:
    """Single plan or edit turn."""

    turn_index: int
    event_type: str
    scope: str
    file_count: int = 0
    timestamp: datetime | None = None


@dataclass(frozen=True)
class PlanEditOutcome:
    """Edit matching outcome for one plan."""

    plan_turn_index: int
    scope: str
    edit_turn_index: int | None
    edit_latency_turns: int | None
    file_count: int


@dataclass(frozen=True)
class PlanningEditLatencyMetrics:
    """Aggregate planning-to-edit latency metrics."""

    plan_count: int
    plans_with_edits: int
    abandoned_plans: int
    average_edit_latency_turns: float
    immediate_edit_count: int
    delayed_edit_count: int
    scope_distribution: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class PlanningEditLatencyAnalysis:
    """Complete planning edit latency analysis."""

    metrics: PlanningEditLatencyMetrics
    outcomes: tuple[PlanEditOutcome, ...]
    quality: str
    insights: tuple[str, ...]


def analyze_planning_edit_latency(
    turns: Sequence[PlanningEditTurn],
) -> PlanningEditLatencyAnalysis:
    """Analyze latency from each plan to the first later same-scope edit."""

    _validate_turns(turns)
    plans = [turn for turn in turns if turn.event_type == KIND_PLAN]
    if not plans:
        metrics = PlanningEditLatencyMetrics(0, 0, 0, 0.0, 0, 0, ())
        return PlanningEditLatencyAnalysis(
            metrics=metrics,
            outcomes=(),
            quality=QUALITY_NO_PLANS,
            insights=("No implementation plans supplied.",),
        )

    outcomes = tuple(_match_plan_edits(turns))
    latencies = [
        outcome.edit_latency_turns
        for outcome in outcomes
        if outcome.edit_latency_turns is not None
    ]
    plans_with_edits = len(latencies)
    abandoned = len(outcomes) - plans_with_edits
    immediate = sum(
        1 for latency in latencies if latency <= IMMEDIATE_EDIT_LATENCY_TURNS
    )
    delayed = sum(1 for latency in latencies if latency >= DELAYED_EDIT_LATENCY_TURNS)
    scope_counts: dict[str, int] = {}
    for plan in plans:
        scope_counts[plan.scope] = scope_counts.get(plan.scope, 0) + 1

    metrics = PlanningEditLatencyMetrics(
        plan_count=len(plans),
        plans_with_edits=plans_with_edits,
        abandoned_plans=abandoned,
        average_edit_latency_turns=round(
            sum(latencies) / len(latencies), 2
        )
        if latencies
        else 0.0,
        immediate_edit_count=immediate,
        delayed_edit_count=delayed,
        scope_distribution=tuple(sorted(scope_counts.items())),
    )
    quality = _classify_quality(metrics)
    return PlanningEditLatencyAnalysis(
        metrics=metrics,
        outcomes=outcomes,
        quality=quality,
        insights=_generate_insights(metrics),
    )


def _validate_turns(turns: Sequence[PlanningEditTurn]) -> None:
    if not isinstance(turns, (list, tuple)):
        raise ValueError("turns must be a list or tuple")

    last_index = -1
    last_timestamp: datetime | None = None
    for position, turn in enumerate(turns):
        if not isinstance(turn, PlanningEditTurn):
            raise ValueError("turns must contain PlanningEditTurn instances")
        if not isinstance(turn.turn_index, int) or turn.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if turn.turn_index <= last_index:
            raise ValueError("turn_index values must be strictly increasing")
        if turn.event_type not in {KIND_PLAN, KIND_EDIT}:
            raise ValueError(
                f"turn at position {position} has unsupported event_type: {turn.event_type}"
            )
        if not isinstance(turn.scope, str) or not turn.scope.strip():
            raise ValueError("scope must be a non-empty string")
        if not isinstance(turn.file_count, int) or turn.file_count < 0:
            raise ValueError("file_count must be a non-negative integer")
        if turn.timestamp is not None:
            if not _is_timezone_aware(turn.timestamp):
                raise ValueError("timestamp must be timezone-aware when provided")
            if last_timestamp is not None and turn.timestamp < last_timestamp:
                raise ValueError("timestamp values must be chronological")
            last_timestamp = turn.timestamp
        if turn.event_type == KIND_PLAN and turn.file_count != 0:
            raise ValueError("plan turns must have file_count 0")
        if turn.event_type == KIND_EDIT and turn.file_count == 0:
            raise ValueError("edit turns must have file_count greater than 0")
        last_index = turn.turn_index


def _is_timezone_aware(value: object) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _match_plan_edits(turns: Sequence[PlanningEditTurn]) -> list[PlanEditOutcome]:
    outcomes: list[PlanEditOutcome] = []
    for index, plan in enumerate(turns):
        if plan.event_type != KIND_PLAN:
            continue

        match = next(
            (
                later
                for later in turns[index + 1 :]
                if later.event_type == KIND_EDIT and later.scope == plan.scope
            ),
            None,
        )
        outcomes.append(
            PlanEditOutcome(
                plan_turn_index=plan.turn_index,
                scope=plan.scope,
                edit_turn_index=match.turn_index if match else None,
                edit_latency_turns=match.turn_index - plan.turn_index
                if match
                else None,
                file_count=match.file_count if match else 0,
            )
        )
    return outcomes


def _classify_quality(metrics: PlanningEditLatencyMetrics) -> str:
    if metrics.plan_count == 0:
        return QUALITY_NO_PLANS
    if metrics.abandoned_plans:
        return QUALITY_ABANDONED
    if metrics.delayed_edit_count:
        return QUALITY_UNEVEN
    return QUALITY_FAST


def _generate_insights(
    metrics: PlanningEditLatencyMetrics,
) -> tuple[str, ...]:
    if metrics.plan_count == 0:
        return ("No implementation plans supplied.",)

    insights = [
        f"{metrics.plans_with_edits} of {metrics.plan_count} plans reached a same-scope edit."
    ]
    if metrics.abandoned_plans:
        insights.append(f"{metrics.abandoned_plans} plans had no later same-scope edit.")
    if metrics.delayed_edit_count:
        insights.append(
            f"{metrics.delayed_edit_count} plans waited at least "
            f"{DELAYED_EDIT_LATENCY_TURNS} turns before editing."
        )
    if metrics.scope_distribution:
        top_scope, top_count = max(metrics.scope_distribution, key=lambda item: item[1])
        insights.append(f"Most planned scope: {top_scope} ({top_count} plans).")
    return tuple(insights)
