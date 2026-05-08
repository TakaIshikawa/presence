"""Planning followthrough analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class PlanItem:
    step_id: str
    text: str


@dataclass(frozen=True)
class ExecutionEvent:
    event_id: str
    text: str


@dataclass(frozen=True)
class PlanStepOutcome:
    step_id: str
    planned_index: int
    completed: bool
    event_index: int | None
    reordered: bool


@dataclass(frozen=True)
class PlanningFollowthroughMetrics:
    planned_steps: int
    completed_steps: int
    skipped_steps: int
    reordered_steps: int
    completion_rate: float


@dataclass(frozen=True)
class PlanningFollowthrough:
    metrics: PlanningFollowthroughMetrics
    outcomes: tuple[PlanStepOutcome, ...]
    quality: str
    insights: tuple[str, ...]


def analyze_planning_followthrough(
    plan_items: Sequence[PlanItem],
    events: Sequence[ExecutionEvent],
) -> PlanningFollowthrough:
    """Match planned steps to later completion evidence deterministically."""

    _validate_plan_inputs(plan_items, events)
    if not plan_items:
        metrics = PlanningFollowthroughMetrics(0, 0, 0, 0, 1.0)
        return PlanningFollowthrough(metrics, (), "no_plan", ("No planned steps supplied.",))

    outcomes: list[PlanStepOutcome] = []
    used_events: set[int] = set()
    last_event_index = -1
    for planned_index, item in enumerate(plan_items):
        match_index = None
        for event_index, event in enumerate(events):
            if event_index in used_events:
                continue
            if _text_matches(item.text, event.text):
                match_index = event_index
                used_events.add(event_index)
                break
        reordered = match_index is not None and match_index < last_event_index
        if match_index is not None:
            last_event_index = max(last_event_index, match_index)
        outcomes.append(
            PlanStepOutcome(item.step_id, planned_index, match_index is not None, match_index, reordered)
        )

    completed = sum(1 for outcome in outcomes if outcome.completed)
    reordered = sum(1 for outcome in outcomes if outcome.reordered)
    skipped = len(plan_items) - completed
    rate = completed / len(plan_items)
    metrics = PlanningFollowthroughMetrics(
        len(plan_items), completed, skipped, reordered, round(rate, 3)
    )
    quality = "complete" if rate == 1.0 and reordered == 0 else "partial" if rate >= 0.5 else "poor"
    return PlanningFollowthrough(metrics, tuple(outcomes), quality, _plan_insights(metrics))


def _validate_plan_inputs(
    plan_items: Sequence[PlanItem],
    events: Sequence[ExecutionEvent],
) -> None:
    if not isinstance(plan_items, (list, tuple)):
        raise ValueError("plan_items must be a list or tuple")
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    seen: set[str] = set()
    for item in plan_items:
        if not isinstance(item, PlanItem):
            raise ValueError("plan_items must contain PlanItem instances")
        if not item.step_id or item.step_id in seen:
            raise ValueError("step_id values must be non-empty and unique")
        if not isinstance(item.text, str) or not item.text.strip():
            raise ValueError("plan item text must be a non-empty string")
        seen.add(item.step_id)
    for event in events:
        if not isinstance(event, ExecutionEvent):
            raise ValueError("events must contain ExecutionEvent instances")
        if not isinstance(event.event_id, str) or not event.event_id:
            raise ValueError("event_id must be a non-empty string")
        if not isinstance(event.text, str) or not event.text.strip():
            raise ValueError("event text must be a non-empty string")


def _text_matches(plan_text: str, event_text: str) -> bool:
    plan = " ".join(plan_text.lower().split())
    event = " ".join(event_text.lower().split())
    if plan == event:
        return True
    plan_tokens = set(plan.split())
    event_tokens = set(event.split())
    if plan in event or event in plan:
        return True
    return bool(plan_tokens) and len(plan_tokens & event_tokens) / len(plan_tokens) >= 0.6


def _plan_insights(metrics: PlanningFollowthroughMetrics) -> tuple[str, ...]:
    insights = [f"Completed {metrics.completed_steps} of {metrics.planned_steps} planned steps."]
    if metrics.skipped_steps:
        insights.append(f"{metrics.skipped_steps} planned steps had no completion evidence.")
    if metrics.reordered_steps:
        insights.append(f"{metrics.reordered_steps} steps completed out of planned order.")
    return tuple(insights)
