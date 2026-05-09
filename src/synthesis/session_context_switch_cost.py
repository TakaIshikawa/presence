"""Session context switch cost analyzer.

Analyzes context switching overhead and costs during session execution. Identifies
context switch boundaries, calculates switch frequency and duration costs, and
reports statistics on context retention vs reload patterns.

Context switch metrics:
- Switch frequency: Number of context switches per session
- Switch overhead per transition: Average time/tokens spent on context switching
- Cumulative context cost: Total overhead from all context switches
- Switch efficiency: Ratio of productive work vs context switching
- Context retention patterns: How well context is maintained vs reloaded

Quality indicators:
- Low switch frequency (<5 per session): Minimal context disruption
- Low switch overhead (<100 tokens): Efficient context transitions
- High switch efficiency (>90%): Most time spent on productive work
- Strong context retention: Context reused effectively without frequent reloads
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence, TypedDict


EVENT_CONTEXT_SWITCH = "context_switch"
EVENT_CONTEXT_LOAD = "context_load"
EVENT_CONTEXT_RETAIN = "context_retain"

# Default threshold for significant context switch cost (tokens)
DEFAULT_SWITCH_COST_THRESHOLD = 100


class ContextSwitchDetail(TypedDict):
    """Details of a context switch event."""

    switch_id: str
    turn_index: int
    from_context: str
    to_context: str
    switch_cost_tokens: int
    switch_duration_seconds: int


class ContextLoadDetail(TypedDict):
    """Details of a context load event."""

    load_id: str
    turn_index: int
    context_name: str
    load_cost_tokens: int
    is_reload: bool


@dataclass(frozen=True)
class ContextSwitchEvent:
    """Event in context switch tracking."""

    event_type: str
    turn_index: int
    event_id: str
    context_name: str = ""
    from_context: str = ""
    to_context: str = ""
    cost_tokens: int = 0
    duration_seconds: int = 0
    is_reload: bool = False


@dataclass(frozen=True)
class ContextSwitchMetrics:
    """Metrics for context switching overhead."""

    total_switches: int
    total_loads: int
    total_retains: int
    switch_frequency: float
    avg_switch_cost_tokens: float
    avg_switch_duration_seconds: float
    cumulative_context_cost_tokens: int
    switch_efficiency_score: float
    reload_ratio: float
    retention_ratio: float


@dataclass(frozen=True)
class SessionContextSwitchCostAnalysis:
    """Complete analysis of context switch costs."""

    metrics: ContextSwitchMetrics
    switch_details: tuple[ContextSwitchDetail, ...]
    load_details: tuple[ContextLoadDetail, ...]
    insights: tuple[str, ...]
    total_session_tokens: int


def analyze_session_context_switch_cost(
    events: Sequence[ContextSwitchEvent],
    total_session_tokens: int = 0,
) -> SessionContextSwitchCostAnalysis:
    """Measure context switching overhead and costs.

    Args:
        events: Sequence of context switch events
        total_session_tokens: Total tokens used in session for efficiency calculation

    Returns:
        Analysis with context switch metrics, details, and insights
    """
    _validate_events(events)
    _validate_total_tokens(total_session_tokens)

    if not events:
        return SessionContextSwitchCostAnalysis(
            metrics=ContextSwitchMetrics(0, 0, 0, 0.0, 0.0, 0.0, 0, 0.0, 0.0, 0.0),
            switch_details=(),
            load_details=(),
            insights=("No events provided.",),
            total_session_tokens=total_session_tokens,
        )

    # Track context switches and loads
    switch_details: list[ContextSwitchDetail] = []
    load_details: list[ContextLoadDetail] = []
    retain_count = 0

    cumulative_cost = 0
    switch_costs: list[int] = []
    switch_durations: list[int] = []
    reload_count = 0
    total_loads = 0

    for event in events:
        if event.event_type == EVENT_CONTEXT_SWITCH:
            switch_details.append({
                "switch_id": event.event_id,
                "turn_index": event.turn_index,
                "from_context": event.from_context,
                "to_context": event.to_context,
                "switch_cost_tokens": event.cost_tokens,
                "switch_duration_seconds": event.duration_seconds,
            })
            cumulative_cost += event.cost_tokens
            switch_costs.append(event.cost_tokens)
            if event.duration_seconds > 0:
                switch_durations.append(event.duration_seconds)

        elif event.event_type == EVENT_CONTEXT_LOAD:
            load_details.append({
                "load_id": event.event_id,
                "turn_index": event.turn_index,
                "context_name": event.context_name,
                "load_cost_tokens": event.cost_tokens,
                "is_reload": event.is_reload,
            })
            cumulative_cost += event.cost_tokens
            total_loads += 1
            if event.is_reload:
                reload_count += 1

        elif event.event_type == EVENT_CONTEXT_RETAIN:
            retain_count += 1

    # Calculate metrics
    total_switches = len(switch_details)
    total_turns = events[-1].turn_index + 1 if events else 0
    switch_frequency = total_switches / total_turns if total_turns > 0 else 0.0

    avg_switch_cost = sum(switch_costs) / len(switch_costs) if switch_costs else 0.0
    avg_switch_duration = sum(switch_durations) / len(switch_durations) if switch_durations else 0.0

    # Switch efficiency: productive tokens / total tokens
    if total_session_tokens > 0:
        productive_tokens = total_session_tokens - cumulative_cost
        switch_efficiency = productive_tokens / total_session_tokens
    else:
        switch_efficiency = 1.0

    # Reload and retention ratios
    reload_ratio = reload_count / total_loads if total_loads > 0 else 0.0
    total_context_events = total_loads + retain_count
    retention_ratio = retain_count / total_context_events if total_context_events > 0 else 0.0

    metrics = ContextSwitchMetrics(
        total_switches=total_switches,
        total_loads=total_loads,
        total_retains=retain_count,
        switch_frequency=round(switch_frequency, 3),
        avg_switch_cost_tokens=round(avg_switch_cost, 1),
        avg_switch_duration_seconds=round(avg_switch_duration, 1),
        cumulative_context_cost_tokens=cumulative_cost,
        switch_efficiency_score=round(switch_efficiency, 3),
        reload_ratio=round(reload_ratio, 3),
        retention_ratio=round(retention_ratio, 3),
    )

    return SessionContextSwitchCostAnalysis(
        metrics=metrics,
        switch_details=tuple(switch_details),
        load_details=tuple(load_details),
        insights=_generate_insights(metrics, total_session_tokens),
        total_session_tokens=total_session_tokens,
    )


def _validate_events(events: Sequence[ContextSwitchEvent]) -> None:
    """Validate event sequence structure and content."""
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, ContextSwitchEvent):
            raise ValueError("events must contain ContextSwitchEvent instances")

        if event.event_type not in {EVENT_CONTEXT_SWITCH, EVENT_CONTEXT_LOAD, EVENT_CONTEXT_RETAIN}:
            raise ValueError(
                f"event at index {index} has invalid event_type: {event.event_type}"
            )

        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool):
            raise ValueError(f"turn_index at index {index} must be an integer")

        if event.turn_index < 0:
            raise ValueError(f"turn_index at index {index} must be non-negative")

        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")

        last_turn = event.turn_index

        if not isinstance(event.event_id, str) or not event.event_id.strip():
            raise ValueError(
                f"event at index {index} must have a non-empty event_id"
            )

        if not isinstance(event.cost_tokens, int) or isinstance(event.cost_tokens, bool):
            raise ValueError(f"cost_tokens at index {index} must be an integer")

        if event.cost_tokens < 0:
            raise ValueError(f"cost_tokens at index {index} must be non-negative")


def _validate_total_tokens(total_tokens: int) -> None:
    """Validate total session tokens value."""
    if not isinstance(total_tokens, int) or isinstance(total_tokens, bool):
        raise ValueError("total_session_tokens must be an integer")

    if total_tokens < 0:
        raise ValueError("total_session_tokens must be non-negative")


def _generate_insights(metrics: ContextSwitchMetrics, total_tokens: int) -> tuple[str, ...]:
    """Generate human-readable insights about context switch costs."""
    if metrics.total_switches == 0 and metrics.total_loads == 0:
        return ("No context switches or loads tracked in session.",)

    insights = [
        f"Detected {metrics.total_switches} context switch(es) and "
        f"{metrics.total_loads} context load(s)."
    ]

    if metrics.switch_frequency > 0.5:
        insights.append(
            f"High switch frequency ({metrics.switch_frequency:.2f} per turn). "
            "Frequent context switching may reduce efficiency."
        )

    if metrics.avg_switch_cost_tokens > DEFAULT_SWITCH_COST_THRESHOLD:
        insights.append(
            f"High average switch cost ({metrics.avg_switch_cost_tokens:.0f} tokens). "
            "Consider optimizing context transitions."
        )

    if total_tokens > 0:
        cost_percentage = (metrics.cumulative_context_cost_tokens / total_tokens) * 100
        insights.append(
            f"Context switching overhead: {metrics.cumulative_context_cost_tokens:,} tokens "
            f"({cost_percentage:.1f}% of session)."
        )

    if metrics.switch_efficiency_score < 0.8:
        insights.append(
            f"Low switch efficiency ({metrics.switch_efficiency_score:.1%}). "
            "Context switching consuming significant session resources."
        )
    elif metrics.switch_efficiency_score >= 0.9:
        insights.append(
            f"High switch efficiency ({metrics.switch_efficiency_score:.1%}). "
            "Context switching overhead is minimal."
        )

    if metrics.reload_ratio > 0.3:
        insights.append(
            f"High reload ratio ({metrics.reload_ratio:.1%}). "
            f"Contexts are frequently reloaded rather than retained."
        )

    if metrics.retention_ratio > 0.7:
        insights.append(
            f"Strong context retention ({metrics.retention_ratio:.1%}). "
            "Contexts are effectively maintained across turns."
        )

    return tuple(insights)
