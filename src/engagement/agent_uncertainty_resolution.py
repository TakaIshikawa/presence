"""Agent uncertainty resolution analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


SEVERITY_LOW = "low"
SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"

QUALITY_NO_UNCERTAINTY = "no_uncertainty"
QUALITY_STRONG = "strong"
QUALITY_MODERATE = "moderate"
QUALITY_WEAK = "weak"


@dataclass(frozen=True)
class UncertaintyEvent:
    turn_index: int
    uncertainty_type: str
    severity: str
    resolved_turn_index: int | None = None
    resolution_source: str = ""


@dataclass(frozen=True)
class AgentUncertaintyResolutionMetrics:
    total_uncertainties: int
    resolved_count: int
    unresolved_count: int
    resolution_rate: float
    average_resolution_latency: float
    high_severity_unresolved_count: int
    source_distribution: tuple[tuple[str, int], ...]


@dataclass(frozen=True)
class AgentUncertaintyResolutionAnalysis:
    metrics: AgentUncertaintyResolutionMetrics
    quality: str
    insights: tuple[str, ...]


def analyze_agent_uncertainty_resolution(
    events: Sequence[UncertaintyEvent],
) -> AgentUncertaintyResolutionAnalysis:
    """Analyze whether explicit uncertainty is resolved with evidence."""

    _validate_events(events)
    if not events:
        metrics = AgentUncertaintyResolutionMetrics(0, 0, 0, 0.0, 0.0, 0, ())
        return AgentUncertaintyResolutionAnalysis(
            metrics, QUALITY_NO_UNCERTAINTY, ("No uncertainty events supplied.",)
        )

    resolved = [event for event in events if event.resolved_turn_index is not None]
    unresolved = len(events) - len(resolved)
    latencies = [event.resolved_turn_index - event.turn_index for event in resolved]
    sources: dict[str, int] = {}
    for event in resolved:
        sources[event.resolution_source] = sources.get(event.resolution_source, 0) + 1
    metrics = AgentUncertaintyResolutionMetrics(
        total_uncertainties=len(events),
        resolved_count=len(resolved),
        unresolved_count=unresolved,
        resolution_rate=round(len(resolved) / len(events), 3),
        average_resolution_latency=round(sum(latencies) / len(latencies), 2)
        if latencies
        else 0.0,
        high_severity_unresolved_count=sum(
            1
            for event in events
            if event.severity == SEVERITY_HIGH and event.resolved_turn_index is None
        ),
        source_distribution=tuple(sorted(sources.items())),
    )
    return AgentUncertaintyResolutionAnalysis(
        metrics,
        _classify_quality(metrics),
        _generate_insights(metrics),
    )


def _validate_events(events: Sequence[UncertaintyEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    valid_severities = {SEVERITY_LOW, SEVERITY_MEDIUM, SEVERITY_HIGH}
    last_index = -1
    for event in events:
        if not isinstance(event, UncertaintyEvent):
            raise ValueError("events must contain UncertaintyEvent instances")
        if not isinstance(event.turn_index, int) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_index:
            raise ValueError("turn_index values must be ordered")
        if not isinstance(event.uncertainty_type, str) or not event.uncertainty_type.strip():
            raise ValueError("uncertainty_type must be a non-empty string")
        if event.severity not in valid_severities:
            raise ValueError(f"unsupported severity: {event.severity}")
        if event.resolved_turn_index is not None:
            if (
                not isinstance(event.resolved_turn_index, int)
                or event.resolved_turn_index < event.turn_index
            ):
                raise ValueError("resolved_turn_index must be at or after turn_index")
            if not isinstance(event.resolution_source, str) or not event.resolution_source.strip():
                raise ValueError("resolution_source must be a non-empty string")
        elif not isinstance(event.resolution_source, str) or event.resolution_source:
            raise ValueError("unresolved events must use an empty resolution_source string")
        last_index = event.turn_index


def _classify_quality(metrics: AgentUncertaintyResolutionMetrics) -> str:
    if metrics.total_uncertainties == 0:
        return QUALITY_NO_UNCERTAINTY
    if metrics.high_severity_unresolved_count:
        return QUALITY_WEAK
    if metrics.resolution_rate >= 0.8:
        return QUALITY_STRONG
    if metrics.resolution_rate >= 0.5:
        return QUALITY_MODERATE
    return QUALITY_WEAK


def _generate_insights(
    metrics: AgentUncertaintyResolutionMetrics,
) -> tuple[str, ...]:
    if metrics.total_uncertainties == 0:
        return ("No uncertainty events supplied.",)
    insights = [
        f"Resolved {metrics.resolved_count} of {metrics.total_uncertainties} uncertainties ({metrics.resolution_rate:.1%})."
    ]
    if metrics.high_severity_unresolved_count:
        insights.append(
            f"{metrics.high_severity_unresolved_count} high-severity uncertainties remained unresolved."
        )
    if metrics.source_distribution:
        source, count = max(metrics.source_distribution, key=lambda item: item[1])
        insights.append(f"Most common resolution source: {source} ({count}).")
    return tuple(insights)
