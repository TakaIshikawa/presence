"""Agent uncertainty resolution analyzer.

Measures whether explicit uncertainty in an agent session is resolved through
later evidence-gathering actions.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any, Sequence


SEVERITY_LOW = "low"
SEVERITY_MEDIUM = "medium"
SEVERITY_HIGH = "high"
SEVERITY_CRITICAL = "critical"

QUALITY_NO_UNCERTAINTIES = "no_uncertainties"
QUALITY_NO_UNCERTAINTY = QUALITY_NO_UNCERTAINTIES
QUALITY_STRONG = "strong"
QUALITY_MODERATE = "moderate"
QUALITY_WEAK = "weak"
QUALITY_CRITICAL = "critical"

SUPPORTED_SEVERITIES = {
    SEVERITY_LOW,
    SEVERITY_MEDIUM,
    SEVERITY_HIGH,
    SEVERITY_CRITICAL,
}
HIGH_SEVERITIES = {SEVERITY_HIGH, SEVERITY_CRITICAL}


@dataclass(frozen=True)
class UncertaintyEvent:
    """One explicit uncertainty raised during an agent session."""

    turn_index: int
    uncertainty_type: str
    severity: str
    resolved_turn_index: int | None = None
    resolution_source: str | None = None


@dataclass(frozen=True, eq=False)
class ResolutionSourceCount:
    """Count of resolved uncertainties by evidence source."""

    source: str
    count: int

    def __iter__(self):
        yield self.source
        yield self.count

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, ResolutionSourceCount):
            return (self.source, self.count) == (other.source, other.count)
        if isinstance(other, tuple):
            return (self.source, self.count) == other
        return False


@dataclass(frozen=True)
class AgentUncertaintyResolutionMetrics:
    """Aggregate uncertainty resolution metrics."""

    total_uncertainties: int
    resolved_count: int
    unresolved_count: int
    resolution_rate: float
    average_resolution_latency: float
    high_severity_unresolved_count: int
    source_distribution: tuple[ResolutionSourceCount, ...]


@dataclass(frozen=True)
class AgentUncertaintyResolution:
    """Complete uncertainty resolution analysis."""

    metrics: AgentUncertaintyResolutionMetrics
    quality_tier: str
    insights: tuple[str, ...]

    @property
    def quality(self) -> str:
        """Compatibility alias for callers that use ``quality``."""

        return self.quality_tier


AgentUncertaintyResolutionAnalysis = AgentUncertaintyResolution


def analyze_agent_uncertainty_resolution(
    events: Sequence[UncertaintyEvent],
) -> AgentUncertaintyResolution:
    """Analyze whether explicit uncertainty is resolved with evidence.

    A resolved uncertainty must include both ``resolved_turn_index`` and a
    non-empty ``resolution_source``. Resolution latency is the turn distance
    from ``turn_index`` to ``resolved_turn_index``.
    """

    _validate_events(events)

    if not events:
        metrics = AgentUncertaintyResolutionMetrics(
            total_uncertainties=0,
            resolved_count=0,
            unresolved_count=0,
            resolution_rate=0.0,
            average_resolution_latency=0.0,
            high_severity_unresolved_count=0,
            source_distribution=(),
        )
        return AgentUncertaintyResolution(
            metrics=metrics,
            quality_tier=QUALITY_NO_UNCERTAINTIES,
            insights=("No uncertainty events supplied - nothing to resolve.",),
        )

    resolved_events = [event for event in events if _is_resolved(event)]
    unresolved_events = [event for event in events if not _is_resolved(event)]
    latencies = [
        event.resolved_turn_index - event.turn_index
        for event in resolved_events
        if event.resolved_turn_index is not None
    ]
    source_counts = Counter(
        event.resolution_source for event in resolved_events if event.resolution_source
    )
    source_distribution = tuple(
        ResolutionSourceCount(source=source, count=count)
        for source, count in sorted(source_counts.items())
    )
    high_unresolved = sum(
        1 for event in unresolved_events if event.severity in HIGH_SEVERITIES
    )

    metrics = AgentUncertaintyResolutionMetrics(
        total_uncertainties=len(events),
        resolved_count=len(resolved_events),
        unresolved_count=len(unresolved_events),
        resolution_rate=round(len(resolved_events) / len(events), 3),
        average_resolution_latency=round(sum(latencies) / len(latencies), 2)
        if latencies
        else 0.0,
        high_severity_unresolved_count=high_unresolved,
        source_distribution=source_distribution,
    )
    quality = _classify_quality(metrics)

    return AgentUncertaintyResolution(
        metrics=metrics,
        quality_tier=quality,
        insights=_generate_insights(metrics, quality),
    )


def _validate_events(events: Sequence[UncertaintyEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")

    last_index = -1
    for position, event in enumerate(events):
        if not isinstance(event, UncertaintyEvent):
            raise ValueError("events must contain UncertaintyEvent instances")
        if not isinstance(event.turn_index, int) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_index:
            raise ValueError("turn_index values must be ordered")
        if not isinstance(event.uncertainty_type, str) or not event.uncertainty_type.strip():
            raise ValueError("uncertainty_type must be a non-empty string")
        if event.severity not in SUPPORTED_SEVERITIES:
            raise ValueError(
                f"event at position {position} has unsupported severity: {event.severity}"
            )
        if event.resolved_turn_index is not None:
            if (
                not isinstance(event.resolved_turn_index, int)
                or event.resolved_turn_index < 0
            ):
                raise ValueError("resolved_turn_index must be a non-negative integer")
            if event.resolved_turn_index < event.turn_index:
                raise ValueError(
                    "resolved_turn_index must be greater than or equal to turn_index"
                )
        if event.resolution_source is not None:
            if not isinstance(event.resolution_source, str):
                raise ValueError("resolution_source must be a string when provided")
            if event.resolution_source.strip() == "":
                raise ValueError("resolution_source must be non-empty when provided")
        if (event.resolved_turn_index is None) != (event.resolution_source is None):
            raise ValueError(
                "resolved_turn_index and resolution_source must be provided together"
            )
        last_index = event.turn_index


def _is_resolved(event: UncertaintyEvent) -> bool:
    return event.resolved_turn_index is not None and event.resolution_source is not None


def _classify_quality(metrics: AgentUncertaintyResolutionMetrics) -> str:
    if metrics.total_uncertainties == 0:
        return QUALITY_NO_UNCERTAINTIES
    if metrics.high_severity_unresolved_count:
        return QUALITY_CRITICAL
    if metrics.resolution_rate >= 0.9:
        return QUALITY_STRONG
    if metrics.resolution_rate >= 0.6:
        return QUALITY_MODERATE
    return QUALITY_WEAK


def _generate_insights(
    metrics: AgentUncertaintyResolutionMetrics,
    quality: str,
) -> tuple[str, ...]:
    if quality == QUALITY_NO_UNCERTAINTIES:
        return ("No explicit uncertainties were detected in the session.",)

    insights = [
        f"Resolved {metrics.resolved_count} of {metrics.total_uncertainties} "
        f"uncertainties ({metrics.resolution_rate:.1%})."
    ]
    if metrics.unresolved_count:
        insights.append(
            f"{metrics.unresolved_count} uncertainties remained unresolved at session end."
        )
    if metrics.high_severity_unresolved_count:
        insights.append(
            f"{metrics.high_severity_unresolved_count} high-severity uncertainties "
            "remained unresolved and require evidence before closure."
        )
    if metrics.source_distribution:
        dominant = max(
            metrics.source_distribution,
            key=lambda item: (item.count, item.source),
        )
        insights.append(
            f"Most resolutions came from {dominant.source} "
            f"({dominant.count} resolved uncertainties)."
        )
    if metrics.average_resolution_latency:
        insights.append(
            f"Average resolution latency was "
            f"{metrics.average_resolution_latency:.2f} turns."
        )
    if quality == QUALITY_WEAK:
        insights.append(
            "Low resolution rate indicates uncertainty is being recorded without "
            "enough follow-up evidence."
        )
    return tuple(insights)
