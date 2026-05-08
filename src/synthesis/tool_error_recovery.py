"""Tool error recovery analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


STATUS_ERROR = "error"
STATUS_SUCCESS = "success"


@dataclass(frozen=True)
class ToolEvent:
    tool_name: str
    status: str
    turn_index: int
    error_message: str | None = None


@dataclass(frozen=True)
class ToolErrorRecoveryMetrics:
    total_events: int
    tool_errors: int
    successful_retries: int
    abandoned_errors: int
    recovery_rate: float
    average_recovery_latency: float


@dataclass(frozen=True)
class ToolErrorRecovery:
    metrics: ToolErrorRecoveryMetrics
    repeated_failing_tools: tuple[str, ...]
    recovery_quality: str
    insights: tuple[str, ...]


def analyze_tool_error_recovery(events: Sequence[ToolEvent]) -> ToolErrorRecovery:
    """Measure recovery behavior after failed tool calls."""

    _validate_events(events)
    errors = [event for event in events if event.status == STATUS_ERROR]
    latencies: list[int] = []
    recovered_error_ids: set[int] = set()
    for error_index, event in enumerate(events):
        if event.status != STATUS_ERROR:
            continue
        for later in events[error_index + 1 :]:
            if later.tool_name == event.tool_name and later.status == STATUS_SUCCESS:
                latencies.append(later.turn_index - event.turn_index)
                recovered_error_ids.add(error_index)
                break

    failing_counts: dict[str, int] = {}
    for error in errors:
        failing_counts[error.tool_name] = failing_counts.get(error.tool_name, 0) + 1
    repeated_tools = tuple(sorted(tool for tool, count in failing_counts.items() if count > 1))
    recovered = len(recovered_error_ids)
    abandoned = len(errors) - recovered
    recovery_rate = recovered / len(errors) if errors else 1.0
    average_latency = sum(latencies) / len(latencies) if latencies else 0.0
    metrics = ToolErrorRecoveryMetrics(
        total_events=len(events),
        tool_errors=len(errors),
        successful_retries=recovered,
        abandoned_errors=abandoned,
        recovery_rate=round(recovery_rate, 3),
        average_recovery_latency=round(average_latency, 2),
    )
    quality = "clean" if not errors else "strong" if recovery_rate >= 0.8 else "partial" if recovery_rate >= 0.5 else "poor"
    return ToolErrorRecovery(metrics, repeated_tools, quality, _tool_error_insights(metrics, repeated_tools))


def _validate_events(events: Sequence[ToolEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    for event in events:
        if not isinstance(event, ToolEvent):
            raise ValueError("events must contain ToolEvent instances")
        if not isinstance(event.tool_name, str) or not event.tool_name.strip():
            raise ValueError("tool_name must be a non-empty string")
        if event.status not in {STATUS_ERROR, STATUS_SUCCESS}:
            raise ValueError("status must be 'error' or 'success'")
        if not isinstance(event.turn_index, int) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")
        if event.error_message is not None and not isinstance(event.error_message, str):
            raise ValueError("error_message must be a string or None")
        last_turn = event.turn_index


def _tool_error_insights(
    metrics: ToolErrorRecoveryMetrics,
    repeated_tools: tuple[str, ...],
) -> tuple[str, ...]:
    if metrics.tool_errors == 0:
        return ("No tool errors detected.",)
    insights = [
        f"Recovered {metrics.successful_retries} of {metrics.tool_errors} tool errors "
        f"({metrics.recovery_rate:.1%})."
    ]
    if metrics.abandoned_errors:
        insights.append(f"{metrics.abandoned_errors} tool errors were abandoned.")
    if repeated_tools:
        insights.append("Repeated failures came from: " + ", ".join(repeated_tools) + ".")
    return tuple(insights)
