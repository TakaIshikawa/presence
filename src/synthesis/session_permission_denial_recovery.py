"""Session permission denial recovery analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


EVENT_PERMISSION_DENIAL = "permission_denial"
EVENT_TOOL_SUCCESS = "tool_success"

QUALITY_NO_DENIALS = "no_denials"
QUALITY_RECOVERED = "recovered"
QUALITY_PARTIAL = "partial"
QUALITY_UNRECOVERED = "unrecovered"


@dataclass(frozen=True)
class SessionPermissionEvent:
    turn_index: int
    event_type: str
    tool_name: str
    command: str = ""


@dataclass(frozen=True)
class PermissionDenialRecoveryOutcome:
    denial_turn_index: int
    recovery_turn_index: int | None
    recovery_tool_name: str | None
    recovery_command: str | None
    recovery_latency_turns: int | None
    retried_same_command: bool


@dataclass(frozen=True)
class SessionPermissionDenialRecoveryMetrics:
    permission_denials: int
    recovered_denials: int
    unrecovered_denials: int
    retried_same_command: int
    average_turns_to_recovery: float


@dataclass(frozen=True)
class SessionPermissionDenialRecoveryAnalysis:
    metrics: SessionPermissionDenialRecoveryMetrics
    outcomes: tuple[PermissionDenialRecoveryOutcome, ...]
    quality: str
    insights: tuple[str, ...]


def analyze_session_permission_denial_recovery(
    events: Sequence[SessionPermissionEvent],
) -> SessionPermissionDenialRecoveryAnalysis:
    """Analyze whether denied permission events are followed by successful recovery."""

    _validate_events(events)
    outcomes: list[PermissionDenialRecoveryOutcome] = []
    for index, event in enumerate(events):
        if event.event_type != EVENT_PERMISSION_DENIAL:
            continue
        recovery = next(
            (
                later
                for later in events[index + 1 :]
                if later.event_type == EVENT_TOOL_SUCCESS
            ),
            None,
        )
        same_command = bool(
            recovery
            and event.command
            and recovery.command
            and _normalize_command(event.command) == _normalize_command(recovery.command)
        )
        outcomes.append(
            PermissionDenialRecoveryOutcome(
                denial_turn_index=event.turn_index,
                recovery_turn_index=recovery.turn_index if recovery else None,
                recovery_tool_name=recovery.tool_name if recovery else None,
                recovery_command=recovery.command if recovery else None,
                recovery_latency_turns=recovery.turn_index - event.turn_index
                if recovery
                else None,
                retried_same_command=same_command,
            )
        )

    latencies = [
        outcome.recovery_latency_turns
        for outcome in outcomes
        if outcome.recovery_latency_turns is not None
    ]
    metrics = SessionPermissionDenialRecoveryMetrics(
        permission_denials=len(outcomes),
        recovered_denials=sum(1 for outcome in outcomes if outcome.recovery_turn_index is not None),
        unrecovered_denials=sum(1 for outcome in outcomes if outcome.recovery_turn_index is None),
        retried_same_command=sum(1 for outcome in outcomes if outcome.retried_same_command),
        average_turns_to_recovery=round(sum(latencies) / len(latencies), 2)
        if latencies
        else 0.0,
    )
    return SessionPermissionDenialRecoveryAnalysis(
        metrics=metrics,
        outcomes=tuple(outcomes),
        quality=_classify_quality(metrics),
        insights=_generate_insights(metrics),
    )


def _validate_events(events: Sequence[SessionPermissionEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    for index, event in enumerate(events):
        if not isinstance(event, SessionPermissionEvent):
            raise ValueError("events must contain SessionPermissionEvent instances")
        if (
            not isinstance(event.turn_index, int)
            or isinstance(event.turn_index, bool)
            or event.turn_index < 0
        ):
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index <= last_turn:
            raise ValueError("turn_index values must be strictly increasing")
        if event.event_type not in {EVENT_PERMISSION_DENIAL, EVENT_TOOL_SUCCESS}:
            raise ValueError(f"unsupported event_type at position {index}: {event.event_type}")
        if not isinstance(event.tool_name, str) or not event.tool_name.strip():
            raise ValueError("tool_name must be a non-empty string")
        if not isinstance(event.command, str):
            raise ValueError("command must be a string")
        last_turn = event.turn_index


def _classify_quality(metrics: SessionPermissionDenialRecoveryMetrics) -> str:
    if metrics.permission_denials == 0:
        return QUALITY_NO_DENIALS
    if metrics.unrecovered_denials == 0:
        return QUALITY_RECOVERED
    if metrics.recovered_denials:
        return QUALITY_PARTIAL
    return QUALITY_UNRECOVERED


def _generate_insights(metrics: SessionPermissionDenialRecoveryMetrics) -> tuple[str, ...]:
    if metrics.permission_denials == 0:
        return ("No permission denial events supplied.",)
    insights = [
        f"{metrics.recovered_denials} of {metrics.permission_denials} permission denials recovered."
    ]
    if metrics.unrecovered_denials:
        insights.append(f"{metrics.unrecovered_denials} permission denials had no later successful tool call.")
    if metrics.retried_same_command:
        insights.append(f"{metrics.retried_same_command} recoveries retried the denied command.")
    return tuple(insights)


def _normalize_command(command: str) -> str:
    return " ".join(command.strip().split())
