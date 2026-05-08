"""Session verification outcome analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


EVENT_IMPLEMENTATION = "implementation"
EVENT_VERIFICATION = "verification"

STATUS_PASS = "pass"
STATUS_FAIL = "fail"

QUALITY_NO_IMPLEMENTATION = "no_implementation"
QUALITY_VERIFIED = "verified"
QUALITY_RECOVERED = "recovered"
QUALITY_UNRESOLVED = "unresolved"


@dataclass(frozen=True)
class SessionVerificationEvent:
    turn_index: int
    event_type: str
    command: str = ""
    status: str = ""


@dataclass(frozen=True)
class ImplementationVerificationOutcome:
    implementation_turn_index: int
    first_verification_turn_index: int | None
    first_verification_status: str | None
    final_verification_status: str | None
    verification_latency_turns: int | None


@dataclass(frozen=True)
class SessionVerificationOutcomeMetrics:
    implemented_changes: int
    verification_attempts: int
    passing_verifications: int
    failing_verifications: int
    unresolved_failures: int
    recovered_failures: int
    average_turns_to_first_verification: float


@dataclass(frozen=True)
class SessionVerificationOutcomeAnalysis:
    metrics: SessionVerificationOutcomeMetrics
    outcomes: tuple[ImplementationVerificationOutcome, ...]
    quality: str
    insights: tuple[str, ...]


def analyze_session_verification_outcome(
    events: Sequence[SessionVerificationEvent],
) -> SessionVerificationOutcomeAnalysis:
    """Analyze whether implementation clusters receive passing verification."""

    _validate_events(events)
    implementation_indexes = [
        index for index, event in enumerate(events) if event.event_type == EVENT_IMPLEMENTATION
    ]
    if not implementation_indexes:
        metrics = SessionVerificationOutcomeMetrics(0, 0, 0, 0, 0, 0, 0.0)
        return SessionVerificationOutcomeAnalysis(
            metrics,
            (),
            QUALITY_NO_IMPLEMENTATION,
            ("No implementation events supplied.",),
        )

    outcomes: list[ImplementationVerificationOutcome] = []
    for number, event_index in enumerate(implementation_indexes):
        start = event_index + 1
        end = (
            implementation_indexes[number + 1]
            if number + 1 < len(implementation_indexes)
            else len(events)
        )
        implementation = events[event_index]
        verifications = [
            event
            for event in events[start:end]
            if event.event_type == EVENT_VERIFICATION
        ]
        first = verifications[0] if verifications else None
        final_status = None
        if any(event.status == STATUS_PASS for event in verifications):
            final_status = STATUS_PASS
        elif verifications:
            final_status = STATUS_FAIL
        outcomes.append(
            ImplementationVerificationOutcome(
                implementation_turn_index=implementation.turn_index,
                first_verification_turn_index=first.turn_index if first else None,
                first_verification_status=first.status if first else None,
                final_verification_status=final_status,
                verification_latency_turns=first.turn_index - implementation.turn_index
                if first
                else None,
            )
        )

    attempts = [event for event in events if event.event_type == EVENT_VERIFICATION]
    passing = sum(1 for event in attempts if event.status == STATUS_PASS)
    failing = sum(1 for event in attempts if event.status == STATUS_FAIL)
    unresolved = sum(
        1 for outcome in outcomes if outcome.final_verification_status == STATUS_FAIL
    )
    recovered = sum(
        1
        for outcome in outcomes
        if outcome.first_verification_status == STATUS_FAIL
        and outcome.final_verification_status == STATUS_PASS
    )
    latencies = [
        outcome.verification_latency_turns
        for outcome in outcomes
        if outcome.verification_latency_turns is not None
    ]
    metrics = SessionVerificationOutcomeMetrics(
        implemented_changes=len(outcomes),
        verification_attempts=len(attempts),
        passing_verifications=passing,
        failing_verifications=failing,
        unresolved_failures=unresolved,
        recovered_failures=recovered,
        average_turns_to_first_verification=round(sum(latencies) / len(latencies), 2)
        if latencies
        else 0.0,
    )
    return SessionVerificationOutcomeAnalysis(
        metrics,
        tuple(outcomes),
        _classify_quality(metrics),
        _generate_insights(metrics),
    )


def _validate_events(events: Sequence[SessionVerificationEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_index = -1
    for position, event in enumerate(events):
        if not isinstance(event, SessionVerificationEvent):
            raise ValueError("events must contain SessionVerificationEvent instances")
        if not isinstance(event.turn_index, int) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index <= last_index:
            raise ValueError("turn_index values must be strictly increasing")
        if event.event_type not in {EVENT_IMPLEMENTATION, EVENT_VERIFICATION}:
            raise ValueError(
                f"event at position {position} has unsupported event_type: {event.event_type}"
            )
        if not isinstance(event.command, str):
            raise ValueError("command must be a string")
        if event.event_type == EVENT_VERIFICATION:
            if not event.command.strip():
                raise ValueError("verification events require a command")
            if event.status not in {STATUS_PASS, STATUS_FAIL}:
                raise ValueError(f"unsupported verification status: {event.status}")
        elif event.status:
            raise ValueError("implementation events must not have a status")
        last_index = event.turn_index


def _classify_quality(metrics: SessionVerificationOutcomeMetrics) -> str:
    if metrics.implemented_changes == 0:
        return QUALITY_NO_IMPLEMENTATION
    if metrics.unresolved_failures:
        return QUALITY_UNRESOLVED
    if metrics.recovered_failures:
        return QUALITY_RECOVERED
    return QUALITY_VERIFIED if metrics.passing_verifications else QUALITY_UNRESOLVED


def _generate_insights(
    metrics: SessionVerificationOutcomeMetrics,
) -> tuple[str, ...]:
    if metrics.implemented_changes == 0:
        return ("No implementation events supplied.",)
    insights = [
        f"{metrics.passing_verifications} passing and {metrics.failing_verifications} failing verification attempts recorded."
    ]
    if metrics.recovered_failures:
        insights.append(f"{metrics.recovered_failures} failed verification paths recovered later.")
    if metrics.unresolved_failures:
        insights.append(f"{metrics.unresolved_failures} implementation clusters ended with unresolved failures.")
    return tuple(insights)
