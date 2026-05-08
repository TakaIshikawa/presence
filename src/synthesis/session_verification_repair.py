"""Session verification repair analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import median
from typing import Sequence


EVENT_VERIFICATION = "verification"
EVENT_EDIT = "edit"
EVENT_REPAIR = "repair"
STATUS_FAILED = "failed"
STATUS_PASSED = "passed"
STATUS_ATTEMPTED = "attempted"


@dataclass(frozen=True)
class VerificationRepairEvent:
    event_type: str
    status: str
    turn_index: int
    command: str | None = None
    summary: str | None = None


@dataclass(frozen=True)
class UnresolvedVerificationExample:
    turn_index: int
    command: str | None
    summary: str | None


@dataclass(frozen=True)
class SessionVerificationRepairMetrics:
    total_failures: int
    repaired_failures: int
    unresolved_failures: int
    repair_rate: float
    median_repair_latency: float


@dataclass(frozen=True)
class SessionVerificationRepair:
    metrics: SessionVerificationRepairMetrics
    unresolved_examples: tuple[UnresolvedVerificationExample, ...]
    insights: tuple[str, ...]


def analyze_session_verification_repair(
    events: Sequence[VerificationRepairEvent],
) -> SessionVerificationRepair:
    """Measure whether failed verification is repaired and later passes."""

    _validate_events(events)
    latencies: list[int] = []
    unresolved_examples: list[UnresolvedVerificationExample] = []
    total_failures = 0
    repaired_failures = 0
    consumed_pass_indices: set[int] = set()

    for index, event in enumerate(events):
        if event.event_type != EVENT_VERIFICATION or event.status != STATUS_FAILED:
            continue
        total_failures += 1
        repair_index = _first_repair_index(events, index)
        pass_index = (
            _first_passing_verification_index(events, repair_index, consumed_pass_indices)
            if repair_index is not None
            else None
        )
        if pass_index is None:
            if len(unresolved_examples) < 5:
                unresolved_examples.append(
                    UnresolvedVerificationExample(
                        event.turn_index,
                        event.command,
                        event.summary,
                    )
                )
            continue
        consumed_pass_indices.add(pass_index)
        pass_event = events[pass_index]
        repaired_failures += 1
        latencies.append(pass_event.turn_index - event.turn_index)

    unresolved = total_failures - repaired_failures
    repair_rate = repaired_failures / total_failures if total_failures else 0.0
    median_latency = float(median(latencies)) if latencies else 0.0
    metrics = SessionVerificationRepairMetrics(
        total_failures=total_failures,
        repaired_failures=repaired_failures,
        unresolved_failures=unresolved,
        repair_rate=round(repair_rate, 3),
        median_repair_latency=round(median_latency, 2),
    )
    return SessionVerificationRepair(
        metrics=metrics,
        unresolved_examples=tuple(unresolved_examples),
        insights=_verification_repair_insights(metrics),
    )


def _validate_events(events: Sequence[VerificationRepairEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    for event in events:
        if not isinstance(event, VerificationRepairEvent):
            raise ValueError("events must contain VerificationRepairEvent instances")
        if event.event_type not in {EVENT_VERIFICATION, EVENT_EDIT, EVENT_REPAIR}:
            raise ValueError("event_type must be 'verification', 'edit', or 'repair'")
        if event.status not in {STATUS_FAILED, STATUS_PASSED, STATUS_ATTEMPTED}:
            raise ValueError("status must be 'failed', 'passed', or 'attempted'")
        if event.event_type == EVENT_VERIFICATION and event.status == STATUS_ATTEMPTED:
            raise ValueError("verification events must be failed or passed")
        if event.event_type in {EVENT_EDIT, EVENT_REPAIR} and event.status != STATUS_ATTEMPTED:
            raise ValueError("edit and repair events must have attempted status")
        if (
            not isinstance(event.turn_index, int)
            or isinstance(event.turn_index, bool)
            or event.turn_index < 0
        ):
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")
        if event.command is not None and not isinstance(event.command, str):
            raise ValueError("command must be a string or None")
        if isinstance(event.command, str) and not event.command.strip():
            raise ValueError("command must be non-empty when provided")
        if event.summary is not None and not isinstance(event.summary, str):
            raise ValueError("summary must be a string or None")
        if isinstance(event.summary, str) and not event.summary.strip():
            raise ValueError("summary must be non-empty when provided")
        last_turn = event.turn_index


def _first_repair_index(
    events: Sequence[VerificationRepairEvent],
    failure_index: int,
) -> int | None:
    for later_index, later in enumerate(events[failure_index + 1 :], start=failure_index + 1):
        if later.event_type in {EVENT_EDIT, EVENT_REPAIR}:
            return later_index
    return None


def _first_passing_verification_index(
    events: Sequence[VerificationRepairEvent],
    repair_index: int | None,
    consumed_indices: set[int],
) -> int | None:
    if repair_index is None:
        return None
    for later_index in range(repair_index + 1, len(events)):
        later = events[later_index]
        if (
            later_index not in consumed_indices
            and later.event_type == EVENT_VERIFICATION
            and later.status == STATUS_PASSED
        ):
            return later_index
    return None


def _verification_repair_insights(
    metrics: SessionVerificationRepairMetrics,
) -> tuple[str, ...]:
    if metrics.total_failures == 0:
        return ("No failed verification attempts detected.",)
    insights = [
        f"Repaired {metrics.repaired_failures} of {metrics.total_failures} failed "
        f"verification attempts ({metrics.repair_rate:.1%})."
    ]
    if metrics.unresolved_failures:
        insights.append(
            f"{metrics.unresolved_failures} failed verification attempts remain unresolved."
        )
    return tuple(insights)
