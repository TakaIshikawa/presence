"""Session failure triage analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


DIAGNOSTIC_TYPES = {"read", "search"}
FAILURE_TYPES = {"command_failure", "test_failure", "failure"}


@dataclass(frozen=True)
class SessionFailureEvent:
    turn_index: int
    event_type: str
    command: str | None = None
    file_path: str | None = None
    failure_text: str | None = None


@dataclass(frozen=True)
class SessionFailureTriageReport:
    total_failures: int
    diagnostic_actions: int
    triaged_failures: int
    direct_retries_without_diagnostics: int
    abandoned_failures: int
    triage_quality: str
    insights: tuple[str, ...]


def analyze_session_failure_triage(events: Sequence[SessionFailureEvent]) -> SessionFailureTriageReport:
    _validate_events(events)
    failures = [event for event in events if event.event_type in FAILURE_TYPES]
    diagnostic_actions = sum(1 for event in events if event.event_type in DIAGNOSTIC_TYPES)
    triaged = 0
    retries = 0
    abandoned = 0

    for index, failure in enumerate(events):
        if failure.event_type not in FAILURE_TYPES:
            continue
        saw_diagnostic = False
        resolved = False
        for later in events[index + 1 :]:
            if later.event_type in FAILURE_TYPES:
                break
            if later.event_type in DIAGNOSTIC_TYPES:
                saw_diagnostic = True
            if later.event_type in {"command", "test"} and _same_command(failure.command, later.command):
                if saw_diagnostic:
                    triaged += 1
                else:
                    retries += 1
                resolved = True
                break
        if not resolved:
            abandoned += 1

    quality = _quality(len(failures), triaged, retries, abandoned)
    return SessionFailureTriageReport(
        total_failures=len(failures),
        diagnostic_actions=diagnostic_actions,
        triaged_failures=triaged,
        direct_retries_without_diagnostics=retries,
        abandoned_failures=abandoned,
        triage_quality=quality,
        insights=_insights(len(failures), triaged, retries, abandoned),
    )


def _validate_events(events: Sequence[SessionFailureEvent]) -> None:
    if not isinstance(events, (list, tuple)):
        raise ValueError("events must be a list or tuple")
    last_turn = -1
    allowed = FAILURE_TYPES | DIAGNOSTIC_TYPES | {"command", "test"}
    for index, event in enumerate(events):
        if not isinstance(event, SessionFailureEvent):
            raise ValueError(f"events[{index}] must be a SessionFailureEvent")
        if not isinstance(event.turn_index, int) or isinstance(event.turn_index, bool) or event.turn_index < 0:
            raise ValueError("turn_index must be a non-negative integer")
        if event.turn_index < last_turn:
            raise ValueError("events must be ordered by turn_index")
        if event.event_type not in allowed:
            raise ValueError("event_type is not supported")
        for field_name, value in (("command", event.command), ("file_path", event.file_path), ("failure_text", event.failure_text)):
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{field_name} must be a string or None")
        if event.event_type in FAILURE_TYPES and not (event.command or event.failure_text):
            raise ValueError("failure events require command or failure_text")
        last_turn = event.turn_index


def _same_command(left: str | None, right: str | None) -> bool:
    return bool(left and right and " ".join(left.split()) == " ".join(right.split()))


def _quality(total: int, triaged: int, retries: int, abandoned: int) -> str:
    if total == 0:
        return "clean"
    if triaged == total:
        return "strong"
    if triaged and triaged >= retries + abandoned:
        return "partial"
    return "weak"


def _insights(total: int, triaged: int, retries: int, abandoned: int) -> tuple[str, ...]:
    if total == 0:
        return ("No failed commands or tests detected.",)
    insights = [f"Triaged {triaged} of {total} failure(s) with diagnostics before retry."]
    if retries:
        insights.append(f"{retries} failure(s) were retried directly without diagnostics.")
    if abandoned:
        insights.append(f"{abandoned} failure(s) were abandoned without retry.")
    return tuple(insights)
