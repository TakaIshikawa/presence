"""Session command repetition analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class SessionCommandRecord:
    turn_index: int
    command: str


@dataclass(frozen=True)
class SessionCommandRepetitionMetrics:
    total_commands: int
    unique_commands: int
    repeated_commands: int
    repeat_rate: float


@dataclass(frozen=True)
class RepeatedCommandExample:
    command: str
    first_turn: int
    repeated_turn: int
    repeat_count: int


@dataclass(frozen=True)
class SessionCommandRepetitionReport:
    metrics: SessionCommandRepetitionMetrics
    repeated_examples: tuple[RepeatedCommandExample, ...]


def analyze_session_command_repetition(
    records: Sequence[SessionCommandRecord],
) -> SessionCommandRepetitionReport:
    """Detect repeated shell commands within a session."""

    _validate_records(records)
    if not records:
        return SessionCommandRepetitionReport(
            SessionCommandRepetitionMetrics(0, 0, 0, 0.0),
            (),
        )

    first_turns: dict[str, int] = {}
    repeat_counts: dict[str, int] = {}
    examples: list[RepeatedCommandExample] = []

    for record in records:
        normalized = _normalize_command(record.command)
        if normalized not in first_turns:
            first_turns[normalized] = record.turn_index
            continue

        repeat_counts[normalized] = repeat_counts.get(normalized, 1) + 1
        if len(examples) < 5:
            examples.append(
                RepeatedCommandExample(
                    command=normalized,
                    first_turn=first_turns[normalized],
                    repeated_turn=record.turn_index,
                    repeat_count=repeat_counts[normalized],
                )
            )

    repeated_commands = sum(count - 1 for count in repeat_counts.values())
    metrics = SessionCommandRepetitionMetrics(
        total_commands=len(records),
        unique_commands=len(first_turns),
        repeated_commands=repeated_commands,
        repeat_rate=round(repeated_commands / len(records), 3),
    )
    return SessionCommandRepetitionReport(metrics, tuple(examples))


def _validate_records(records: Sequence[SessionCommandRecord]) -> None:
    if not isinstance(records, (list, tuple)):
        raise ValueError("records must be a list or tuple of SessionCommandRecord instances")

    last_turn = -1
    for index, record in enumerate(records):
        if not isinstance(record, SessionCommandRecord):
            raise ValueError(f"records[{index}] must be a SessionCommandRecord")
        if (
            not isinstance(record.turn_index, int)
            or isinstance(record.turn_index, bool)
            or record.turn_index < 0
        ):
            raise ValueError("turn_index must be a non-negative integer")
        if record.turn_index < last_turn:
            raise ValueError("records must be ordered by turn_index")
        if not isinstance(record.command, str) or not record.command.strip():
            raise ValueError("command must be a non-empty string")
        last_turn = record.turn_index


def _normalize_command(command: str) -> str:
    return " ".join(command.lower().split())
