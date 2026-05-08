"""Session prompt drift analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


DRIFT_TOPIC_CHANGE = "topic_change"
DRIFT_INSTRUCTION_CHANGE = "instruction_change"


@dataclass(frozen=True)
class PromptInstructionRecord:
    topic: str
    instruction: str
    turn_index: int
    prompt_id: str | None = None


@dataclass(frozen=True)
class PromptDriftExample:
    previous_turn: int
    current_turn: int
    labels: tuple[str, ...]
    previous_topic: str
    current_topic: str
    previous_instruction: str
    current_instruction: str


@dataclass(frozen=True)
class SessionPromptDriftMetrics:
    total_records: int
    comparable_transitions: int
    drift_events: int
    topic_changes: int
    instruction_changes: int
    drift_rate: float


@dataclass(frozen=True)
class SessionPromptDrift:
    metrics: SessionPromptDriftMetrics
    most_frequent_drift_labels: tuple[str, ...]
    examples: tuple[PromptDriftExample, ...]
    insights: tuple[str, ...]


def analyze_session_prompt_drift(
    records: Sequence[PromptInstructionRecord],
) -> SessionPromptDrift:
    """Measure topic and instruction drift across ordered prompt records."""

    _validate_records(records)
    label_counts: dict[str, int] = {}
    examples: list[PromptDriftExample] = []
    drift_events = 0

    for previous, current in zip(records, records[1:]):
        labels = _drift_labels(previous, current)
        if not labels:
            continue
        drift_events += 1
        for label in labels:
            label_counts[label] = label_counts.get(label, 0) + 1
        if len(examples) < 5:
            examples.append(
                PromptDriftExample(
                    previous_turn=previous.turn_index,
                    current_turn=current.turn_index,
                    labels=labels,
                    previous_topic=previous.topic,
                    current_topic=current.topic,
                    previous_instruction=previous.instruction,
                    current_instruction=current.instruction,
                )
            )

    transitions = max(len(records) - 1, 0)
    drift_rate = drift_events / transitions if transitions else 0.0
    metrics = SessionPromptDriftMetrics(
        total_records=len(records),
        comparable_transitions=transitions,
        drift_events=drift_events,
        topic_changes=label_counts.get(DRIFT_TOPIC_CHANGE, 0),
        instruction_changes=label_counts.get(DRIFT_INSTRUCTION_CHANGE, 0),
        drift_rate=round(drift_rate, 3),
    )
    labels = tuple(
        label
        for label, _count in sorted(
            label_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    )
    return SessionPromptDrift(
        metrics=metrics,
        most_frequent_drift_labels=labels,
        examples=tuple(examples),
        insights=_prompt_drift_insights(metrics, labels),
    )


def _validate_records(records: Sequence[PromptInstructionRecord]) -> None:
    if not isinstance(records, (list, tuple)):
        raise ValueError("records must be a list or tuple")
    last_turn = -1
    seen_ids: set[str] = set()
    for record in records:
        if not isinstance(record, PromptInstructionRecord):
            raise ValueError("records must contain PromptInstructionRecord instances")
        if not isinstance(record.topic, str) or not record.topic.strip():
            raise ValueError("topic must be a non-empty string")
        if not isinstance(record.instruction, str) or not record.instruction.strip():
            raise ValueError("instruction must be a non-empty string")
        if (
            not isinstance(record.turn_index, int)
            or isinstance(record.turn_index, bool)
            or record.turn_index < 0
        ):
            raise ValueError("turn_index must be a non-negative integer")
        if record.turn_index < last_turn:
            raise ValueError("records must be ordered by turn_index")
        if record.prompt_id is not None:
            if not isinstance(record.prompt_id, str) or not record.prompt_id.strip():
                raise ValueError("prompt_id must be a non-empty string or None")
            if record.prompt_id in seen_ids:
                raise ValueError("prompt_id values must be unique")
            seen_ids.add(record.prompt_id)
        last_turn = record.turn_index


def _drift_labels(
    previous: PromptInstructionRecord,
    current: PromptInstructionRecord,
) -> tuple[str, ...]:
    labels: list[str] = []
    if _normalize(previous.topic) != _normalize(current.topic):
        labels.append(DRIFT_TOPIC_CHANGE)
    if _normalize(previous.instruction) != _normalize(current.instruction):
        labels.append(DRIFT_INSTRUCTION_CHANGE)
    return tuple(labels)


def _normalize(value: str) -> str:
    return " ".join(value.lower().strip().split())


def _prompt_drift_insights(
    metrics: SessionPromptDriftMetrics,
    labels: tuple[str, ...],
) -> tuple[str, ...]:
    if metrics.total_records == 0:
        return ("No prompt records supplied.",)
    if metrics.drift_events == 0:
        return ("No prompt drift detected.",)
    insights = [
        f"Detected {metrics.drift_events} prompt drift events across "
        f"{metrics.comparable_transitions} transitions ({metrics.drift_rate:.1%})."
    ]
    if labels:
        insights.append("Most frequent drift labels: " + ", ".join(labels) + ".")
    return tuple(insights)
