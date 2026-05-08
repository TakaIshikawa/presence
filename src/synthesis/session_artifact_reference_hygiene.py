"""Session artifact reference hygiene analyzer."""

from __future__ import annotations

import re
from typing import Any, Iterable, Mapping


URL_RE = re.compile(r"https?://[^\s)]+")
PATH_RE = re.compile(r"(?:[\w.-]+/)+[\w.-]+")
VAGUE_TERMS = ("the output", "the report", "the artifact", "the file", "the results")


def analyze_session_artifact_reference_hygiene(records: object) -> dict[str, Any]:
    """Evaluate whether final summaries cite concrete produced artifacts."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session artifact dictionaries")

    concrete_reference_count = 0
    vague_reference_count = 0
    missing_reference_count = 0
    artifact_record_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        artifacts = _artifacts(record.get("artifacts"))
        if not artifacts:
            continue
        artifact_record_count += 1
        final_answer = _string(record.get("final_answer")) or _string(record.get("final_message"))
        commands = _commands(record.get("commands"))
        if _has_concrete_reference(final_answer, artifacts, commands):
            concrete_reference_count += 1
            continue
        if any(term in final_answer.lower() for term in VAGUE_TERMS):
            vague_reference_count += 1
            _example(examples, record, index, "vague_reference")
        missing_reference_count += 1
        if not examples or examples[-1].get("session_id") != _session_id(record, index):
            _example(examples, record, index, "missing_reference")

    return {
        "artifact_record_count": artifact_record_count,
        "concrete_reference_count": concrete_reference_count,
        "concrete_reference_rate": _percentage(concrete_reference_count, artifact_record_count),
        "vague_reference_count": vague_reference_count,
        "missing_reference_count": missing_reference_count,
        "examples": examples[:5],
    }


def _artifacts(value: object) -> list[str]:
    values = _strings(value)
    artifacts: list[str] = []
    for item in values:
        artifacts.append(item)
    return artifacts


def _commands(value: object) -> list[str]:
    return _strings(value)


def _strings(value: object) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in ("path", "url", "command", "name"):
            nested = value.get(key)
            if isinstance(nested, str) and nested.strip():
                values.append(nested.strip())
        return values
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        values = []
        for item in value:
            values.extend(_strings(item))
        return values
    return []


def _has_concrete_reference(final_answer: str, artifacts: list[str], commands: list[str]) -> bool:
    if not final_answer:
        return False
    lowered = final_answer.lower()
    for artifact in artifacts:
        if artifact and artifact.lower() in lowered:
            return True
    for command in commands:
        if command and command.lower() in lowered:
            return True
    return bool(URL_RE.search(final_answer) or PATH_RE.search(final_answer))


def _example(examples: list[dict[str, Any]], record: Mapping[str, Any], index: int, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"session_id": _session_id(record, index), "reason": reason})


def _session_id(record: Mapping[str, Any], fallback: int) -> str:
    value = record.get("session_id")
    return value.strip() if isinstance(value, str) and value.strip() else str(fallback)


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
