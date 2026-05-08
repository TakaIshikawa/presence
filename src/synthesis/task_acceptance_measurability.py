"""Task acceptance criteria measurability analyzer."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


VAGUE_TERMS = ("improve", "better", "appropriate", "etc", "clean", "nice", "robust")
OBSERVABLE_TERMS = ("count", "percentage", "raises", "returns", "includes", "excludes", "test", "pytest", "file", "command")


def analyze_task_acceptance_measurability(records: object) -> dict[str, Any]:
    """Score whether generated tasks have observable, testable acceptance criteria."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    measurable = 0
    vague = 0
    missing_criteria = 0
    missing_test_command = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            missing_criteria += 1
            _example(examples, str(index), "", "malformed_task")
            missing_test_command += 1
            continue
        title = _title(record, index)
        criteria = _criteria(record.get("acceptanceCriteria", record.get("acceptance_criteria")))
        if not criteria:
            missing_criteria += 1
            _example(examples, title, "", "missing_criteria")
        for criterion in criteria:
            if _is_vague(criterion):
                vague += 1
                _example(examples, title, criterion, "vague_criterion")
            else:
                measurable += 1
        test_command = record.get("testCommand", record.get("test_command"))
        if not isinstance(test_command, str) or not test_command.strip():
            missing_test_command += 1
            _example(examples, title, "", "missing_test_command")

    total_criteria = measurable + vague
    return {
        "total_tasks": len(records),
        "measurable_criteria_count": measurable,
        "vague_criteria_count": vague,
        "missing_criteria_count": missing_criteria,
        "missing_test_command_count": missing_test_command,
        "measurable_percentage": _percentage(measurable, total_criteria),
        "examples": examples[:5],
    }


def _criteria(value: object) -> list[str]:
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [item.strip() for item in value if isinstance(item, str) and item.strip()]
    return []


def _is_vague(criterion: str) -> bool:
    lowered = criterion.lower()
    return any(term in lowered for term in VAGUE_TERMS) and not any(term in lowered for term in OBSERVABLE_TERMS)


def _example(examples: list[dict[str, Any]], title: str, criterion: str, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"title": title, "criterion": criterion, "reason": reason})


def _title(record: Mapping[str, Any], index: int) -> str:
    return str(record.get("title") or record.get("task_id") or record.get("id") or index)


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
