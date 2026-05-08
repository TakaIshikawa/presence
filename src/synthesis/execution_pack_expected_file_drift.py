"""Execution pack expected file drift analyzer for workflow reports."""

from __future__ import annotations

<<<<<<< HEAD
from typing import Any, Mapping


def analyze_execution_pack_expected_file_drift(records: object) -> dict[str, Any]:
    """Compare each task's expected_files with actual changed_files and report drift."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")
=======
from typing import Any, Iterable, Mapping


def analyze_execution_pack_expected_file_drift(records: object) -> dict[str, Any]:
    """Compare each task's expected_files with changed_files and report drift."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task record dictionaries")
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    task_count = 0
    tasks_with_unexpected_files = 0
    unexpected_file_count = 0
    missing_expected_file_count = 0
    examples: list[dict[str, Any]] = []

<<<<<<< HEAD
    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("records must be a list of task dictionaries")

        task_id = _string(record.get("task_id"))
        if not task_id:
            raise ValueError("task_id must be a non-empty string")

        expected_files = _normalize_file_list(record.get("expected_files"))
        changed_files = _normalize_file_list(record.get("changed_files"))

        task_count += 1

        unexpected_files = [f for f in changed_files if f not in expected_files]
        missing_expected_files = [f for f in expected_files if f not in changed_files]

        if unexpected_files or missing_expected_files:
            tasks_with_unexpected_files += 1

        unexpected_file_count += len(unexpected_files)
=======
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"record at index {index} is not a dictionary")

        task_id = _task_id(record)
        if not task_id:
            raise ValueError(f"record at index {index} is missing task_id")

        expected_files = _file_list(record.get("expected_files"), index, "expected_files")
        changed_files = _file_list(record.get("changed_files"), index, "changed_files")

        task_count += 1

        expected_set = set(expected_files)
        changed_set = set(changed_files)

        unexpected_files = sorted(changed_set - expected_set)
        missing_expected_files = sorted(expected_set - changed_set)

        if unexpected_files:
            tasks_with_unexpected_files += 1
            unexpected_file_count += len(unexpected_files)

>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
        missing_expected_file_count += len(missing_expected_files)

        if unexpected_files or missing_expected_files:
            _example(examples, task_id, unexpected_files, missing_expected_files)

    drift_rate = _percentage(tasks_with_unexpected_files, task_count)

    return {
        "task_count": task_count,
        "tasks_with_unexpected_files": tasks_with_unexpected_files,
        "unexpected_file_count": unexpected_file_count,
        "missing_expected_file_count": missing_expected_file_count,
        "drift_rate": drift_rate,
        "examples": examples,
    }


<<<<<<< HEAD
def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_file_list(value: object) -> set[str]:
    """Normalize file paths into a deduplicated set."""
    if value is None:
        return set()

    if not isinstance(value, (list, tuple)):
        raise ValueError("expected_files and changed_files must be sequences")

    normalized: set[str] = set()
    for item in value:
        if not isinstance(item, str):
            raise ValueError("file paths must be strings")
        path = item.strip()
        if path:
            normalized.add(path)

    return normalized
=======
def _task_id(record: Mapping[str, Any]) -> str:
    value = record.get("task_id")
    return value.strip() if isinstance(value, str) and value.strip() else ""


def _file_list(value: object, index: int, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        result: list[str] = []
        seen: set[str] = set()
        for item in value:
            if not isinstance(item, str):
                raise ValueError(f"record at index {index} has non-string item in {field_name}")
            normalized = item.strip()
            if normalized and normalized not in seen:
                result.append(normalized)
                seen.add(normalized)
        return result
    raise ValueError(f"record at index {index} has invalid {field_name} type")
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def _example(
    examples: list[dict[str, Any]],
    task_id: str,
    unexpected_files: list[str],
    missing_expected_files: list[str],
) -> None:
    if len(examples) < 5:
<<<<<<< HEAD
        examples.append(
            {
                "task_id": task_id,
                "unexpected_files": unexpected_files,
                "missing_expected_files": missing_expected_files,
            }
        )
=======
        examples.append({
            "task_id": task_id,
            "unexpected_files": unexpected_files,
            "missing_expected_files": missing_expected_files,
        })
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
