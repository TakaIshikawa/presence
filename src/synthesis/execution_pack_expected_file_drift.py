"""Execution pack expected file drift analyzer for workflow reports."""

from __future__ import annotations

from typing import Any, Mapping


def analyze_execution_pack_expected_file_drift(records: object) -> dict[str, Any]:
    """Compare each task's expected_files with actual changed_files and report drift."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    task_count = 0
    tasks_with_unexpected_files = 0
    unexpected_file_count = 0
    missing_expected_file_count = 0
    examples: list[dict[str, Any]] = []

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


def _example(
    examples: list[dict[str, Any]],
    task_id: str,
    unexpected_files: list[str],
    missing_expected_files: list[str],
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "task_id": task_id,
                "unexpected_files": unexpected_files,
                "missing_expected_files": missing_expected_files,
            }
        )


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
