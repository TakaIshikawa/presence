"""Command working-directory hygiene analyzer for workflow reports."""

from __future__ import annotations

import os
from typing import Any, Mapping


def analyze_command_cwd_hygiene(records: object) -> dict[str, Any]:
    """Identify commands without an explicit cwd or outside the project path."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command record dictionaries")

    missing_cwd_count = 0
    outside_project_count = 0
    valid_cwd_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            missing_cwd_count += 1
            _example(examples, index, "", "", "", "malformed_record")
            continue

        cwd = _string(record.get("cwd")) or _string(record.get("workdir"))
        command = _string(record.get("command")) or _string(record.get("cmd"))
        project_path = _string(record.get("project_path"))
        if not cwd:
            missing_cwd_count += 1
            _example(examples, _record_index(record, index), command, "", project_path, "missing_cwd")
            continue
        if project_path and not _is_within_project(cwd, project_path):
            outside_project_count += 1
            _example(
                examples,
                _record_index(record, index),
                command,
                cwd,
                project_path,
                "outside_project_path",
            )
            continue
        valid_cwd_count += 1

    issue_count = missing_cwd_count + outside_project_count
    return {
        "total_commands": len(records),
        "valid_cwd_count": valid_cwd_count,
        "missing_cwd_count": missing_cwd_count,
        "outside_project_count": outside_project_count,
        "issue_count": issue_count,
        "hygiene_percentage": _percentage(valid_cwd_count, len(records)),
        "examples": examples,
    }


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _record_index(record: Mapping[str, Any], fallback: int) -> int:
    value = record.get("turn_index", record.get("index"))
    return value if isinstance(value, int) else fallback


def _is_within_project(cwd: str, project_path: str) -> bool:
    normalized_cwd = os.path.abspath(os.path.expanduser(cwd))
    normalized_project = os.path.abspath(os.path.expanduser(project_path))
    try:
        return os.path.commonpath([normalized_cwd, normalized_project]) == normalized_project
    except ValueError:
        return False


def _example(
    examples: list[dict[str, Any]],
    index: int,
    command: str,
    cwd: str,
    project_path: str,
    reason: str,
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "index": index,
                "command": command,
                "cwd": cwd,
                "project_path": project_path,
                "reason": reason,
            }
        )


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
