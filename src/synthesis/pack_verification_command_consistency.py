"""Execution-pack verification command consistency analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


TEST_PATH_RE = re.compile(r"tests/[^\s'\"`]+")


def analyze_pack_verification_command_consistency(records: object) -> dict[str, Any]:
    """Flag packs whose verification command omits task-level targeted tests."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of execution-pack task dictionaries")

    packs: dict[str, dict[str, Any]] = {}
    missing_task_command_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        pack_key = _pack_key(record)
        pack = packs.setdefault(pack_key, {"pack_key": pack_key, "task_count": 0, "pack_command": "", "task_test_paths": []})
        pack["task_count"] += 1
        pack_command = _pack_command(record)
        if pack_command and not pack["pack_command"]:
            pack["pack_command"] = pack_command
        task_command = _task_command(record)
        if not task_command:
            missing_task_command_count += 1
            _example(examples, pack_key, _task_id(record, index), "missing_task_command", [])
            continue
        pack["task_test_paths"].extend(_test_paths(task_command))

    summaries: list[dict[str, Any]] = []
    inconsistent_pack_count = 0
    for pack_key in sorted(packs):
        pack = packs[pack_key]
        expected_paths = _dedupe(pack["task_test_paths"])
        pack_command = pack["pack_command"]
        missing_paths = [path for path in expected_paths if path not in pack_command]
        inconsistent = bool(expected_paths and (not pack_command or missing_paths))
        if inconsistent:
            inconsistent_pack_count += 1
            _example(examples, pack_key, "", "pack_command_missing_tests", missing_paths)
        summaries.append(
            {
                "pack_key": pack_key,
                "task_count": pack["task_count"],
                "pack_command": pack_command,
                "expected_test_paths": expected_paths,
                "missing_test_paths": missing_paths,
                "consistent": not inconsistent,
            }
        )

    return {
        "pack_count": len(packs),
        "inconsistent_pack_count": inconsistent_pack_count,
        "missing_task_command_count": missing_task_command_count,
        "packs": summaries,
        "examples": examples[:5],
    }


def _pack_key(record: Mapping[str, Any]) -> str:
    for key in ("executionPack", "execution_pack"):
        value = record.get(key)
        if isinstance(value, Mapping):
            nested = value.get("key")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    for key in ("pack_key", "pack"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unpackaged"


def _pack_command(record: Mapping[str, Any]) -> str:
    for key in ("verificationCommand", "verification_command", "packVerificationCommand"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    return ""


def _task_command(record: Mapping[str, Any]) -> str:
    for key in ("testCommand", "test_command", "verification_command"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    return ""


def _test_paths(command: str) -> list[str]:
    return [match.group(0).rstrip(".,;:") for match in TEST_PATH_RE.finditer(command)]


def _task_id(record: Mapping[str, Any], fallback: int) -> str:
    for key in ("taskId", "task_id", "id"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(fallback)


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value not in seen:
            deduped.append(value)
            seen.add(value)
    return deduped


def _example(examples: list[dict[str, Any]], pack_key: str, task_id: str, reason: str, missing_paths: list[str]) -> None:
    if len(examples) < 5:
        examples.append({"pack_key": pack_key, "task_id": task_id, "reason": reason, "missing_test_paths": missing_paths})
