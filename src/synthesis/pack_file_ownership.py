"""Execution pack file ownership overlap analyzer."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations
from typing import Any, Iterable, Mapping


def analyze_pack_file_ownership(records: object) -> dict[str, Any]:
    """Identify same-pack tasks with overlapping expected files."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    packs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue
        pack = _pack_key(record)
        files = sorted(set(_files(record.get("expectedFiles", record.get("expected_files")))))
        packs[pack].append({"task": _task_id(record, index), "files": files})

    per_pack: dict[str, dict[str, int]] = {}
    conflict_pairs: list[dict[str, Any]] = []
    high_risk_files: list[dict[str, Any]] = []

    for pack, tasks in sorted(packs.items()):
        file_to_tasks: dict[str, list[str]] = defaultdict(list)
        for task in tasks:
            for file_path in task["files"]:
                file_to_tasks[file_path].append(task["task"])
        overlapping_files = {file_path: owners for file_path, owners in file_to_tasks.items() if len(owners) > 1}
        per_pack[pack] = {"task_count": len(tasks), "overlapping_file_count": len(overlapping_files)}
        for left, right in combinations(tasks, 2):
            shared = sorted(set(left["files"]) & set(right["files"]))
            if shared:
                conflict_pairs.append({"pack": pack, "tasks": [left["task"], right["task"]], "files": shared})
        for file_path, owners in sorted(overlapping_files.items()):
            if len(owners) >= 3:
                high_risk_files.append({"pack": pack, "file": file_path, "task_count": len(owners), "tasks": sorted(owners)})

    return {
        "total_tasks": len(records),
        "pack_count": len(packs),
        "per_pack": per_pack,
        "overlapping_file_count": sum(pack["overlapping_file_count"] for pack in per_pack.values()),
        "conflict_pairs": conflict_pairs,
        "high_risk_files": high_risk_files,
    }


def _pack_key(record: Mapping[str, Any]) -> str:
    value = record.get("executionPack", record.get("execution_pack"))
    if isinstance(value, Mapping):
        value = value.get("key") or value.get("id")
    return str(value) if value else "unknown"


def _files(value: object) -> list[str]:
    if isinstance(value, str):
        values = [value]
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        values = [item for item in value if isinstance(item, str)]
    else:
        values = []
    return ["/".join(item.strip().split()).strip("./") for item in values if item.strip()]


def _task_id(record: Mapping[str, Any], index: int) -> str:
    return str(record.get("title") or record.get("task_id") or record.get("id") or index)
