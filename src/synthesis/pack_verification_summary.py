"""Execution pack verification summary analyzer."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def analyze_pack_verification_summary(tasks: object) -> dict[str, Any]:
    """Summarize verification outcomes for execution pack tasks."""
    if tasks is None:
        tasks = []
    if not isinstance(tasks, list):
        raise ValueError("tasks must be a list of dictionaries")

    totals = {"passed": 0, "failed": 0, "missing": 0}
    summaries = []
    pack_totals: dict[str, dict[str, int]] = {}
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            status = "missing"
            command = ""
            task_id = str(index)
            pack_key = "unpackaged"
        else:
            command = str(task.get("verification_command", task.get("verification", "")) or "")
            raw_status = str(task.get("verification_status", task.get("status", ""))).lower()
            status = raw_status if raw_status in {"passed", "failed"} else "missing"
            task_id = str(task.get("task_id", task.get("id", index)))
            pack_key = _pack_key(task)
            if not command and status == "passed":
                status = "missing"
        totals[status] += 1
        pack_totals.setdefault(pack_key, {"passed": 0, "failed": 0, "missing": 0})[status] += 1
        summaries.append({"task_id": task_id, "verification_status": status, "command": command})

    total = len(tasks)
    verified = totals["passed"] + totals["failed"]
    return {
        "task_count": total,
        "verified_task_count": verified,
        "passed_count": totals["passed"],
        "failed_count": totals["failed"],
        "missing_count": totals["missing"],
        "verification_coverage_percentage": round((verified / total) * 100.0, 2) if total else 0.0,
        "pass_rate_percentage": round((totals["passed"] / verified) * 100.0, 2) if verified else 0.0,
        "packs": {key: _pack_summary(counts) for key, counts in sorted(pack_totals.items())},
        "tasks": summaries,
    }


def _pack_key(task: dict[str, Any]) -> str:
    for key in ("execution_pack", "executionPack", "pack", "pack_key"):
        value = task.get(key)
        if isinstance(value, Mapping) and key in {"execution_pack", "executionPack"}:
            value = value.get("key")
        if value:
            return str(value)
    return "unpackaged"


def _pack_summary(counts: dict[str, int]) -> dict[str, Any]:
    task_count = sum(counts.values())
    verified = counts["passed"] + counts["failed"]
    return {
        "task_count": task_count,
        "passed": counts["passed"],
        "failed": counts["failed"],
        "missing": counts["missing"],
        "verification_coverage_percentage": round((verified / task_count) * 100.0, 2) if task_count else 0.0,
        "pass_rate_percentage": round((counts["passed"] / verified) * 100.0, 2) if verified else 0.0,
    }
