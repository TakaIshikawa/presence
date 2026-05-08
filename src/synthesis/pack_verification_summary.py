"""Execution pack verification summary analyzer."""

from __future__ import annotations

from typing import Any


def analyze_pack_verification_summary(tasks: object) -> dict[str, Any]:
    """Summarize verification outcomes for execution pack tasks."""
    if tasks is None:
        tasks = []
    if not isinstance(tasks, list):
        raise ValueError("tasks must be a list of dictionaries")

    totals = {"passed": 0, "failed": 0, "missing": 0}
    summaries = []
    for index, task in enumerate(tasks):
        if not isinstance(task, dict):
            status = "missing"
            command = ""
            task_id = str(index)
        else:
            command = str(task.get("verification_command", task.get("verification", "")) or "")
            raw_status = str(task.get("verification_status", task.get("status", ""))).lower()
            status = raw_status if raw_status in {"passed", "failed"} else "missing"
            task_id = str(task.get("task_id", task.get("id", index)))
            if not command and status == "passed":
                status = "missing"
        totals[status] += 1
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
        "tasks": summaries,
    }
