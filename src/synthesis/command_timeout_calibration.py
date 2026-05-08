"""Command timeout calibration analyzer for workflow hygiene reports."""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


CATEGORIES = ("test", "install", "build", "git", "other")


def analyze_command_timeout_calibration(records: object) -> dict[str, Any]:
    """Evaluate whether command timeouts fit observed command duration."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command event dictionaries")

    category_counts: Counter[str] = Counter({name: 0 for name in CATEGORIES})
    missing_timeout = 0
    missing_duration = 0
    timed_out = 0
    near_timeout = 0
    excessive_timeout = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            missing_timeout += 1
            _append_example(examples, "", index, "malformed_record")
            continue

        command = _command(record)
        category_counts[_category(command)] += 1
        duration = _number(record.get("duration_seconds"))
        timeout = _number(record.get("timeout_seconds"))
        exit_code = record.get("exit_code")

        if timeout is None or timeout <= 0:
            missing_timeout += 1
            _append_example(examples, command, _turn_index(record, index), "missing_timeout")
            continue
        if duration is None:
            missing_duration += 1
            _append_example(examples, command, _turn_index(record, index), "missing_duration")
            continue
        if exit_code == "timeout" or exit_code == 124 or record.get("timed_out") is True:
            timed_out += 1
            _append_example(examples, command, _turn_index(record, index), "timed_out")
        if duration >= timeout * 0.9:
            near_timeout += 1
            _append_example(examples, command, _turn_index(record, index), "near_timeout")
        if duration > 0 and timeout >= duration * 5:
            excessive_timeout += 1
            _append_example(examples, command, _turn_index(record, index), "excessive_timeout")

    total = len(records)
    risk_count = missing_timeout + timed_out + near_timeout + excessive_timeout
    return {
        "total_commands": total,
        "missing_timeout_count": missing_timeout,
        "missing_duration_count": missing_duration,
        "timed_out_count": timed_out,
        "near_timeout_count": near_timeout,
        "excessive_timeout_count": excessive_timeout,
        "risk_percentage": _percentage(risk_count, total),
        "category_counts": dict(category_counts),
        "examples": examples[:5],
    }


def _append_example(examples: list[dict[str, Any]], command: str, turn_index: int, reason: str) -> None:
    if len(examples) < 5:
        examples.append({"command": command, "turn_index": turn_index, "reason": reason})


def _command(record: Mapping[str, Any]) -> str:
    value = record.get("command") or record.get("cmd")
    return " ".join(value.strip().split()) if isinstance(value, str) else ""


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _category(command: str) -> str:
    normalized = command.lower()
    if any(token in normalized for token in ("pytest", "jest", "vitest", "go test", "cargo test", "npm test")):
        return "test"
    if any(token in normalized for token in ("npm install", "pnpm install", "yarn install", "pip install", "uv sync")):
        return "install"
    if any(token in normalized for token in ("build", "tsc", "webpack", "vite build")):
        return "build"
    if normalized == "git" or normalized.startswith("git "):
        return "git"
    return "other"


def _turn_index(record: Mapping[str, Any], fallback: int) -> int:
    value = record.get("turn_index")
    return value if isinstance(value, int) else fallback


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
