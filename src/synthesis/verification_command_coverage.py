"""Verification command coverage analyzer for execution hygiene reports."""

from __future__ import annotations

from collections import Counter
from typing import Any, Iterable


COMMAND_CLASSES = ("missing", "targeted", "broad", "typecheck", "build", "lint", "unknown")
_COMMAND_FIELDS = (
    "verification_commands",
    "verification",
    "verify_commands",
    "test_commands",
    "tests",
    "commands",
)


def analyze_verification_command_coverage(records: object) -> dict[str, Any]:
    """Measure whether task/session records include meaningful verification commands.

    Args:
        records: List of dictionaries describing completed tasks or sessions.

    Returns:
        Stable aggregate metrics, command class distribution, and weak examples.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task/session dictionaries")

    class_counts: Counter[str] = Counter({name: 0 for name in COMMAND_CLASSES})
    command_count = 0
    verified_records = 0
    weak_examples: list[dict[str, Any]] = []
    record_summaries: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        commands = _extract_commands(record)
        classes = [_classify_command(command) for command in commands]
        if not classes:
            classes = ["missing"]

        for command_class in classes:
            class_counts[command_class] += 1

        meaningful_classes = [c for c in classes if c not in {"missing", "unknown"}]
        if meaningful_classes:
            verified_records += 1
        if commands:
            command_count += len(commands)

        summary = {
            "index": index,
            "record_id": _record_id(record, index),
            "command_count": len(commands),
            "classes": classes,
        }
        record_summaries.append(summary)

        if not meaningful_classes:
            weak_examples.append(
                {
                    "index": index,
                    "record_id": summary["record_id"],
                    "reason": "missing" if not commands else "unknown_verification",
                    "commands": commands[:3],
                }
            )

    total_records = len(records)
    missing_records = total_records - verified_records
    coverage = _percentage(verified_records, total_records)
    total_classifications = sum(class_counts.values())

    return {
        "total_records": total_records,
        "records_with_verification": verified_records,
        "records_missing_verification": missing_records,
        "total_commands": command_count,
        "coverage_percentage": coverage,
        "missing_percentage": _percentage(missing_records, total_records),
        "command_class_counts": dict(class_counts),
        "command_class_percentages": {
            name: _percentage(class_counts[name], total_classifications)
            for name in COMMAND_CLASSES
        },
        "weak_or_missing_examples": weak_examples[:5],
        "record_summaries": record_summaries,
    }


def _extract_commands(record: object) -> list[str]:
    if not isinstance(record, dict):
        return []

    commands: list[str] = []
    for field in _COMMAND_FIELDS:
        commands.extend(_coerce_commands(record.get(field)))

    tool_calls = record.get("tool_calls")
    if isinstance(tool_calls, list):
        for call in tool_calls:
            if isinstance(call, dict):
                name = str(call.get("name", call.get("tool", ""))).lower()
                value = call.get("command") or call.get("input") or call.get("args")
                if name in {"bash", "shell", "exec", "exec_command"}:
                    commands.extend(_coerce_commands(value))

    normalized_commands: list[str] = []
    seen_commands: set[str] = set()
    for command in commands:
        normalized_command = _normalize_command(command)
        if normalized_command and normalized_command not in seen_commands:
            normalized_commands.append(normalized_command)
            seen_commands.add(normalized_command)
    return normalized_commands


def _coerce_commands(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        command = value.get("command") or value.get("cmd")
        return [command] if isinstance(command, str) else []
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, dict)):
        commands: list[str] = []
        for item in value:
            commands.extend(_coerce_commands(item))
        return commands
    return []


def _classify_command(command: str) -> str:
    normalized = command.lower()
    if not normalized.strip():
        return "missing"

    if any(token in normalized for token in ("mypy", "pyright", "tsc", "typecheck", "type-check")):
        return "typecheck"
    if any(token in normalized for token in ("npm run build", "pnpm build", "yarn build", "cargo build", "go build", "make build")):
        return "build"
    if _is_lint_or_format_check(normalized):
        return "lint"
    if _is_broad_suite(normalized):
        return "broad"
    if _is_targeted_verification(normalized):
        return "targeted"
    if any(token in normalized for token in ("pytest", "vitest", "jest", "go test", "cargo test", "npm test", "pnpm test", "yarn test")):
        return "broad"
    return "unknown"


def _is_lint_or_format_check(command: str) -> bool:
    lint_tokens = ("ruff check", "flake8", "pylint", "eslint")
    if any(token in command for token in lint_tokens):
        return True
    if any(token in command for token in ("black", "prettier")) and "--check" in command:
        return True
    return False


def _is_broad_suite(command: str) -> bool:
    broad_commands = {
        "pytest",
        "python -m pytest",
        "npm test",
        "pnpm test",
        "yarn test",
        "go test ./...",
        "cargo test",
    }
    stripped = command.strip()
    return stripped in broad_commands or stripped.endswith("pytest tests") or "pytest tests/" not in stripped and stripped.startswith("pytest")


def _is_targeted_verification(command: str) -> bool:
    if "pytest" in command and ("tests/test_" in command or "::" in command):
        return True
    if any(token in command for token in ("vitest", "jest")) and (" -- " in command or ".test." in command or ".spec." in command):
        return True
    if "go test" in command and ("-run" in command or "/..." not in command):
        return True
    if "cargo test" in command and "--" in command:
        return True
    return False


def _normalize_command(command: str) -> str:
    return " ".join(command.strip().split())


def _record_id(record: object, index: int) -> str:
    if isinstance(record, dict):
        for key in ("id", "task_id", "session_id", "title"):
            value = record.get(key)
            if value:
                return str(value)
    return str(index)


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
