"""Command output truncation risk analyzer for workflow hygiene reports."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable, Mapping


COMMAND_CATEGORIES = ("test", "lint", "git", "other")
TRUNCATION_REASONS = (
    "truncated",
    "max_output_tokens",
    "output_omitted",
    "ellipsis",
)

_COMMAND_FIELDS = ("command", "cmd", "name")
_TEXT_FIELDS = (
    "output",
    "stdout",
    "stderr",
    "result",
    "message",
    "summary",
    "content",
)
_STRUCTURED_FIELDS = (
    "truncated",
    "is_truncated",
    "output_truncated",
    "max_output_tokens",
)


def analyze_command_output_truncation(records: object) -> dict[str, Any]:
    """Measure whether command results likely lost useful diagnostic context.

    Args:
        records: List of command result dictionaries or dataclass-style records.

    Returns:
        Stable aggregate metrics, category distributions, and risky examples.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command result dictionaries")

    category_counts: Counter[str] = Counter({name: 0 for name in COMMAND_CATEGORIES})
    risk_category_counts: Counter[str] = Counter({name: 0 for name in COMMAND_CATEGORIES})
    reason_counts: Counter[str] = Counter({name: 0 for name in TRUNCATION_REASONS})
    weak_examples: list[dict[str, Any]] = []

    total_commands = 0
    risky_commands = 0

    for index, raw_record in enumerate(records):
        record = _record_mapping(raw_record)
        if record is None:
            continue

        command = _extract_command(record)
        if not command:
            continue

        total_commands += 1
        category = _command_category(command)
        category_counts[category] += 1

        reasons = _truncation_reasons(record)
        if not reasons:
            continue

        risky_commands += 1
        risk_category_counts[category] += 1
        for reason in reasons:
            reason_counts[reason] += 1

        if len(weak_examples) < 5:
            weak_examples.append(
                {
                    "command": command,
                    "turn_index": _turn_index(record, index),
                    "category": category,
                    "reason": reasons[0],
                }
            )

    clean_commands = total_commands - risky_commands
    return {
        "total_records": len(records),
        "total_commands": total_commands,
        "risky_commands": risky_commands,
        "clean_commands": clean_commands,
        "risk_percentage": _percentage(risky_commands, total_commands),
        "command_category_counts": dict(category_counts),
        "risk_category_counts": dict(risk_category_counts),
        "truncation_reason_counts": dict(reason_counts),
        "weak_examples": weak_examples,
    }


def _record_mapping(record: object) -> Mapping[str, Any] | None:
    if isinstance(record, Mapping):
        return record
    if is_dataclass(record) and not isinstance(record, type):
        return asdict(record)
    return None


def _extract_command(record: Mapping[str, Any]) -> str:
    for field in _COMMAND_FIELDS:
        value = record.get(field)
        if isinstance(value, str) and value.strip():
            return _normalize(value)

    args = record.get("args") or record.get("input")
    if isinstance(args, Mapping):
        for field in ("cmd", "command"):
            value = args.get(field)
            if isinstance(value, str) and value.strip():
                return _normalize(value)
    return ""


def _truncation_reasons(record: Mapping[str, Any]) -> list[str]:
    reasons: list[str] = []

    for field in _STRUCTURED_FIELDS:
        value = record.get(field)
        if field == "max_output_tokens" and value not in (None, "", False):
            reasons.append("max_output_tokens")
        elif field != "max_output_tokens" and _truthy_truncation(value):
            reasons.append("truncated")

    metadata = record.get("metadata")
    if isinstance(metadata, Mapping):
        for field in _STRUCTURED_FIELDS:
            value = metadata.get(field)
            if field == "max_output_tokens" and value not in (None, "", False):
                reasons.append("max_output_tokens")
            elif field != "max_output_tokens" and _truthy_truncation(value):
                reasons.append("truncated")
        for reason in _textual_reasons(_string_values(metadata)):
            reasons.append(reason)

    for reason in _textual_reasons(_record_text_values(record)):
        reasons.append(reason)

    return _dedupe_reasons(reasons)


def _truthy_truncation(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() not in {"false", "no", "0"}
    return value not in (None, 0, 0.0, "")


def _record_text_values(record: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for field in _TEXT_FIELDS:
        values.extend(_string_values(record.get(field)))
    return values


def _string_values(value: object) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        values: list[str] = []
        for nested_value in value.values():
            values.extend(_string_values(nested_value))
        return values
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        values = []
        for item in value:
            values.extend(_string_values(item))
        return values
    return []


def _textual_reasons(values: Iterable[str]) -> list[str]:
    reasons: list[str] = []
    for value in values:
        normalized = value.lower()
        if "max_output_tokens" in normalized:
            reasons.append("max_output_tokens")
        if "output omitted" in normalized or "omitted output" in normalized:
            reasons.append("output_omitted")
        if "truncated" in normalized:
            reasons.append("truncated")
        if "..." in value:
            reasons.append("ellipsis")
    return reasons


def _dedupe_reasons(reasons: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for reason in reasons:
        if reason in TRUNCATION_REASONS and reason not in seen:
            deduped.append(reason)
            seen.add(reason)
    return deduped


def _command_category(command: str) -> str:
    normalized = command.lower().strip()
    if any(
        token in normalized
        for token in (
            "pytest",
            "vitest",
            "jest",
            "go test",
            "cargo test",
            "npm test",
            "pnpm test",
            "yarn test",
        )
    ):
        return "test"
    if any(
        token in normalized
        for token in (
            "ruff",
            "flake8",
            "eslint",
            "pylint",
            "prettier",
            "black --check",
            "pre-commit",
            "shellcheck",
        )
    ):
        return "lint"
    if normalized == "git" or normalized.startswith("git "):
        return "git"
    return "other"


def _turn_index(record: Mapping[str, Any], fallback: int) -> int:
    value = record.get("turn_index", record.get("turn"))
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return fallback


def _normalize(command: str) -> str:
    return " ".join(command.strip().split())


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
