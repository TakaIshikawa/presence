"""Pack branch rebase behavior analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_pack_branch_rebase_behavior(records: object) -> dict[str, Any]:
    """Measure branch update and rebase behavior before merge."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack command dictionaries")

    total_packs = 0
    packs_with_update_signal = 0
    packs_with_rebase = 0
    packs_with_conflicts = 0
    packs_verified_after_rebase = 0
    stale_merge_risk_examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        commands = _extract_commands(record)
        total_packs += 1

        update_indexes = [index for index, command in enumerate(commands) if _is_update_signal(command)]
        rebase_indexes = [index for index, command in enumerate(commands) if _is_rebase_signal(command)]
        merge_indexes = [index for index, command in enumerate(commands) if _is_merge_to_main(command)]
        conflict_indexes = [index for index, command in enumerate(commands) if _is_conflict_signal(command)]

        has_update_signal = bool(update_indexes)
        has_rebase = bool(rebase_indexes)
        has_conflict = bool(conflict_indexes)
        has_verification_after_rebase = bool(
            rebase_indexes
            and any(_is_verification(command) for command in commands[min(rebase_indexes) + 1 :])
        )

        if has_update_signal:
            packs_with_update_signal += 1
        if has_rebase:
            packs_with_rebase += 1
        if has_conflict:
            packs_with_conflicts += 1
        if has_verification_after_rebase:
            packs_verified_after_rebase += 1

        if merge_indexes and not _has_update_before_merge(update_indexes, merge_indexes):
            if len(stale_merge_risk_examples) < 5:
                stale_merge_risk_examples.append(
                    {
                        "pack_id": _string_or_none(record.get("pack_id")),
                        "merge_command": commands[merge_indexes[0]],
                        "reason": "merged_without_prior_update_signal",
                    }
                )

    return {
        "total_packs": total_packs,
        "packs_with_update_signal": packs_with_update_signal,
        "packs_with_rebase": packs_with_rebase,
        "packs_with_conflicts": packs_with_conflicts,
        "packs_verified_after_rebase": packs_verified_after_rebase,
        "stale_merge_risk_examples": stale_merge_risk_examples,
    }


def _extract_commands(record: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("commands", "command_records", "tool_calls", "session_commands"):
        value = record.get(key)
        if isinstance(value, list):
            for item in value:
                command = _command_text(item)
                if command:
                    commands.append(command)
    command = _command_text(record)
    if command:
        commands.append(command)
    return commands


def _command_text(value: object) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        for key in ("command", "cmd", "bash", "input", "text"):
            item = value.get(key)
            if isinstance(item, str) and item.strip():
                return item.strip()
    return ""


def _is_update_signal(command: str) -> bool:
    normalized = _normalize(command)
    return (
        bool(re.search(r"\bgit\s+fetch\b", normalized))
        or bool(re.search(r"\bgit\s+pull\b.*\s--rebase\b", normalized))
        or _is_rebase_signal(command)
        or bool(re.search(r"\bgit\s+merge\s+origin/(?:main|master)\b", normalized))
    )


def _is_rebase_signal(command: str) -> bool:
    normalized = _normalize(command)
    return bool(re.search(r"\bgit\s+(?:pull\b.*\s--rebase|rebase\b)", normalized))


def _is_merge_to_main(command: str) -> bool:
    normalized = _normalize(command)
    return bool(re.search(r"\bgit\s+merge\s+(?:origin/)?(?:main|master)\b", normalized))


def _is_conflict_signal(command: str) -> bool:
    normalized = _normalize(command)
    return any(
        term in normalized
        for term in (
            "conflict",
            "merge conflict",
            "fix conflicts",
            "resolve conflicts",
            "both modified",
            "<<<<<<<",
        )
    )


def _is_verification(command: str) -> bool:
    normalized = _normalize(command)
    return any(
        term in normalized
        for term in ("pytest", "npm test", "pnpm test", "yarn test", "cargo test", "go test", "ruff", "mypy")
    )


def _has_update_before_merge(update_indexes: list[int], merge_indexes: list[int]) -> bool:
    first_merge = min(merge_indexes)
    return any(index < first_merge for index in update_indexes)


def _normalize(command: str) -> str:
    return " ".join(command.lower().split())


def _string_or_none(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
