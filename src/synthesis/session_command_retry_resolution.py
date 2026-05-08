"""Session command retry resolution analyzer for workflow reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_command_retry_resolution(records: object) -> dict[str, Any]:
    """Identify failed commands that were retried and whether the retry eventually passed."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command record dictionaries")

    failed_command_count = 0
    retried_failure_count = 0
    resolved_retry_count = 0
    unresolved_retry_count = 0
    examples: list[dict[str, Any]] = []

    # Track failed commands by normalized command
    failed_commands: dict[str, list[int]] = {}
    successful_commands: set[str] = set()

    prev_turn = -1

    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("records must be a list of command record dictionaries")

        turn_index = record.get("turn_index")
        command = _string(record.get("command"))
        exit_code = record.get("exit_code")

        if not isinstance(turn_index, int) or isinstance(turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if turn_index <= prev_turn:
            raise ValueError("records must be ordered by turn_index")
        prev_turn = turn_index

        if not command:
            raise ValueError("command must be a non-empty string")

        if not isinstance(exit_code, int) or isinstance(exit_code, bool):
            raise ValueError("exit_code must be an integer")

        normalized_command = record.get("normalized_command")
        if normalized_command is not None:
            if not isinstance(normalized_command, str):
                raise ValueError("normalized_command must be a string")
            normalized_cmd = normalized_command.strip()
        else:
            normalized_cmd = _normalize_command(command)

        if exit_code != 0:
            # Failed command
            if normalized_cmd not in failed_commands:
                failed_commands[normalized_cmd] = []
                failed_command_count += 1
            failed_commands[normalized_cmd].append(turn_index)
        else:
            # Successful command
            successful_commands.add(normalized_cmd)

    # Analyze retries
    for normalized_cmd, failure_turns in failed_commands.items():
        if normalized_cmd in successful_commands:
            # At least one retry succeeded
            retried_failure_count += 1
            resolved_retry_count += 1
            _example(examples, failure_turns, normalized_cmd, "resolved")
        elif len(failure_turns) > 1:
            # Multiple failures, no success
            retried_failure_count += 1
            unresolved_retry_count += 1
            _example(examples, failure_turns, normalized_cmd, "unresolved")

    resolution_rate = _percentage(resolved_retry_count, retried_failure_count)

    return {
        "failed_command_count": failed_command_count,
        "retried_failure_count": retried_failure_count,
        "resolved_retry_count": resolved_retry_count,
        "unresolved_retry_count": unresolved_retry_count,
        "resolution_rate": resolution_rate,
        "examples": examples,
    }


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_command(command: str) -> str:
    """Normalize command by lowercasing and collapsing whitespace."""
    normalized = command.lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _example(
    examples: list[dict[str, Any]],
    failure_turns: list[int],
    command: str,
    status: str,
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "first_failure_turn": failure_turns[0],
                "retry_turns": failure_turns[1:] if len(failure_turns) > 1 else [],
                "command": command,
                "status": status,
            }
        )


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
