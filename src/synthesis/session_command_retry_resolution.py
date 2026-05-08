"""Session command retry resolution analyzer for workflow reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_command_retry_resolution(records: object) -> dict[str, Any]:
    """Identify failed commands that were retried and whether retry passed."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command record dictionaries")

    failed_command_count = 0
    retried_failure_count = 0
    resolved_retry_count = 0
    unresolved_retry_count = 0
    examples: list[dict[str, Any]] = []

    # Track failures by normalized command
    failed_commands: dict[str, dict[str, Any]] = {}
    # Track resolved failures separately to handle re-failures
    resolved_failures: list[dict[str, Any]] = []
    previous_turn: int | None = None

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"record at index {index} is not a dictionary")

        turn_index = _turn_index(record, index)
        if previous_turn is not None and turn_index < previous_turn:
            raise ValueError(f"record at index {index} has unordered turn_index")
        previous_turn = turn_index

        command = _command(record, index)
        exit_code = _exit_code(record, index)
        normalized_command = _normalized_command(record, command)

        if exit_code == 0:
            # Check if this resolves a previous failure
            if normalized_command in failed_commands:
                failure_info = failed_commands[normalized_command]
                failure_info["resolved"] = True
                failure_info["retry_turns"].append(turn_index)
                resolved_retry_count += 1
                # Move to resolved list and remove from active failures
                resolved_failures.append(failure_info)
                del failed_commands[normalized_command]
        else:
            # This is a failed command
            if normalized_command not in failed_commands:
                # New failure for this command
                failed_command_count += 1
                failed_commands[normalized_command] = {
                    "first_failure_turn": turn_index,
                    "retry_turns": [],
                    "resolved": False,
                    "command": command,
                }
            else:
                # Retry of an existing failure
                failed_commands[normalized_command]["retry_turns"].append(turn_index)

    # Count retries and unresolved retries from both active and resolved failures
    for info in resolved_failures:
        if info["retry_turns"]:
            retried_failure_count += 1

    for info in failed_commands.values():
        if info["retry_turns"]:
            retried_failure_count += 1
            unresolved_retry_count += 1
            _example(
                examples,
                info["first_failure_turn"],
                info["retry_turns"],
                info["command"],
            )

    resolution_rate = _percentage(resolved_retry_count, retried_failure_count)

    return {
        "failed_command_count": failed_command_count,
        "retried_failure_count": retried_failure_count,
        "resolved_retry_count": resolved_retry_count,
        "unresolved_retry_count": unresolved_retry_count,
        "resolution_rate": resolution_rate,
        "examples": examples,
    }


def _turn_index(record: Mapping[str, Any], index: int) -> int:
    value = record.get("turn_index")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record at index {index} has invalid turn_index")
    if value < 0:
        raise ValueError(f"record at index {index} has negative turn_index")
    return value


def _command(record: Mapping[str, Any], index: int) -> str:
    value = record.get("command")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"record at index {index} has empty command")
    return value.strip()


def _exit_code(record: Mapping[str, Any], index: int) -> int:
    value = record.get("exit_code")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record at index {index} has invalid exit_code")
    return value


def _normalized_command(record: Mapping[str, Any], command: str) -> str:
    # Use provided normalized_command if available, otherwise normalize
    value = record.get("normalized_command")
    if isinstance(value, str) and value.strip():
        return value.strip().lower()
    # Normalize by converting to lowercase and collapsing whitespace
    return re.sub(r"\s+", " ", command.lower()).strip()


def _example(
    examples: list[dict[str, Any]],
    first_failure_turn: int,
    retry_turns: list[int],
    command: str,
) -> None:
    if len(examples) < 5:
        examples.append({
            "first_failure_turn": first_failure_turn,
            "retry_turns": retry_turns.copy(),
            "command": command,
        })


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
