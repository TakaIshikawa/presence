"""Session command retry resolution analyzer for workflow reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_session_command_retry_resolution(records: object) -> dict[str, Any]:
<<<<<<< HEAD
    """Identify failed commands that were retried and whether the retry eventually passed."""
=======
    """Identify failed commands that were retried and whether retry passed."""
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command record dictionaries")

    failed_command_count = 0
    retried_failure_count = 0
    resolved_retry_count = 0
    unresolved_retry_count = 0
    examples: list[dict[str, Any]] = []

<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    resolution_rate = _percentage(resolved_retry_count, retried_failure_count)

    return {
        "failed_command_count": failed_command_count,
        "retried_failure_count": retried_failure_count,
        "resolved_retry_count": resolved_retry_count,
        "unresolved_retry_count": unresolved_retry_count,
        "resolution_rate": resolution_rate,
        "examples": examples,
    }


<<<<<<< HEAD
def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _normalize_command(command: str) -> str:
    """Normalize command by lowercasing and collapsing whitespace."""
    normalized = command.lower()
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized
=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def _example(
    examples: list[dict[str, Any]],
<<<<<<< HEAD
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
=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
