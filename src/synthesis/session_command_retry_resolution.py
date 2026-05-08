"""Session command retry resolution analyzer for workflow reports."""

from __future__ import annotations

from typing import Any


def analyze_session_command_retry_resolution(records: object) -> dict[str, Any]:
    """Identify failed commands that were retried and whether retries resolved."""
    if not isinstance(records, list):
        raise ValueError("records must be a list")

    _validate_records(records)

    failed_commands: dict[str, dict[str, Any]] = {}
    resolved_retries: set[str] = set()
    unresolved_retries: set[str] = set()

    for record in records:
        turn_index = record["turn_index"]
        command = record["command"]
        exit_code = record["exit_code"]
        normalized_command = record.get("normalized_command", _normalize_command(command))

        if exit_code == 0:
            # Successful command - check if it resolves a previous failure
            if normalized_command in failed_commands:
                resolved_retries.add(normalized_command)
                if normalized_command in unresolved_retries:
                    unresolved_retries.remove(normalized_command)
        else:
            # Failed command
            if normalized_command not in failed_commands:
                failed_commands[normalized_command] = {
                    "first_failure_turn": turn_index,
                    "retry_turns": [],
                    "command": command,
                }
            else:
                # Additional retry failure
                failed_commands[normalized_command]["retry_turns"].append(turn_index)
                if normalized_command not in resolved_retries:
                    unresolved_retries.add(normalized_command)

    # Collect examples (capped at 5)
    examples: list[dict[str, Any]] = []
    for normalized_command in sorted(failed_commands.keys()):
        if len(examples) >= 5:
            break
        info = failed_commands[normalized_command]
        if normalized_command in resolved_retries or normalized_command in unresolved_retries:
            examples.append({
                "first_failure_turn": info["first_failure_turn"],
                "retry_turns": info["retry_turns"],
                "command": info["command"],
                "resolved": normalized_command in resolved_retries,
            })

    retried_failure_count = len(resolved_retries) + len(unresolved_retries)
    resolution_rate = _percentage(len(resolved_retries), retried_failure_count)

    return {
        "failed_command_count": len(failed_commands),
        "retried_failure_count": retried_failure_count,
        "resolved_retry_count": len(resolved_retries),
        "unresolved_retry_count": len(unresolved_retries),
        "resolution_rate": resolution_rate,
        "examples": examples,
    }


def _validate_records(records: list[Any]) -> None:
    """Validate record structure and ordering."""
    prev_turn = -1
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("each record must be a dict")

        if "turn_index" not in record:
            raise ValueError("each record must have a turn_index")
        if "command" not in record:
            raise ValueError("each record must have a command")
        if "exit_code" not in record:
            raise ValueError("each record must have an exit_code")

        turn_index = record["turn_index"]
        command = record["command"]
        exit_code = record["exit_code"]

        if not isinstance(turn_index, int):
            raise ValueError("turn_index must be an integer")
        if isinstance(turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if turn_index < 0:
            raise ValueError("turn_index must be non-negative")

        if not isinstance(command, str):
            raise ValueError("command must be a string")
        if not command.strip():
            raise ValueError("command must not be empty")

        if not isinstance(exit_code, int):
            raise ValueError("exit_code must be an integer")

        if turn_index < prev_turn:
            raise ValueError("records must be ordered by turn_index")
        prev_turn = turn_index


def _normalize_command(command: str) -> str:
    """Normalize command for comparison (lowercase, collapse whitespace)."""
    import re
    return re.sub(r"\s+", " ", command.strip().lower())


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
