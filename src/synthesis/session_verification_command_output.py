"""Session verification command output analyzer for workflow hygiene reports."""

from __future__ import annotations

from typing import Any


def analyze_session_verification_command_output(records: object) -> dict[str, Any]:
    """Detect verification commands with unexpected output patterns."""
    if not isinstance(records, list):
        raise ValueError("records must be a list")

    _validate_records(records)

    total_verifications = 0
    stderr_on_success_count = 0
    no_output_on_failure_count = 0
    truncated_output_count = 0

    examples: list[dict[str, Any]] = []

    for record in records:
        turn_index = record["turn_index"]
        command = record["command"]
        exit_code = record["exit_code"]
        stdout = record.get("stdout", "")
        stderr = record.get("stderr", "")
        truncated = record.get("truncated", False)

        # Only analyze verification commands
        if not _is_verification_command(command):
            continue

        total_verifications += 1

        # Check for stderr on success
        if exit_code == 0 and stderr.strip():
            stderr_on_success_count += 1
            _append_example(
                examples,
                turn_index,
                command,
                "stderr_on_success",
                f"stderr present: {stderr.strip()[:100]}"
            )

        # Check for no output on failure
        if exit_code != 0 and not stdout.strip() and not stderr.strip():
            no_output_on_failure_count += 1
            _append_example(
                examples,
                turn_index,
                command,
                "no_output_on_failure",
                f"exit code {exit_code} with empty stdout and stderr"
            )

        # Check for truncated output
        if truncated:
            truncated_output_count += 1
            _append_example(
                examples,
                turn_index,
                command,
                "truncated_output",
                "output was truncated"
            )

    risk_count = stderr_on_success_count + no_output_on_failure_count + truncated_output_count

    return {
        "total_verifications": total_verifications,
        "stderr_on_success_count": stderr_on_success_count,
        "no_output_on_failure_count": no_output_on_failure_count,
        "truncated_output_count": truncated_output_count,
        "risk_percentage": _percentage(risk_count, total_verifications),
        "examples": examples[:5],
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


def _is_verification_command(command: str) -> bool:
    """Identify verification commands (tests, type checks, linters)."""
    normalized = command.lower().strip()
    verification_patterns = [
        "pytest",
        "jest",
        "vitest",
        "npm test",
        "yarn test",
        "pnpm test",
        "go test",
        "cargo test",
        "mvn test",
        "gradle test",
        "tsc",
        "mypy",
        "pylint",
        "eslint",
        "flake8",
        "black",
        "ruff",
        "clippy",
    ]
    return any(pattern in normalized for pattern in verification_patterns)


def _append_example(
    examples: list[dict[str, Any]],
    turn_index: int,
    command: str,
    reason: str,
    details: str
) -> None:
    """Add example if under limit."""
    if len(examples) < 5:
        examples.append({
            "turn_index": turn_index,
            "command": command,
            "reason": reason,
            "details": details,
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
