"""Session command failure acknowledgement analyzer for workflow reports."""

from __future__ import annotations

from typing import Any, Mapping


ACKNOWLEDGEMENT_TERMS = (
    "fail",
    "error",
    "exit",
    "traceback",
    "retry",
    "fix",
    "issue",
    "problem",
)


def analyze_session_command_failure_acknowledgement(records: object) -> dict[str, Any]:
    """Detect failed commands whose output is not acknowledged in later turns."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command failure record dictionaries")

    total_failures = 0
    acknowledged_failures = 0
    unacknowledged_failures = 0
    examples: list[dict[str, Any]] = []

    previous_turn: int | None = None

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            raise ValueError(f"record at index {index} is not a dictionary")

        turn_index = _turn_index(record, index)
        if previous_turn is not None and turn_index < previous_turn:
            raise ValueError(f"record at index {index} has unordered turn_index")
        previous_turn = turn_index

        exit_code = _exit_code(record, index)

        # Only process failures
        if exit_code == 0:
            continue

        total_failures += 1

        command = _string(record.get("command"))
        output_excerpt = _string(record.get("output_excerpt"))
        following_response = _string(record.get("following_response"))

        if _is_acknowledged(following_response):
            acknowledged_failures += 1
        else:
            unacknowledged_failures += 1
            _example(examples, turn_index, command, exit_code, output_excerpt, following_response)

    acknowledgement_rate = _percentage(acknowledged_failures, total_failures)

    return {
        "total_failures": total_failures,
        "acknowledged_failures": acknowledged_failures,
        "unacknowledged_failures": unacknowledged_failures,
        "acknowledgement_rate": acknowledgement_rate,
        "examples": examples,
    }


def _turn_index(record: Mapping[str, Any], index: int) -> int:
    value = record.get("turn_index")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record at index {index} has invalid turn_index")
    if value < 0:
        raise ValueError(f"record at index {index} has negative turn_index")
    return value


def _exit_code(record: Mapping[str, Any], index: int) -> int:
    value = record.get("exit_code")
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"record at index {index} has invalid exit_code")
    return value


def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _is_acknowledged(following_response: str) -> bool:
    if not following_response:
        return False
    normalized = following_response.lower()
    return any(term in normalized for term in ACKNOWLEDGEMENT_TERMS)


def _example(
    examples: list[dict[str, Any]],
    turn_index: int,
    command: str,
    exit_code: int,
    output_excerpt: str,
    following_response: str,
) -> None:
    if len(examples) < 5:
        examples.append({
            "turn_index": turn_index,
            "command": command,
            "exit_code": exit_code,
            "output_excerpt": output_excerpt,
            "following_response": following_response,
        })


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
