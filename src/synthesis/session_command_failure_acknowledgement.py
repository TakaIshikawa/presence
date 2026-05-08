"""Session command failure acknowledgement analyzer for workflow reports."""

from __future__ import annotations

from typing import Any, Mapping


ACKNOWLEDGEMENT_TERMS = (
<<<<<<< HEAD
    "failure",
    "error",
    "exit code",
    "traceback",
    "retry",
    "fix",
    "failed",
=======
    "fail",
    "error",
    "exit",
    "traceback",
    "retry",
    "fix",
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
    "issue",
    "problem",
)


def analyze_session_command_failure_acknowledgement(records: object) -> dict[str, Any]:
<<<<<<< HEAD
    """Detect failed shell commands whose output is not acknowledged in later assistant turns."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command event dictionaries")
=======
    """Detect failed commands whose output is not acknowledged in later turns."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of command failure record dictionaries")
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    total_failures = 0
    acknowledged_failures = 0
    unacknowledged_failures = 0
    examples: list[dict[str, Any]] = []

<<<<<<< HEAD
    prev_turn = -1

    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("records must be a list of command event dictionaries")

        turn_index = record.get("turn_index")
        command = _string(record.get("command"))
        exit_code = record.get("exit_code")
        output_excerpt = _string(record.get("output_excerpt"))
        following_response = _string(record.get("following_response"))

        if not isinstance(turn_index, int) or isinstance(turn_index, bool):
            raise ValueError("turn_index must be an integer")
        if turn_index < 0:
            raise ValueError("turn_index must be non-negative")
        if turn_index <= prev_turn:
            raise ValueError("records must be ordered by turn_index")
        prev_turn = turn_index

        if not isinstance(exit_code, int) or isinstance(exit_code, bool):
            raise ValueError("exit_code must be an integer")

        # Only process failed commands (non-zero exit code)
=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
        if exit_code == 0:
            continue

        total_failures += 1

<<<<<<< HEAD
=======
        command = _string(record.get("command"))
        output_excerpt = _string(record.get("output_excerpt"))
        following_response = _string(record.get("following_response"))

>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
        if _is_acknowledged(following_response):
            acknowledged_failures += 1
        else:
            unacknowledged_failures += 1
<<<<<<< HEAD
            _example(examples, turn_index, command, exit_code, output_excerpt)
=======
            _example(examples, turn_index, command, exit_code, output_excerpt, following_response)
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD

    acknowledgement_rate = _percentage(acknowledged_failures, total_failures)

    return {
        "total_failures": total_failures,
        "acknowledged_failures": acknowledged_failures,
        "unacknowledged_failures": unacknowledged_failures,
        "acknowledgement_rate": acknowledgement_rate,
        "examples": examples,
    }


<<<<<<< HEAD
=======
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


>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
def _string(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _is_acknowledged(following_response: str) -> bool:
<<<<<<< HEAD
    """Check if the following response contains acknowledgement terms."""
    if not following_response:
        return False

=======
    if not following_response:
        return False
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD
    normalized = following_response.lower()
    return any(term in normalized for term in ACKNOWLEDGEMENT_TERMS)


def _example(
    examples: list[dict[str, Any]],
    turn_index: int,
    command: str,
    exit_code: int,
    output_excerpt: str,
<<<<<<< HEAD
) -> None:
    if len(examples) < 5:
        examples.append(
            {
                "turn_index": turn_index,
                "command": command,
                "exit_code": exit_code,
                "output_excerpt": output_excerpt,
            }
        )
=======
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
>>>>>>> relay/claude-code/add-execution-pack-expected-file-drift-analyzer-01KR3ATD


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
