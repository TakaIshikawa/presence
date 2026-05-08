"""Session bash command error handling analyzer for workflow hygiene reports.

Analyzes patterns where bash commands fail but the agent continues without
acknowledgement or retry. This detects workflow problems where errors are
silently ignored, leading to cascading failures or incomplete task execution.

Error indicators:
- Non-zero exit codes (1-255)
- stderr output containing error messages
- Timeout errors (exit code 124 or timeout indicator)
- Command not found (exit code 127)
- Permission errors (exit code 126)

Acknowledgement patterns:
- Next turn references the error or failure
- Agent attempts to fix or retry the command
- Agent acknowledges and explains the issue
"""

from __future__ import annotations

from typing import Any, Mapping


# Error acknowledgement terms to detect in following responses
ACKNOWLEDGEMENT_TERMS = (
    "error",
    "fail",
    "exit",
    "stderr",
    "timeout",
    "timed",
    "retry",
    "fix",
    "issue",
    "problem",
    "not found",
    "permission",
    "denied",
)


def analyze_session_bash_command_error_handling(records: object) -> dict[str, Any]:
    """Analyze bash command error handling patterns in a session.

    Detects sequences where bash commands fail but the agent doesn't acknowledge
    or address the failure in subsequent turns.

    Args:
        records: List of bash command execution dictionaries with keys:
            - command: The bash command executed
            - exit_code: Exit code (0 = success, non-zero = failure)
            - stderr: Standard error output
            - stdout: Standard output
            - timed_out: Boolean indicating timeout
            - following_response: Agent's response in next turn
            - turn_index: Turn number when command executed

    Returns:
        Dict with:
            - total_commands: Total bash commands executed
            - failed_commands: Number of commands with errors
            - acknowledged_failures: Number of failures acknowledged
            - unhandled_errors: Number of failures not acknowledged
            - error_acknowledgement_rate: Percentage of acknowledged failures
            - error_types: Dict mapping error types to counts
            - examples: Sample unhandled errors

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of bash command dictionaries")

    total_commands = 0
    failed_commands = 0
    acknowledged_failures = 0
    unhandled_errors = 0
    error_types: dict[str, int] = {
        "non_zero_exit": 0,
        "timeout": 0,
        "command_not_found": 0,
        "permission_denied": 0,
        "stderr_output": 0,
    }
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        total_commands += 1

        # Check for various error indicators
        exit_code = _number(record.get("exit_code"))
        stderr = _string(record.get("stderr"))
        timed_out = record.get("timed_out") is True
        command = _string(record.get("command"))
        following_response = _string(record.get("following_response"))
        turn_index = record.get("turn_index", index)

        # Determine if this is a failure
        is_failure = False
        error_type = None

        if timed_out:
            is_failure = True
            error_type = "timeout"
            error_types["timeout"] += 1
        elif exit_code == 127:
            is_failure = True
            error_type = "command_not_found"
            error_types["command_not_found"] += 1
        elif exit_code == 126:
            is_failure = True
            error_type = "permission_denied"
            error_types["permission_denied"] += 1
        elif exit_code is not None and exit_code != 0:
            is_failure = True
            error_type = "non_zero_exit"
            error_types["non_zero_exit"] += 1

        # Check for stderr even if exit code is 0 (some commands report errors but exit 0)
        if stderr:
            error_types["stderr_output"] += 1
            if not is_failure:
                # Only count as failure if stderr contains error-like content
                if _contains_error_indicators(stderr):
                    is_failure = True
                    error_type = "stderr_output"

        if not is_failure:
            continue

        failed_commands += 1

        # Check if the failure was acknowledged
        if _is_acknowledged(following_response):
            acknowledged_failures += 1
        else:
            unhandled_errors += 1
            _add_example(examples, turn_index, command, exit_code, stderr, error_type)

    error_acknowledgement_rate = _percentage(acknowledged_failures, failed_commands)

    return {
        "total_commands": total_commands,
        "failed_commands": failed_commands,
        "acknowledged_failures": acknowledged_failures,
        "unhandled_errors": unhandled_errors,
        "error_acknowledgement_rate": error_acknowledgement_rate,
        "error_types": error_types,
        "examples": examples[:5],  # Limit to 5 examples
    }


def _number(value: object) -> int | None:
    """Extract integer from value, handling various types."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _contains_error_indicators(text: str) -> bool:
    """Check if text contains error-like content."""
    if not text:
        return False
    normalized = text.lower()
    error_indicators = ("error", "fatal", "exception", "failed", "traceback", "abort")
    return any(indicator in normalized for indicator in error_indicators)


def _is_acknowledged(following_response: str) -> bool:
    """Check if the following response acknowledges the error."""
    if not following_response:
        return False
    normalized = following_response.lower()
    return any(term in normalized for term in ACKNOWLEDGEMENT_TERMS)


def _add_example(
    examples: list[dict[str, Any]],
    turn_index: int,
    command: str,
    exit_code: int | None,
    stderr: str,
    error_type: str | None,
) -> None:
    """Add an example if we have fewer than 5."""
    if len(examples) < 5:
        examples.append({
            "turn_index": turn_index,
            "command": command[:100] if command else "",  # Limit command length
            "exit_code": exit_code,
            "stderr_excerpt": stderr[:200] if stderr else "",  # Limit stderr length
            "error_type": error_type or "unknown",
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
