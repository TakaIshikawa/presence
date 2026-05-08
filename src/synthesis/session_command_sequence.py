"""Session command sequence analyzer for workflow hygiene reports."""

from __future__ import annotations

from typing import Any


def analyze_session_command_sequence(records: object) -> dict[str, Any]:
    """Detect illogical command execution patterns in session workflow."""
    if not isinstance(records, list):
        raise ValueError("records must be a list")

    _validate_records(records)

    test_before_build_count = 0
    verify_before_install_count = 0
    redundant_install_count = 0

    examples: list[dict[str, Any]] = []

    # First pass: identify verification commands that occur before install
    verification_turns_before_install: set[int] = set()
    first_install_turn = None
    for record in records:
        normalized = _normalize_command(record["command"])
        if _is_install_command(normalized) and first_install_turn is None:
            first_install_turn = record["turn_index"]
        if first_install_turn is None and (_is_test_command(normalized) or _is_type_check_command(normalized)):
            verification_turns_before_install.add(record["turn_index"])

    # Second pass: analyze patterns
    seen_build = False
    install_turns: list[int] = []
    redundant_sequence_flagged = False

    for record in records:
        turn_index = record["turn_index"]
        command = record["command"]
        normalized = _normalize_command(command)

        is_test = _is_test_command(normalized)
        is_build = _is_build_command(normalized)
        is_install = _is_install_command(normalized)

        # Check for issues (prioritize verify_before_install for examples)
        has_verify_before_install = turn_index in verification_turns_before_install and first_install_turn is not None
        has_test_before_build = is_test and not seen_build

        # Check verify-before-install pattern (only if install exists)
        if has_verify_before_install:
            verify_before_install_count += 1
            _append_example(
                examples,
                turn_index,
                command,
                "verify_before_install",
                "verification command executed before any install command"
            )

        # Check test-before-build pattern (only if not already added as verify_before_install)
        if has_test_before_build:
            test_before_build_count += 1
            if not has_verify_before_install:  # Avoid duplicate example
                _append_example(
                    examples,
                    turn_index,
                    command,
                    "test_before_build",
                    "test command executed before any build command"
                )

        # Track builds and installs
        if is_build:
            seen_build = True

        if is_install:
            install_turns.append(turn_index)
            # Check for redundant install sequences (3+ installs within short gaps)
            if len(install_turns) >= 3:
                # Check if last 3 installs are all within reasonable proximity
                recent_three = install_turns[-3:]
                if recent_three[-1] - recent_three[0] <= 6:  # All within 6 turns
                    # Only flag once per sequence
                    if not redundant_sequence_flagged:
                        redundant_install_count += 1
                        _append_example(
                            examples,
                            turn_index,
                            command,
                            "redundant_install",
                            f"multiple installs in short sequence (turns {recent_three[0]}-{recent_three[-1]})"
                        )
                        redundant_sequence_flagged = True
                else:
                    # Gap detected, reset flag
                    redundant_sequence_flagged = False

    total_commands = len(records)
    issue_count = test_before_build_count + verify_before_install_count + redundant_install_count

    return {
        "total_commands": total_commands,
        "test_before_build_count": test_before_build_count,
        "verify_before_install_count": verify_before_install_count,
        "redundant_install_count": redundant_install_count,
        "issue_percentage": _percentage(issue_count, total_commands),
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

        turn_index = record["turn_index"]
        command = record["command"]

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

        if turn_index < prev_turn:
            raise ValueError("records must be ordered by turn_index")
        prev_turn = turn_index


def _normalize_command(command: str) -> str:
    """Normalize command for comparison."""
    return " ".join(command.strip().lower().split())


def _is_test_command(normalized: str) -> bool:
    """Check if command is a test command."""
    test_patterns = [
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
    ]
    return any(pattern in normalized for pattern in test_patterns)


def _is_build_command(normalized: str) -> bool:
    """Check if command is a build command."""
    build_patterns = [
        "build",
        "tsc",
        "webpack",
        "vite build",
        "npm run build",
        "yarn build",
        "pnpm build",
        "cargo build",
        "mvn compile",
        "gradle build",
        "make",
    ]
    return any(pattern in normalized for pattern in build_patterns)


def _is_install_command(normalized: str) -> bool:
    """Check if command is an install/dependency command."""
    install_patterns = [
        "npm install",
        "npm i",
        "yarn install",
        "yarn add",
        "pnpm install",
        "pnpm add",
        "pip install",
        "poetry install",
        "uv sync",
        "cargo add",
        "mvn install",
    ]
    return any(pattern in normalized for pattern in install_patterns)


def _is_type_check_command(normalized: str) -> bool:
    """Check if command is a type check or lint command (not test)."""
    type_check_patterns = [
        "tsc",
        "mypy",
        "pylint",
        "eslint",
        "flake8",
        "ruff check",
        "clippy",
    ]
    return any(pattern in normalized for pattern in type_check_patterns)


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
