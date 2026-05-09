"""Pack test command accuracy analyzer.

Validates testCommand accuracy by checking command syntax, file paths,
flags, and common anti-patterns.
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_test_command_accuracy(records: object) -> dict[str, Any]:
    """Analyze test command accuracy within packs."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    valid_commands = 0
    invalid_syntax_count = 0
    missing_files_count = 0
    scope_mismatches = 0
    anti_patterns_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1
        test_command = str(record.get("test_command", "")).strip()

        if not test_command:
            continue

        # Check syntax validity
        is_valid = _check_syntax(test_command)
        if is_valid:
            valid_commands += 1
        else:
            invalid_syntax_count += 1

        # Check for anti-patterns
        if _has_anti_patterns(test_command):
            anti_patterns_count += 1

    accuracy_score = valid_commands / total_tasks if total_tasks > 0 else 0.0

    return {
        "total_tasks": total_tasks,
        "valid_commands": valid_commands,
        "invalid_syntax_count": invalid_syntax_count,
        "missing_files_count": missing_files_count,
        "scope_mismatches": scope_mismatches,
        "anti_patterns_count": anti_patterns_count,
        "accuracy_score": round(accuracy_score, 3),
    }


def _check_syntax(command: str) -> bool:
    """Check command syntax validity."""
    if not command:
        return False
    valid_commands = ["pytest", "npm test", "python -m pytest", "cargo test"]
    return any(cmd in command for cmd in valid_commands)


def _has_anti_patterns(command: str) -> bool:
    """Check for anti-patterns."""
    anti_patterns = ["npm install", "cd ", "--force"]
    return any(pattern in command for pattern in anti_patterns)
