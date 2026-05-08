"""Final answer test command alignment analyzer for workflow hygiene reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


TEST_PATH_PATTERN = re.compile(r"tests?/[^\s'\"`]+")


def analyze_final_answer_test_command_alignment(records: object) -> dict[str, Any]:
    """Detect when final answer testCommand doesn't match files changed."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    wrong_path_count = 0
    missing_coverage_count = 0
    generic_command_count = 0
    examples: list[dict[str, Any]] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        test_command = _test_command(record)
        changed_files = _changed_files(record)

        if not test_command or not changed_files:
            continue

        # Extract test paths from command
        test_paths = _extract_test_paths(test_command)

        # Determine changed modules (non-test files)
        changed_modules = {_module_name(f) for f in changed_files if not _is_test_file(f)}

        # Check for generic test commands on specific changes
        if _is_generic_command(test_command) and len(changed_files) <= 3:
            generic_command_count += 1
            _append_example(
                examples,
                _session_id(record, index),
                "generic_command",
                f"generic command ({test_command[:40]}) for {len(changed_files)} file(s)"
            )
            continue  # Don't double-flag

        # Check for wrong paths: test paths that don't match changed files
        if test_paths:
            tested_modules = {_module_from_test_path(p) for p in test_paths}
            wrong_modules = tested_modules - changed_modules

            # Allow some flexibility - if there's overlap, it's okay
            overlap = tested_modules & changed_modules
            if wrong_modules and not overlap and changed_modules:
                wrong_path_count += 1
                _append_example(
                    examples,
                    _session_id(record, index),
                    "wrong_path",
                    f"testing {', '.join(sorted(list(wrong_modules)[:2]))} but changed {', '.join(sorted(list(changed_modules)[:2]))}"
                )
                continue

        # Check for missing coverage: new/changed files without corresponding tests
        for changed_file in changed_files:
            if _is_test_file(changed_file):
                continue
            module = _module_name(changed_file)
            companion_test = _companion_test_path(module)
            # Check if test command covers this module
            has_coverage = any(
                module in test_path or companion_test in test_command
                for test_path in test_paths
            ) or _is_generic_command(test_command)

            if not has_coverage:
                missing_coverage_count += 1
                _append_example(
                    examples,
                    _session_id(record, index),
                    "missing_coverage",
                    f"changed {changed_file} but testCommand doesn't cover it"
                )
                break  # Only flag once per session

    issue_count = wrong_path_count + missing_coverage_count + generic_command_count

    return {
        "total_sessions": total_sessions,
        "wrong_path_count": wrong_path_count,
        "missing_coverage_count": missing_coverage_count,
        "generic_command_count": generic_command_count,
        "issue_percentage": _percentage(issue_count, total_sessions),
        "examples": examples[:5],
    }


def _test_command(record: Mapping[str, Any]) -> str:
    """Extract test command from record."""
    for key in ("testCommand", "test_command", "final_answer_test_command"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    # Try nested
    final_answer = record.get("finalAnswer") or record.get("final_answer")
    if isinstance(final_answer, Mapping):
        for key in ("testCommand", "test_command"):
            value = final_answer.get(key)
            if isinstance(value, str) and value.strip():
                return " ".join(value.split())
    return ""


def _changed_files(record: Mapping[str, Any]) -> list[str]:
    """Extract changed files from record."""
    for key in ("changedFiles", "changed_files", "files_changed"):
        value = record.get(key)
        if isinstance(value, list):
            return [str(f).strip() for f in value if isinstance(f, str) and f.strip()]
    return []


def _session_id(record: Mapping[str, Any], fallback: int) -> str:
    """Extract session ID from record."""
    for key in ("sessionId", "session_id", "id"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(fallback)


def _extract_test_paths(command: str) -> list[str]:
    """Extract test file paths from command."""
    if not command:
        return []
    matches = TEST_PATH_PATTERN.findall(command)
    return [m.rstrip(".,;:") for m in matches]


def _module_name(file_path: str) -> str:
    """Extract module name from file path."""
    # Remove extension
    if "." in file_path:
        file_path = file_path.rsplit(".", 1)[0]
    # Remove src/ or lib/ prefix
    for prefix in ("src/", "lib/"):
        if file_path.startswith(prefix):
            file_path = file_path[len(prefix):]
    return file_path


def _module_from_test_path(test_path: str) -> str:
    """Extract module name from test path."""
    # Remove tests/ prefix
    if test_path.startswith("tests/"):
        test_path = test_path[6:]
    elif test_path.startswith("test/"):
        test_path = test_path[5:]
    # Remove test_ prefix
    if test_path.startswith("test_"):
        test_path = test_path[5:]
    # Remove .test, .spec patterns
    test_path = test_path.replace(".test", "").replace(".spec", "")
    # Remove extension
    if "." in test_path:
        test_path = test_path.rsplit(".", 1)[0]
    # Convert underscores to slashes for nested modules
    test_path = test_path.replace("_", "/")
    return test_path


def _companion_test_path(module: str) -> str:
    """Generate expected companion test path pattern."""
    # Return a pattern that matches common test naming conventions
    parts = module.split("/")
    return f"test_{parts[-1]}"  # Match final component


def _is_test_file(file_path: str) -> bool:
    """Check if file is a test file."""
    normalized = file_path.lower()
    return (
        "test" in normalized
        or normalized.startswith("tests/")
        or normalized.startswith("test/")
        or ".test." in normalized
        or ".spec." in normalized
    )


def _is_generic_command(command: str) -> bool:
    """Check if command is generic (runs all tests)."""
    normalized = command.lower().strip()
    generic_patterns = [
        "pytest",
        "npm test",
        "yarn test",
        "pnpm test",
        "jest",
        "vitest",
        "go test ./...",
        "cargo test",
        "mvn test",
        "gradle test",
    ]

    # Check if it's just the command without specific paths
    for pattern in generic_patterns:
        # Match exact or with flags but no specific files
        if normalized == pattern or (
            normalized.startswith(pattern + " ")
            and not any(ext in normalized for ext in [".py", ".js", ".ts", ".go", ".rs", "test_", "/"])
        ):
            return True

    return False


def _append_example(
    examples: list[dict[str, Any]],
    session_id: str,
    reason: str,
    details: str
) -> None:
    """Add example if under limit."""
    if len(examples) < 5:
        examples.append({
            "session_id": session_id,
            "reason": reason,
            "details": details,
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
