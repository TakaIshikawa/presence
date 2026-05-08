"""Pack verification test selection analyzer for workflow hygiene reports."""

from __future__ import annotations

import re
from typing import Any, Mapping


TEST_PATH_PATTERN = re.compile(r"tests?/[^\s'\"`]+(?:\.py|\.test\.ts|\.test\.js|\.spec\.ts|\.spec\.js)?")


def analyze_pack_verification_test_selection(records: object) -> dict[str, Any]:
    """Detect misaligned verification test selection relative to expectedFiles."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack task dictionaries")

    packs: dict[str, dict[str, Any]] = {}

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_key = _pack_key(record)
        pack = packs.setdefault(pack_key, {
            "pack_key": pack_key,
            "tasks": [],
            "verification_command": "",
        })

        expected_files = _expected_files(record)
        task_verification = _verification_command(record)

        pack["tasks"].append({
            "task_id": _task_id(record, index),
            "expected_files": expected_files,
            "verification_command": task_verification,
        })

        # Store pack-level verification command
        pack_verification = _pack_verification_command(record)
        if pack_verification and not pack["verification_command"]:
            pack["verification_command"] = pack_verification

    # Analyze each pack
    wrong_module_count = 0
    missing_companion_count = 0
    overly_broad_count = 0
    packs_with_issues = 0
    examples: list[dict[str, Any]] = []

    for pack_key in sorted(packs):
        pack = packs[pack_key]
        verification_cmd = pack["verification_command"]

        # Collect all expected files across tasks in pack
        all_expected_files = []
        for task in pack["tasks"]:
            all_expected_files.extend(task["expected_files"])

        # Extract test paths from verification command
        test_paths = _extract_test_paths(verification_cmd)

        pack_has_issue = False
        example_added_for_pack = False

        # Check for wrong module: tests that don't correspond to any expectedFiles
        expected_modules = {_module_name(f) for f in all_expected_files if not _is_test_file(f)}
        tested_modules = {_module_from_test_path(p) for p in test_paths}

        # Find tested modules that don't match any expected module (allowing partial matches)
        wrong_modules = set()
        for tested in tested_modules:
            # Check if tested module matches or is part of any expected module
            has_match = any(
                tested in expected or expected in tested or
                tested.split("/")[-1] == expected.split("/")[-1]  # Match on final component
                for expected in expected_modules
            )
            if not has_match:
                wrong_modules.add(tested)

        if wrong_modules and expected_modules:
            wrong_module_count += 1
            pack_has_issue = True
            if not example_added_for_pack:
                _append_example(
                    examples,
                    pack_key,
                    "wrong_module",
                    f"testing modules not in expectedFiles: {', '.join(sorted(wrong_modules))}"
                )
                example_added_for_pack = True

        # Check for missing companion test: expectedFiles with no corresponding test
        for expected_file in all_expected_files:
            if _is_test_file(expected_file):
                continue
            module = _module_name(expected_file)
            companion_test = _companion_test_path(expected_file)
            # Check if any test path covers this module
            has_coverage = any(module in test_path or companion_test in test_path for test_path in test_paths)
            if not has_coverage and test_paths:
                missing_companion_count += 1
                pack_has_issue = True
                if not example_added_for_pack:
                    _append_example(
                        examples,
                        pack_key,
                        "missing_companion",
                        f"expectedFile {expected_file} has no test coverage in verification"
                    )
                    example_added_for_pack = True
                break  # Only flag once per pack

        # Check for overly broad patterns
        if _is_broad_pattern(verification_cmd) and len(all_expected_files) <= 3:
            overly_broad_count += 1
            pack_has_issue = True
            if not example_added_for_pack:
                _append_example(
                    examples,
                    pack_key,
                    "overly_broad",
                    f"verification uses broad pattern ({verification_cmd[:50]}) for {len(all_expected_files)} files"
                )
                example_added_for_pack = True

        if pack_has_issue:
            packs_with_issues += 1

    return {
        "pack_count": len(packs),
        "wrong_module_count": wrong_module_count,
        "missing_companion_count": missing_companion_count,
        "overly_broad_count": overly_broad_count,
        "issue_percentage": _percentage(packs_with_issues, len(packs)),
        "examples": examples[:5],
    }


def _pack_key(record: Mapping[str, Any]) -> str:
    """Extract pack key from record."""
    for key in ("executionPack", "execution_pack"):
        value = record.get(key)
        if isinstance(value, Mapping):
            nested = value.get("key") or value.get("id")
            if isinstance(nested, str) and nested.strip():
                return nested.strip()
    for key in ("pack_key", "pack"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unpackaged"


def _expected_files(record: Mapping[str, Any]) -> list[str]:
    """Extract expected files from record."""
    for key in ("expectedFiles", "expected_files"):
        value = record.get(key)
        if isinstance(value, list):
            return [str(f).strip() for f in value if isinstance(f, str) and f.strip()]
    return []


def _verification_command(record: Mapping[str, Any]) -> str:
    """Extract task-level verification command."""
    for key in ("testCommand", "test_command", "verificationCommand", "verification_command"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    return ""


def _pack_verification_command(record: Mapping[str, Any]) -> str:
    """Extract pack-level verification command."""
    for key in ("packVerificationCommand", "pack_verification_command"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return " ".join(value.split())
    # Try nested
    for key in ("executionPack", "execution_pack"):
        value = record.get(key)
        if isinstance(value, Mapping):
            verification = value.get("verificationCommand") or value.get("verification_command")
            if isinstance(verification, str) and verification.strip():
                return " ".join(verification.split())
    return ""


def _task_id(record: Mapping[str, Any], fallback: int) -> str:
    """Extract task ID from record."""
    for key in ("taskId", "task_id", "id"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return str(fallback)


def _extract_test_paths(command: str) -> list[str]:
    """Extract test file paths from verification command."""
    if not command:
        return []
    matches = TEST_PATH_PATTERN.findall(command)
    return [m.rstrip(".,;:") for m in matches]


def _module_name(file_path: str) -> str:
    """Extract module name from file path (e.g., src/foo/bar.py -> foo/bar)."""
    # Remove extension
    if "." in file_path:
        file_path = file_path.rsplit(".", 1)[0]
    # Remove src/ or lib/ prefix
    for prefix in ("src/", "lib/"):
        if file_path.startswith(prefix):
            file_path = file_path[len(prefix):]
    return file_path


def _module_from_test_path(test_path: str) -> str:
    """Extract module name from test path (e.g., tests/test_foo.py -> foo, tests/test_synthesis_analyzer.py -> synthesis/analyzer)."""
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
    # Convert underscores to slashes for nested modules (test_foo_bar -> foo/bar)
    test_path = test_path.replace("_", "/")
    return test_path


def _companion_test_path(file_path: str) -> str:
    """Generate expected companion test path for a source file."""
    module = _module_name(file_path)
    # Handle different test patterns
    if file_path.endswith(".py"):
        return f"tests/test_{module}.py"
    elif file_path.endswith((".ts", ".js")):
        return f"tests/{module}.test"
    return f"tests/test_{module}"


def _is_test_file(file_path: str) -> bool:
    """Check if file path is a test file."""
    normalized = file_path.lower()
    return (
        "test" in normalized
        or normalized.startswith("tests/")
        or normalized.startswith("test/")
        or ".test." in normalized
        or ".spec." in normalized
    )


def _is_broad_pattern(command: str) -> bool:
    """Check if command uses broad test patterns (directory-level or full suite)."""
    normalized = command.lower().strip()

    # Check for directory patterns (tests/ without specific files after)
    # "pytest tests/" is broad, but "pytest tests/test_foo.py" is not
    if " tests/" in normalized or normalized.endswith("tests/") or normalized == "pytest tests":
        # Check if there's a specific file after tests/
        after_tests = normalized.split("tests/", 1)
        if len(after_tests) > 1 and after_tests[1].strip():
            # Has content after tests/, check if it's a directory or file
            rest = after_tests[1].strip().split()[0]  # Get first token
            if "." in rest:  # Has extension, likely a file
                return False
        return True

    # Check for full suite runners without specific files
    full_suite_patterns = [
        ("npm test", True),
        ("yarn test", True),
        ("pnpm test", True),
        ("go test ./...", True),
        ("cargo test", True),
    ]

    for pattern, _ in full_suite_patterns:
        if pattern in normalized:
            return True

    # Jest/vitest without specific file paths
    if ("jest" in normalized or "vitest" in normalized) and not any(
        ext in normalized for ext in [".test.", ".spec.", ".test.ts", ".test.js", ".spec.ts", ".spec.js"]
    ):
        return True

    return False


def _append_example(
    examples: list[dict[str, Any]],
    pack_key: str,
    reason: str,
    details: str
) -> None:
    """Add example if under limit."""
    if len(examples) < 5:
        examples.append({
            "pack_key": pack_key,
            "reason": reason,
            "details": details,
        })


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
