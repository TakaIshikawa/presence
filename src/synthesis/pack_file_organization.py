"""Pack file organization and companion test file discipline analyzer.

Analyzes file structure and companion test discipline across session packs.
For each session, extracts expectedFiles from task metadata and actual changed
files, then evaluates test companion discipline, file location consistency,
naming conventions, orphaned tests, and untested changes.

Metrics tracked:
- source_files_with_tests_count: Source files with companion test files
- source_files_without_tests_count: Source files missing companion tests
- orphaned_test_files_count: Test files without corresponding source files
- test_to_source_ratio: Ratio of test files to source files
- naming_convention_violations: Test files not following tests/test_*.py

Scores returned (0-1):
- test_companion_discipline: Proportion of source files with companion tests
- file_organization_correctness: Files placed in correct directories
- naming_consistency: Test files following naming conventions
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_file_organization(records: object) -> dict[str, Any]:
    """Analyze file organization and test companion discipline across pack sessions.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - expected_files: List of expected file paths from task metadata
            - actual_changed_files: List of files actually changed in session

    Returns:
        Dict with metrics and scores for file organization quality.

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0

    # Aggregate file sets across all sessions
    all_source_files: list[str] = []
    all_test_files: list[str] = []
    all_other_files: list[str] = []

    # Track per-session results
    sessions_with_full_pairing = 0
    sessions_with_no_tests = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Collect all unique files from both expected and actual
        combined_files = _collect_files(record)

        session_sources: list[str] = []
        session_tests: list[str] = []

        for file_path in combined_files:
            if _is_test_file(file_path):
                session_tests.append(file_path)
                all_test_files.append(file_path)
            elif _is_source_file(file_path):
                session_sources.append(file_path)
                all_source_files.append(file_path)
            else:
                all_other_files.append(file_path)

        # Evaluate session-level pairing
        if session_sources:
            paired = sum(1 for s in session_sources if _has_companion_test(s, session_tests))
            if paired == len(session_sources):
                sessions_with_full_pairing += 1
            if not session_tests:
                sessions_with_no_tests += 1

    # Deduplicate for cross-session analysis
    unique_sources = list(dict.fromkeys(all_source_files))
    unique_tests = list(dict.fromkeys(all_test_files))
    unique_others = list(dict.fromkeys(all_other_files))
    all_unique = unique_sources + unique_tests + unique_others

    # Metric: test companion discipline
    sources_with_tests = 0
    sources_without_tests = 0
    for source in unique_sources:
        if _has_companion_test(source, unique_tests):
            sources_with_tests += 1
        else:
            sources_without_tests += 1

    # Metric: orphaned test files
    orphaned_tests: list[str] = []
    for test in unique_tests:
        if not _has_companion_source(test, unique_sources):
            orphaned_tests.append(test)

    # Metric: test to source ratio
    test_to_source_ratio = (
        round(len(unique_tests) / len(unique_sources), 3)
        if unique_sources
        else 0.0
    )

    # Metric: naming convention violations
    naming_violations: list[str] = []
    for test in unique_tests:
        if not _follows_naming_convention(test):
            naming_violations.append(test)

    # Metric: file placement correctness
    misplaced_files: list[str] = []
    for file_path in all_unique:
        if not _is_correctly_placed(file_path):
            misplaced_files.append(file_path)

    # Score: test_companion_discipline (0-1)
    test_companion_discipline = (
        round(sources_with_tests / len(unique_sources), 3)
        if unique_sources
        else 1.0
    )

    # Score: file_organization_correctness (0-1)
    correctly_placed = len(all_unique) - len(misplaced_files)
    file_organization_correctness = (
        round(correctly_placed / len(all_unique), 3)
        if all_unique
        else 1.0
    )

    # Score: naming_consistency (0-1)
    naming_consistency = (
        round((len(unique_tests) - len(naming_violations)) / len(unique_tests), 3)
        if unique_tests
        else 1.0
    )

    return {
        "total_sessions": total_sessions,
        "source_files_with_tests_count": sources_with_tests,
        "source_files_without_tests_count": sources_without_tests,
        "orphaned_test_files_count": len(orphaned_tests),
        "orphaned_test_files": orphaned_tests[:10],
        "test_to_source_ratio": test_to_source_ratio,
        "naming_convention_violations": len(naming_violations),
        "naming_violation_files": naming_violations[:10],
        "misplaced_files_count": len(misplaced_files),
        "misplaced_files": misplaced_files[:10],
        "sessions_with_full_pairing": sessions_with_full_pairing,
        "sessions_with_no_tests": sessions_with_no_tests,
        "test_companion_discipline": test_companion_discipline,
        "file_organization_correctness": file_organization_correctness,
        "naming_consistency": naming_consistency,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure with default scores."""
    return {
        "total_sessions": 0,
        "source_files_with_tests_count": 0,
        "source_files_without_tests_count": 0,
        "orphaned_test_files_count": 0,
        "orphaned_test_files": [],
        "test_to_source_ratio": 0.0,
        "naming_convention_violations": 0,
        "naming_violation_files": [],
        "misplaced_files_count": 0,
        "misplaced_files": [],
        "sessions_with_full_pairing": 0,
        "sessions_with_no_tests": 0,
        "test_companion_discipline": 1.0,
        "file_organization_correctness": 1.0,
        "naming_consistency": 1.0,
    }


def _collect_files(record: Mapping) -> list[str]:
    """Collect unique file paths from expected_files and actual_changed_files."""
    seen: set[str] = set()
    result: list[str] = []

    for key in ("expected_files", "actual_changed_files"):
        files = record.get(key)
        if not isinstance(files, list):
            continue
        for f in files:
            if isinstance(f, str) and f.strip() and f.strip() not in seen:
                seen.add(f.strip())
                result.append(f.strip())

    return result


def _is_test_file(file_path: str) -> bool:
    """Check if file is a test file."""
    if not file_path.endswith(".py"):
        return False
    if file_path.startswith("tests/"):
        return True
    file_name = file_path.split("/")[-1]
    return file_name.startswith("test_") or file_name.endswith("_test.py")


def _is_source_file(file_path: str) -> bool:
    """Check if file is a source file (not test, not config)."""
    if not file_path.endswith(".py"):
        return False
    if _is_test_file(file_path):
        return False

    file_name = file_path.split("/")[-1]
    excluded = {"setup.py", "conftest.py", "__init__.py"}
    if file_name in excluded:
        return False

    if file_path.startswith(("src/", "lib/", "synthesis/", "evaluation/")):
        return True

    # Root-level .py files
    if "/" not in file_path:
        return True

    return False


def _follows_naming_convention(test_file: str) -> bool:
    """Check if test file follows tests/test_*.py convention."""
    if not test_file.endswith(".py"):
        return False
    if not test_file.startswith("tests/"):
        return False
    file_name = test_file.split("/")[-1]
    return file_name.startswith("test_")


def _is_correctly_placed(file_path: str) -> bool:
    """Check if file is placed in the correct directory per project structure.

    Expected placement:
    - Source files: src/*, lib/*, synthesis/*, evaluation/*
    - Test files: tests/*
    - Config files: root level
    """
    if not file_path.endswith(".py"):
        # Non-Python files are outside scope; assume correct
        return True

    file_name = file_path.split("/")[-1]

    # Test files should be in tests/
    if file_name.startswith("test_") or file_name.endswith("_test.py"):
        return file_path.startswith("tests/")

    # Config files at root are fine
    if file_name in {"setup.py", "conftest.py", "__init__.py"}:
        return True

    # Source files should be in recognized source directories
    if file_path.startswith(("src/", "lib/", "synthesis/", "evaluation/")):
        return True

    # Root-level scripts are acceptable
    if "/" not in file_path:
        return True

    # Files in unrecognized directories
    return False


def _has_companion_test(source_file: str, test_files: list[str]) -> bool:
    """Check if a source file has a companion test file in the list."""
    source_name = source_file.split("/")[-1].replace(".py", "")

    # Build expected test file names
    expected_patterns = [
        f"tests/test_{source_name}.py",
        f"test_{source_name}.py",
    ]

    # Path-aware: src/synthesis/foo.py -> tests/test_foo.py
    if "/" in source_file:
        parts = source_file.replace(".py", "").split("/")
        if len(parts) > 1:
            path_parts = parts[1:]
            combined = "_".join(path_parts)
            expected_patterns.append(f"tests/test_{combined}.py")

            if len(path_parts) > 1:
                subdir = "/".join(path_parts[:-1])
                fname = path_parts[-1]
                expected_patterns.append(f"tests/{subdir}/test_{fname}.py")

    for test_file in test_files:
        if test_file in expected_patterns:
            return True

        # Fuzzy: test file name contains source name
        test_name = test_file.split("/")[-1].replace("test_", "").replace(".py", "")
        if source_name == test_name:
            return True

    return False


def _has_companion_source(test_file: str, source_files: list[str]) -> bool:
    """Check if a test file has a companion source file in the list."""
    test_name = test_file.split("/")[-1].replace("test_", "").replace(".py", "")

    expected_patterns = [
        f"src/{test_name}.py",
        f"src/synthesis/{test_name}.py",
        f"lib/{test_name}.py",
        f"synthesis/{test_name}.py",
        f"evaluation/{test_name}.py",
        f"{test_name}.py",
    ]

    for source_file in source_files:
        if source_file in expected_patterns:
            return True

        source_name = source_file.split("/")[-1].replace(".py", "")
        if source_name == test_name:
            return True

    return False
