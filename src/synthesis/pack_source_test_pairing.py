"""Pack source-test pairing analyzer for test coverage validation.

Validates source-test file pairing in expectedFiles to ensure each source file
has a companion test file following project conventions. Checks pairing ratio
against project standards and identifies coverage gaps.

Pairing metrics:
- Pairing score: Ratio of source files with companion test files (0-1)
- Unpaired source files: Source files without corresponding tests
- Orphaned test files: Test files without corresponding source
- Convention compliance: Test files following naming conventions
- Test coverage mention: Acceptance criteria mentioning tests

Project conventions:
- Source files: src/**/*.py, lib/**/*.py
- Test files: tests/test_*.py, tests/**/ test_*.py
- Pairing pattern: src/foo.py → tests/test_foo.py
- Project standard pairing ratio: 0.64 (observed from project stats)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_source_test_pairing(records: object) -> dict[str, Any]:
    """Analyze source-test file pairing quality within packs.

    Validates that each source file in expectedFiles has a companion test file
    following project conventions. Computes pairing ratio and identifies gaps.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - expected_files: List of expected file paths
            - acceptance_criteria: List of acceptance criteria strings
            - verification_command: Verification command string

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - total_source_files: Total source files across all tasks
            - total_test_files: Total test files across all tasks
            - paired_source_files: Source files with companion test files
            - unpaired_source_files_count: Source files without tests
            - unpaired_source_files: List of unpaired source file paths
            - orphaned_test_files_count: Test files without source
            - orphaned_test_files: List of orphaned test file paths
            - pairing_score: Ratio of paired source files (0-1)
            - pairing_ratio: Percentage of paired source files
            - project_standard_ratio: Project standard pairing ratio (0.64)
            - meets_project_standard: Boolean if meets standard
            - convention_compliant_tests: Test files following conventions
            - convention_violation_count: Test files violating conventions
            - tasks_mentioning_tests: Tasks with test coverage in ACs
            - well_paired_tasks: Tasks with all sources paired
            - poorly_paired_tasks: Tasks with <50% pairing

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    total_source_files = 0
    total_test_files = 0
    paired_source_files = 0

    unpaired_source_files: list[str] = []
    orphaned_test_files: list[str] = []

    convention_compliant_tests = 0
    convention_violation_count = 0

    tasks_mentioning_tests = 0
    well_paired_tasks = 0
    poorly_paired_tasks = 0

    # Project standard from observed stats
    PROJECT_STANDARD_RATIO = 0.64

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        expected_files = record.get("expected_files")
        if not isinstance(expected_files, list):
            expected_files = []

        acceptance_criteria = record.get("acceptance_criteria")
        if not isinstance(acceptance_criteria, list):
            acceptance_criteria = []

        # Categorize files
        source_files: list[str] = []
        test_files: list[str] = []

        for file_path in expected_files:
            if not isinstance(file_path, str):
                continue

            file_path = file_path.strip()
            if not file_path:
                continue

            if _is_test_file(file_path):
                test_files.append(file_path)
                total_test_files += 1

                # Check convention compliance
                if _follows_test_conventions(file_path):
                    convention_compliant_tests += 1
                else:
                    convention_violation_count += 1
            elif _is_source_file(file_path):
                source_files.append(file_path)
                total_source_files += 1

        # Check pairing for this task
        task_paired_count = 0
        task_unpaired_sources: list[str] = []
        task_orphaned_tests: list[str] = []

        for source_file in source_files:
            if _has_test_pair(source_file, test_files):
                task_paired_count += 1
                paired_source_files += 1
            else:
                task_unpaired_sources.append(source_file)
                unpaired_source_files.append(source_file)

        # Check for orphaned tests
        for test_file in test_files:
            if not _has_source_pair(test_file, source_files):
                task_orphaned_tests.append(test_file)
                orphaned_test_files.append(test_file)

        # Categorize task pairing quality
        if source_files:
            task_pairing_ratio = task_paired_count / len(source_files)
            if task_pairing_ratio >= 1.0:
                well_paired_tasks += 1
            elif task_pairing_ratio < 0.5:
                poorly_paired_tasks += 1

        # Check if acceptance criteria mention tests
        if _mentions_test_coverage(acceptance_criteria):
            tasks_mentioning_tests += 1

    # Calculate metrics
    pairing_score = paired_source_files / total_source_files if total_source_files > 0 else 0.0
    pairing_score = round(pairing_score, 3)

    pairing_ratio = _percentage(paired_source_files, total_source_files)
    meets_project_standard = pairing_score >= PROJECT_STANDARD_RATIO

    return {
        "total_tasks": total_tasks,
        "total_source_files": total_source_files,
        "total_test_files": total_test_files,
        "paired_source_files": paired_source_files,
        "unpaired_source_files_count": len(unpaired_source_files),
        "unpaired_source_files": unpaired_source_files[:10],  # Limit to first 10
        "orphaned_test_files_count": len(orphaned_test_files),
        "orphaned_test_files": orphaned_test_files[:10],  # Limit to first 10
        "pairing_score": pairing_score,
        "pairing_ratio": pairing_ratio,
        "project_standard_ratio": PROJECT_STANDARD_RATIO,
        "meets_project_standard": meets_project_standard,
        "convention_compliant_tests": convention_compliant_tests,
        "convention_violation_count": convention_violation_count,
        "tasks_mentioning_tests": tasks_mentioning_tests,
        "well_paired_tasks": well_paired_tasks,
        "poorly_paired_tasks": poorly_paired_tasks,
    }


def _is_source_file(file_path: str) -> bool:
    """Check if file is a source file (not test, not config).

    Source files:
    - src/**/*.py
    - lib/**/*.py
    - *.py in project root (excluding setup.py, conftest.py, etc.)

    Args:
        file_path: File path to check

    Returns:
        True if source file
    """
    if not file_path.endswith(".py"):
        return False

    # Exclude test files
    if _is_test_file(file_path):
        return False

    # Exclude common config/setup files
    excluded_patterns = [
        "setup.py",
        "conftest.py",
        "__init__.py",
        "pyproject.toml",
        "setup.cfg",
    ]

    file_name = file_path.split("/")[-1]
    if file_name in excluded_patterns:
        return False

    # Include src/, lib/ directories or root-level .py files
    if file_path.startswith("src/") or file_path.startswith("lib/"):
        return True

    # Include synthesis/ and evaluation/ directories (project-specific)
    if file_path.startswith("synthesis/") or file_path.startswith("evaluation/"):
        return True

    # Root-level .py files (excluding excluded patterns)
    if "/" not in file_path and file_path.endswith(".py"):
        return True

    return False


def _is_test_file(file_path: str) -> bool:
    """Check if file is a test file.

    Test file patterns:
    - tests/**/*.py
    - test_*.py
    - *_test.py

    Args:
        file_path: File path to check

    Returns:
        True if test file
    """
    if not file_path.endswith(".py"):
        return False

    # Check directory
    if file_path.startswith("tests/"):
        return True

    # Check filename pattern
    file_name = file_path.split("/")[-1]
    if file_name.startswith("test_") or file_name.endswith("_test.py"):
        return True

    return False


def _follows_test_conventions(test_file: str) -> bool:
    """Check if test file follows project conventions.

    Conventions:
    - Located in tests/ directory
    - Filename starts with test_
    - Extension is .py

    Args:
        test_file: Test file path

    Returns:
        True if follows conventions
    """
    if not test_file.endswith(".py"):
        return False

    if not test_file.startswith("tests/"):
        return False

    file_name = test_file.split("/")[-1]
    if not file_name.startswith("test_"):
        return False

    return True


def _has_test_pair(source_file: str, test_files: list[str]) -> bool:
    """Check if source file has a companion test file.

    Pairing patterns:
    - src/foo.py → tests/test_foo.py
    - src/bar/baz.py → tests/test_bar_baz.py or tests/bar/test_baz.py
    - synthesis/foo.py → tests/test_foo.py
    - lib/foo.py → tests/test_foo.py

    Args:
        source_file: Source file path
        test_files: List of test file paths

    Returns:
        True if test pair exists
    """
    # Extract base name without extension
    source_name = source_file.split("/")[-1].replace(".py", "")

    # Expected test patterns
    expected_test_patterns = [
        f"tests/test_{source_name}.py",
        f"test_{source_name}.py",
    ]

    # Also check path-aware patterns
    # src/foo/bar.py → tests/test_foo_bar.py or tests/foo/test_bar.py
    if "/" in source_file:
        parts = source_file.replace(".py", "").split("/")
        # Remove first directory (src, lib, synthesis, etc.)
        if len(parts) > 1:
            path_parts = parts[1:]
            combined_name = "_".join(path_parts)
            expected_test_patterns.append(f"tests/test_{combined_name}.py")

            # Also check directory structure
            if len(path_parts) > 1:
                subdir = "/".join(path_parts[:-1])
                file_name = path_parts[-1]
                expected_test_patterns.append(f"tests/{subdir}/test_{file_name}.py")

    # Check if any expected pattern matches
    for test_file in test_files:
        if test_file in expected_test_patterns:
            return True

        # Fuzzy match: test file contains source name
        test_name = test_file.split("/")[-1].replace("test_", "").replace(".py", "")
        if source_name in test_name or test_name in source_name:
            return True

    return False


def _has_source_pair(test_file: str, source_files: list[str]) -> bool:
    """Check if test file has a companion source file.

    Reverse pairing patterns:
    - tests/test_foo.py → src/foo.py, synthesis/foo.py, lib/foo.py
    - tests/bar/test_baz.py → src/bar/baz.py

    Args:
        test_file: Test file path
        source_files: List of source file paths

    Returns:
        True if source pair exists
    """
    # Extract base name
    test_name = test_file.split("/")[-1].replace("test_", "").replace(".py", "")

    # Expected source patterns
    expected_source_patterns = [
        f"src/{test_name}.py",
        f"lib/{test_name}.py",
        f"synthesis/{test_name}.py",
        f"evaluation/{test_name}.py",
        f"{test_name}.py",
    ]

    # Path-aware patterns
    if "/" in test_file:
        parts = test_file.replace(".py", "").split("/")
        if len(parts) > 2 and parts[0] == "tests":
            # tests/foo/test_bar.py → src/foo/bar.py
            subdir = "/".join(parts[1:-1])
            file_name = parts[-1].replace("test_", "")
            expected_source_patterns.append(f"src/{subdir}/{file_name}.py")
            expected_source_patterns.append(f"synthesis/{subdir}/{file_name}.py")

    # Check if any expected pattern matches
    for source_file in source_files:
        if source_file in expected_source_patterns:
            return True

        # Fuzzy match: source file contains test name
        source_name = source_file.split("/")[-1].replace(".py", "")
        if test_name in source_name or source_name in test_name:
            return True

    return False


def _mentions_test_coverage(acceptance_criteria: list[Any]) -> bool:
    """Check if acceptance criteria mention test coverage.

    Looks for keywords:
    - test, tests, testing
    - coverage
    - test suite
    - pytest, unittest

    Args:
        acceptance_criteria: List of acceptance criteria strings

    Returns:
        True if tests are mentioned
    """
    if not isinstance(acceptance_criteria, list):
        return False

    test_keywords = [
        "test",
        "tests",
        "testing",
        "coverage",
        "test suite",
        "pytest",
        "unittest",
        "test coverage",
    ]

    for criterion in acceptance_criteria:
        if not isinstance(criterion, str):
            continue

        criterion_lower = criterion.lower()
        for keyword in test_keywords:
            if keyword in criterion_lower:
                return True

    return False


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
