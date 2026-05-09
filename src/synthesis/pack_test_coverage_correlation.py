"""Pack test coverage correlation analyzer.

Correlates test coverage with pack outcomes to identify patterns where better
test discipline improves success rates. Analyzes test-to-source file ratios,
test command specificity, and execution patterns.

Coverage metrics:
- Test-to-source ratio: Ratio of test files to source files in expectedFiles
- Test command presence: Tasks with vs without test commands
- Test command specificity: File-level vs package-level test commands
- Test execution patterns: Tests created but not executed

Correlation analysis:
- Coverage vs success rate: How test coverage correlates with outcomes
- Test specificity vs quality: File-level tests vs broad package tests
- Missing companion tests: Source files without corresponding test files
- Abandoned tests: Test files created but not executed

Quality indicators:
- High correlation: Better test coverage = higher success rate
- Missing tests: Source files lacking test companions
- Broad testing: Package-level tests that may miss specific issues
- Test abandonment: Tests created but never executed
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_test_coverage_correlation(records: object) -> dict[str, Any]:
    """Analyze test coverage correlation with pack outcomes.

    Correlates test coverage patterns with success rates to identify
    the impact of test discipline on pack quality.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - expected_files: List of expected file paths
            - test_command: Task-level test command
            - verification_command: Pack-level verification command
            - outcome: Task outcome (completed|failed|skipped)
            - has_tests_executed: Boolean if tests were run

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - tasks_with_test_command: Tasks with test command
            - tasks_with_tests_executed: Tasks where tests ran
            - total_expected_files: Total expected files across tasks
            - source_file_count: Number of source files
            - test_file_count: Number of test files
            - test_to_source_ratio: Ratio of test files to source files
            - tasks_with_companion_tests: Tasks with matching test files
            - tasks_missing_tests: Tasks without companion tests
            - file_level_test_commands: Specific file-level tests
            - package_level_test_commands: Broad package-level tests
            - test_specificity_ratio: Percentage of file-level tests
            - tests_created_not_executed: Test files created but not run
            - completed_tasks_count: Number of completed tasks
            - failed_tasks_count: Number of failed tasks
            - coverage_success_correlation: Correlation between coverage and success
            - avg_coverage_completed: Average coverage for completed tasks
            - avg_coverage_failed: Average coverage for failed tasks

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    tasks_with_test_command = 0
    tasks_with_tests_executed = 0

    total_expected_files = 0
    source_file_count = 0
    test_file_count = 0

    tasks_with_companion_tests = 0
    tasks_missing_tests = 0

    file_level_test_commands = 0
    package_level_test_commands = 0

    tests_created_not_executed = 0

    completed_tasks_count = 0
    failed_tasks_count = 0

    # Track coverage for completed vs failed tasks
    coverage_completed: list[float] = []
    coverage_failed: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        # Analyze expected files
        expected_files = record.get("expected_files", [])
        if not isinstance(expected_files, list):
            expected_files = []

        task_source_files = 0
        task_test_files = 0

        for file_path in expected_files:
            file_str = _string(file_path)
            if not file_str:
                continue

            total_expected_files += 1

            if _is_test_file(file_str):
                test_file_count += 1
                task_test_files += 1
            else:
                source_file_count += 1
                task_source_files += 1

        # Calculate task-level test coverage
        task_coverage = _calculate_coverage(task_test_files, task_source_files)

        # Track test command presence
        test_command = _string(record.get("test_command"))
        if test_command:
            tasks_with_test_command += 1

            # Analyze test command specificity
            if _is_file_level_test(test_command):
                file_level_test_commands += 1
            else:
                package_level_test_commands += 1

        # Track test execution
        if record.get("has_tests_executed") is True:
            tasks_with_tests_executed += 1

        # Check for tests created but not executed
        if task_test_files > 0 and not record.get("has_tests_executed"):
            tests_created_not_executed += task_test_files

        # Check for companion tests
        if task_source_files > 0:
            if task_test_files > 0:
                tasks_with_companion_tests += 1
            else:
                tasks_missing_tests += 1

        # Track outcomes
        outcome = _string(record.get("outcome")).lower()
        if outcome == "completed":
            completed_tasks_count += 1
            coverage_completed.append(task_coverage)
        elif outcome == "failed":
            failed_tasks_count += 1
            coverage_failed.append(task_coverage)

    # Calculate metrics
    test_to_source_ratio = _ratio(test_file_count, source_file_count)
    test_specificity_ratio = _percentage(
        file_level_test_commands,
        tasks_with_test_command
    )

    avg_coverage_completed = _average(coverage_completed)
    avg_coverage_failed = _average(coverage_failed)

    # Calculate correlation (simplified heuristic)
    coverage_success_correlation = _calculate_correlation(
        avg_coverage_completed,
        avg_coverage_failed
    )

    return {
        "total_tasks": total_tasks,
        "tasks_with_test_command": tasks_with_test_command,
        "tasks_with_tests_executed": tasks_with_tests_executed,
        "total_expected_files": total_expected_files,
        "source_file_count": source_file_count,
        "test_file_count": test_file_count,
        "test_to_source_ratio": test_to_source_ratio,
        "tasks_with_companion_tests": tasks_with_companion_tests,
        "tasks_missing_tests": tasks_missing_tests,
        "file_level_test_commands": file_level_test_commands,
        "package_level_test_commands": package_level_test_commands,
        "test_specificity_ratio": test_specificity_ratio,
        "tests_created_not_executed": tests_created_not_executed,
        "completed_tasks_count": completed_tasks_count,
        "failed_tasks_count": failed_tasks_count,
        "coverage_success_correlation": coverage_success_correlation,
        "avg_coverage_completed": avg_coverage_completed,
        "avg_coverage_failed": avg_coverage_failed,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _is_test_file(file_path: str) -> bool:
    """Check if file path is a test file.

    Args:
        file_path: File path to check

    Returns:
        True if file is a test file
    """
    file_lower = file_path.lower()
    return (
        "test_" in file_lower or
        "_test." in file_lower or
        "/tests/" in file_lower or
        "spec." in file_lower or
        ".test." in file_lower
    )


def _is_file_level_test(test_command: str) -> bool:
    """Check if test command is file-level specific.

    Args:
        test_command: Test command string

    Returns:
        True if command targets specific files
    """
    command_lower = test_command.lower()

    # File-level indicators
    file_indicators = [
        "test_",  # pytest tests/test_specific.py
        ".py",    # python test_file.py
        ".js",    # npm test file.test.js
        ".ts",    # jest specific.test.ts
        "::",     # pytest tests/test_file.py::test_function
    ]

    return any(indicator in command_lower for indicator in file_indicators)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _ratio(numerator: int | float, denominator: int | float) -> float:
    """Calculate ratio, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_coverage(test_files: int, source_files: int) -> float:
    """Calculate test coverage ratio.

    Args:
        test_files: Number of test files
        source_files: Number of source files

    Returns:
        Coverage ratio (test_files / source_files)
    """
    if source_files == 0:
        return 0.0
    return test_files / source_files


def _calculate_correlation(
    avg_coverage_completed: float,
    avg_coverage_failed: float
) -> str:
    """Calculate coverage-success correlation.

    Simplified heuristic based on difference between completed and failed
    task coverage.

    Args:
        avg_coverage_completed: Average coverage for completed tasks
        avg_coverage_failed: Average coverage for failed tasks

    Returns:
        Correlation strength: "positive", "negative", "neutral", or "insufficient_data"
    """
    # Need meaningful data to calculate correlation
    if avg_coverage_completed == 0.0 and avg_coverage_failed == 0.0:
        return "insufficient_data"

    difference = avg_coverage_completed - avg_coverage_failed

    # Positive correlation: completed tasks have higher coverage
    if difference > 0.2:
        return "positive"
    # Negative correlation: failed tasks have higher coverage (unusual)
    elif difference < -0.2:
        return "negative"
    # Neutral: similar coverage
    else:
        return "neutral"
