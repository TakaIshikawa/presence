"""Pack verification command coverage analyzer for test quality assessment.

Analyzes verification command quality within execution packs to ensure
proper testing and validation. Evaluates command presence, targeting,
and comprehensiveness of verification strategies.

Coverage metrics:
- Test command presence: Tasks with explicit test commands
- Command targeting: Specific vs workspace-wide verification
- Type checking: Inclusion of type validation
- Linting: Inclusion of lint/style checks
- Verification-to-file ratio: Balance of verification scope

Quality indicators:
- Targeted commands: Verification scoped to changed files
- Comprehensive checks: Tests + types + lint coverage
- Missing verification: Tasks without validation commands
- Over-broad verification: Workspace-wide checks for small changes
"""

from __future__ import annotations

import re
from typing import Any, Mapping


def analyze_pack_verification_command_coverage(records: object) -> dict[str, Any]:
    """Analyze verification command coverage and quality within packs.

    Evaluates verification commands for completeness, targeting, and
    best-practice adherence.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - verification_command: Verification command string
            - expected_files: List of files task expects to modify
            - changed_files: Optional list of actually changed files

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - has_test_command: Tasks with test commands
            - has_type_check: Tasks with type checking
            - has_lint: Tasks with linting
            - targeted_command_count: Commands targeting specific files
            - workspace_wide_count: Commands running on entire workspace
            - empty_command_count: Tasks without verification commands
            - avg_verification_to_file_ratio: Mean ratio of verification scope
            - comprehensive_coverage_count: Tasks with tests + types + lint
            - missing_verification_count: Tasks without any verification
            - over_broad_verification_count: Workspace checks for 1-2 files

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    has_test_command = 0
    has_type_check = 0
    has_lint = 0
    targeted_command_count = 0
    workspace_wide_count = 0
    empty_command_count = 0

    verification_ratios: list[float] = []
    comprehensive_coverage_count = 0
    missing_verification_count = 0
    over_broad_verification_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        verification_command = _string(record.get("verification_command", ""))
        expected_files = record.get("expected_files")
        file_count = _get_file_count(expected_files, record.get("changed_files"))

        # Check for empty/missing verification
        if not verification_command:
            empty_command_count += 1
            missing_verification_count += 1
            continue

        # Check command types
        has_test = _has_test_command(verification_command)
        has_types = _has_type_check(verification_command)
        has_lint_check = _has_lint(verification_command)

        if has_test:
            has_test_command += 1
        if has_types:
            has_type_check += 1
        if has_lint_check:
            has_lint += 1

        # Check for comprehensive coverage (all three types)
        if has_test and has_types and has_lint_check:
            comprehensive_coverage_count += 1

        # Check command targeting
        is_targeted = _is_targeted_command(verification_command, expected_files)
        is_workspace_wide = _is_workspace_wide(verification_command)

        if is_targeted:
            targeted_command_count += 1
        if is_workspace_wide:
            workspace_wide_count += 1

        # Calculate verification-to-file ratio
        ratio = _calculate_verification_ratio(verification_command, file_count)
        verification_ratios.append(ratio)

        # Detect over-broad verification (workspace-wide for few files)
        if is_workspace_wide and file_count > 0 and file_count <= 2:
            over_broad_verification_count += 1

    # Calculate averages
    avg_verification_ratio = _average(verification_ratios)

    return {
        "total_tasks": total_tasks,
        "has_test_command": has_test_command,
        "has_type_check": has_type_check,
        "has_lint": has_lint,
        "targeted_command_count": targeted_command_count,
        "workspace_wide_count": workspace_wide_count,
        "empty_command_count": empty_command_count,
        "avg_verification_to_file_ratio": avg_verification_ratio,
        "comprehensive_coverage_count": comprehensive_coverage_count,
        "missing_verification_count": missing_verification_count,
        "over_broad_verification_count": over_broad_verification_count,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _get_file_count(expected_files: object, changed_files: object) -> int:
    """Get file count from expected or changed files."""
    if isinstance(changed_files, list):
        return len(changed_files)
    if isinstance(expected_files, list):
        return len(expected_files)
    return 0


def _has_test_command(command: str) -> bool:
    """Check if command includes test execution.

    Test indicators: pytest, jest, npm test, go test, cargo test, etc.
    """
    test_patterns = [
        r'\bpytest\b', r'\btest\b', r'\bjest\b', r'\bmocha\b',
        r'\bgo\s+test\b', r'\bcargo\s+test\b', r'\bmvn\s+test\b',
        r'\bphpunit\b', r'\brspec\b', r'\bminitest\b'
    ]

    for pattern in test_patterns:
        if re.search(pattern, command, re.IGNORECASE):
            return True
    return False


def _has_type_check(command: str) -> bool:
    """Check if command includes type checking.

    Type check indicators: mypy, tsc, pyright, flow, etc.
    """
    type_patterns = [
        r'\bmypy\b', r'\bpyright\b', r'\btsc\b', r'\bflow\b',
        r'\btype[- ]check\b', r'\btypes\b.*\bcheck\b'
    ]

    for pattern in type_patterns:
        if re.search(pattern, command, re.IGNORECASE):
            return True
    return False


def _has_lint(command: str) -> bool:
    """Check if command includes linting.

    Lint indicators: pylint, eslint, flake8, ruff, clippy, etc.
    """
    lint_patterns = [
        r'\bpylint\b', r'\beslint\b', r'\bflake8\b', r'\bruff\b',
        r'\bclippy\b', r'\bblack\b', r'\blint\b', r'\brubocop\b',
        r'\bgolangci-lint\b', r'\bstandardjs\b'
    ]

    for pattern in lint_patterns:
        if re.search(pattern, command, re.IGNORECASE):
            return True
    return False


def _is_targeted_command(command: str, expected_files: object) -> bool:
    """Check if command targets specific files.

    Indicators:
    - File paths in command
    - References to specific test files
    - Explicit file arguments
    """
    if not command:
        return False

    # Check for file paths in command
    file_path_pattern = r'\b[\w/]+\.[\w]{2,4}\b'
    if re.search(file_path_pattern, command):
        return True

    # Check if expected_files are referenced in command
    if isinstance(expected_files, list):
        for file in expected_files:
            if isinstance(file, str) and file in command:
                return True

    return False


def _is_workspace_wide(command: str) -> bool:
    """Check if command runs on entire workspace.

    Workspace-wide indicators:
    - No file arguments
    - Explicit workspace flags (--all, .)
    - Broad directory targets (tests/, src/)
    """
    if not command:
        return False

    # Check for workspace-wide flags
    workspace_patterns = [
        r'\b--all\b', r'\s+\.\s*$', r'\s+\.$',
        r'\btests/\s*$', r'\bsrc/\s*$'
    ]

    for pattern in workspace_patterns:
        if re.search(pattern, command):
            return True

    # If no specific file paths, likely workspace-wide
    file_path_pattern = r'\b[\w/]+\.[\w]{2,4}\b'
    if not re.search(file_path_pattern, command):
        # Has test/lint/type command but no specific files
        if _has_test_command(command) or _has_type_check(command) or _has_lint(command):
            return True

    return False


def _calculate_verification_ratio(command: str, file_count: int) -> float:
    """Calculate ratio of verification scope to file count.

    Higher ratio = more thorough verification per file
    Lower ratio = lighter verification

    Returns:
        Ratio score (0.0-100.0)
    """
    if file_count == 0:
        return 0.0

    # Count verification types in command
    verification_count = sum([
        _has_test_command(command),
        _has_type_check(command),
        _has_lint(command),
    ])

    if verification_count == 0:
        return 0.0

    # Calculate ratio: more verifications per file = higher score
    # Normalize to 0-100 range
    ratio = (verification_count / file_count) * 33.33

    return min(100.0, ratio)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
