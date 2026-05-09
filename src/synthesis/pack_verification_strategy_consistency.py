"""Pack verification strategy consistency analyzer.

Validates consistency between verificationCommand, testCommand, and expectedFiles
across tasks in an execution pack. Ensures verification commands cover all
test commands, use correct package manager, match file patterns, and align
with risk levels.

Consistency metrics:
- Verification-test alignment: verificationCommand covers all testCommands
- Package manager consistency: Commands use correct package manager
- File pattern matching: Commands match expectedFiles patterns
- Risk-verification alignment: Risk levels align with verification breadth
- Missing coverage: expectedFiles not validated by any command

Quality indicators:
- Consistent packs: All verification commands aligned
- Inconsistent packs: Mismatched commands or missing coverage
- Risk misalignment: High-risk task with narrow testCommand
- Unified strategy: Pack uses consistent verification approach
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_verification_strategy_consistency(records: object) -> dict[str, Any]:
    """Analyze verification strategy consistency across pack tasks.

    Validates consistency between verificationCommand, testCommand, and
    expectedFiles. Identifies mismatches and coverage gaps.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - verification_command: Pack-level verification command
            - test_command: Task-level test command
            - expected_files: List of expected file paths
            - risk_level: Task risk level (low|medium|high)

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - has_verification_command: Tasks with verification command
            - has_test_command: Tasks with test command
            - verification_covers_test: Tasks where verify covers test
            - verification_test_alignment_ratio: Percentage aligned
            - package_manager_consistent: Boolean if PM is consistent
            - detected_package_manager: Detected package manager
            - file_pattern_matches: Commands matching expectedFiles
            - risk_verification_aligned: High-risk tasks with broad verify
            - risk_misalignment_count: High-risk with narrow verification
            - missing_coverage_files_count: Files not covered by commands
            - missing_coverage_files: List of uncovered files
            - consistency_score: Overall consistency score (0-1)
            - well_aligned_tasks: Tasks with full alignment
            - poorly_aligned_tasks: Tasks with mismatches
            - unified_strategy: Boolean if pack has unified strategy

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    total_tasks = 0
    has_verification_command = 0
    has_test_command = 0
    verification_covers_test = 0

    package_managers: list[str] = []
    file_pattern_matches = 0
    risk_verification_aligned = 0
    risk_misalignment_count = 0

    missing_coverage_files: list[str] = []
    well_aligned_tasks = 0
    poorly_aligned_tasks = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        verification_command = _string(record.get("verification_command", ""))
        test_command = _string(record.get("test_command", ""))
        expected_files = record.get("expected_files")
        risk_level = _string(record.get("risk_level", "")).lower()

        if not isinstance(expected_files, list):
            expected_files = []

        # Track command presence
        if verification_command:
            has_verification_command += 1
        if test_command:
            has_test_command += 1

        # Check verification covers test
        if verification_command and test_command:
            if _verification_covers_test(verification_command, test_command):
                verification_covers_test += 1

        # Detect package manager
        pm = _detect_package_manager(verification_command or test_command)
        if pm:
            package_managers.append(pm)

        # Check file pattern matching
        if _files_match_commands(expected_files, verification_command, test_command):
            file_pattern_matches += 1

        # Check risk-verification alignment
        if risk_level in ("high", "medium"):
            if _verification_is_broad(verification_command):
                risk_verification_aligned += 1
            elif test_command and not _verification_is_broad(test_command):
                risk_misalignment_count += 1

        # Check coverage
        uncovered = _find_uncovered_files(expected_files, verification_command, test_command)
        missing_coverage_files.extend(uncovered)

        # Categorize task alignment
        task_aligned = (
            (verification_command and test_command and
             _verification_covers_test(verification_command, test_command)) or
            (verification_command and not test_command) or
            (test_command and not verification_command)
        )

        if task_aligned and not uncovered:
            well_aligned_tasks += 1
        elif uncovered or (verification_command and test_command and
                          not _verification_covers_test(verification_command, test_command)):
            poorly_aligned_tasks += 1

    # Calculate metrics
    verification_test_alignment_ratio = _percentage(verification_covers_test, has_test_command)

    # Check package manager consistency
    package_manager_consistent = len(set(package_managers)) <= 1
    detected_package_manager = package_managers[0] if package_managers else "unknown"

    # Calculate consistency score
    consistency_score = _calculate_consistency_score(
        verification_test_alignment_ratio,
        package_manager_consistent,
        file_pattern_matches,
        total_tasks,
        risk_misalignment_count,
        len(missing_coverage_files),
    )

    # Check unified strategy
    unified_strategy = consistency_score >= 0.7

    return {
        "total_tasks": total_tasks,
        "has_verification_command": has_verification_command,
        "has_test_command": has_test_command,
        "verification_covers_test": verification_covers_test,
        "verification_test_alignment_ratio": verification_test_alignment_ratio,
        "package_manager_consistent": package_manager_consistent,
        "detected_package_manager": detected_package_manager,
        "file_pattern_matches": file_pattern_matches,
        "risk_verification_aligned": risk_verification_aligned,
        "risk_misalignment_count": risk_misalignment_count,
        "missing_coverage_files_count": len(missing_coverage_files),
        "missing_coverage_files": missing_coverage_files[:10],  # Limit to first 10
        "consistency_score": consistency_score,
        "well_aligned_tasks": well_aligned_tasks,
        "poorly_aligned_tasks": poorly_aligned_tasks,
        "unified_strategy": unified_strategy,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _verification_covers_test(verification_cmd: str, test_cmd: str) -> bool:
    """Check if verification command covers test command.

    Verification covers test if:
    - Same command
    - Verification is broader (e.g., "pytest tests/" covers "pytest tests/test_foo.py")
    - Verification runs all tests and test is subset

    Args:
        verification_cmd: Verification command string
        test_cmd: Test command string

    Returns:
        True if verification covers test
    """
    if not verification_cmd or not test_cmd:
        return False

    # Same command
    if verification_cmd == test_cmd:
        return True

    # Extract test paths
    verify_path = _extract_test_path(verification_cmd)
    test_path = _extract_test_path(test_cmd)

    # Verification is broader (parent directory)
    if verify_path and test_path:
        if test_path.startswith(verify_path):
            return True

    # Both use same test framework
    if _same_test_framework(verification_cmd, test_cmd):
        # Verification is broader if it has fewer specific files
        verify_files = _extract_file_patterns(verification_cmd)
        test_files = _extract_file_patterns(test_cmd)

        if not verify_files and test_files:
            # Verification runs all, test is specific
            return True

    return False


def _detect_package_manager(command: str) -> str:
    """Detect package manager from command.

    Supported package managers:
    - npm, yarn, pnpm
    - uv, pip, poetry
    - cargo, go
    - pytest (direct)

    Args:
        command: Command string

    Returns:
        Package manager name or empty string
    """
    if not command:
        return ""

    command_lower = command.lower()

    pm_patterns = {
        "npm": ["npm test", "npm run test"],
        "yarn": ["yarn test"],
        "pnpm": ["pnpm test"],
        "uv": ["uv run"],
        "pip": ["python -m pytest", "python -m unittest"],
        "pytest": ["pytest"],
        "cargo": ["cargo test"],
        "go": ["go test"],
    }

    for pm, patterns in pm_patterns.items():
        for pattern in patterns:
            if pattern in command_lower:
                return pm

    return "unknown"


def _files_match_commands(expected_files: list[Any], verify_cmd: str, test_cmd: str) -> bool:
    """Check if file patterns in commands match expectedFiles.

    Args:
        expected_files: List of expected file paths
        verify_cmd: Verification command
        test_cmd: Test command

    Returns:
        True if files match commands
    """
    if not expected_files:
        return True

    # Extract test files from expected_files
    test_files = [f for f in expected_files if isinstance(f, str) and "test" in f.lower()]

    if not test_files:
        return True

    # Check if commands reference test files
    combined_cmd = f"{verify_cmd} {test_cmd}".lower()

    for test_file in test_files:
        file_name = test_file.split("/")[-1].replace(".py", "")
        if file_name in combined_cmd:
            return True

    # If commands mention "tests/" and files are in tests/, consider it a match
    if "tests/" in combined_cmd and any("tests/" in f for f in test_files):
        return True

    return False


def _verification_is_broad(command: str) -> bool:
    """Check if verification command is broad (covers multiple files/modules).

    Broad verification:
    - Runs all tests (pytest tests/, npm test)
    - Covers multiple files or directories
    - No specific file paths

    Args:
        command: Command string

    Returns:
        True if broad verification
    """
    if not command:
        return False

    command_lower = command.lower()

    # Broad patterns
    broad_patterns = [
        "pytest tests/",
        "pytest tests",
        "npm test",
        "yarn test",
        "cargo test",
        "go test ./...",
        "uv run --with pytest pytest",
    ]

    for pattern in broad_patterns:
        if pattern in command_lower:
            return True

    # Check if no specific file mentioned (ends with directory or no args)
    if "pytest" in command_lower:
        # If pytest with no specific .py file, it's broad
        if ".py" not in command_lower:
            return True

    return False


def _find_uncovered_files(expected_files: list[Any], verify_cmd: str, test_cmd: str) -> list[str]:
    """Find expectedFiles not covered by verification or test commands.

    Args:
        expected_files: List of expected file paths
        verify_cmd: Verification command
        test_cmd: Test command

    Returns:
        List of uncovered file paths
    """
    if not expected_files:
        return []

    uncovered: list[str] = []
    combined_cmd = f"{verify_cmd} {test_cmd}".lower()

    # If commands are broad (pytest tests/), assume all test files are covered
    if "pytest tests" in combined_cmd or "npm test" in combined_cmd:
        return []

    for file_path in expected_files:
        if not isinstance(file_path, str):
            continue

        # Only check test files
        if "test" not in file_path.lower():
            continue

        file_name = file_path.split("/")[-1]
        if file_name not in combined_cmd and file_path not in combined_cmd:
            uncovered.append(file_path)

    return uncovered


def _extract_test_path(command: str) -> str:
    """Extract test path from command.

    Args:
        command: Command string

    Returns:
        Test path or empty string
    """
    if not command:
        return ""

    parts = command.split()
    for part in parts:
        if "test" in part.lower() and ("/" in part or "." in part):
            return part.strip()

    return ""


def _extract_file_patterns(command: str) -> list[str]:
    """Extract file patterns from command.

    Args:
        command: Command string

    Returns:
        List of file patterns
    """
    if not command:
        return []

    patterns: list[str] = []
    parts = command.split()

    for part in parts:
        if part.endswith(".py") or part.endswith(".js") or part.endswith(".ts"):
            patterns.append(part)

    return patterns


def _same_test_framework(cmd1: str, cmd2: str) -> bool:
    """Check if two commands use the same test framework.

    Args:
        cmd1: First command
        cmd2: Second command

    Returns:
        True if same framework
    """
    frameworks = ["pytest", "unittest", "jest", "mocha", "cargo test", "go test"]

    for framework in frameworks:
        if framework in cmd1.lower() and framework in cmd2.lower():
            return True

    return False


def _calculate_consistency_score(
    alignment_ratio: float,
    pm_consistent: bool,
    file_matches: int,
    total_tasks: int,
    risk_misalignments: int,
    uncovered_count: int,
) -> float:
    """Calculate overall consistency score (0-1).

    Score components:
    - 0.4: Verification-test alignment ratio
    - 0.2: Package manager consistency
    - 0.2: File pattern matching
    - 0.1: Risk alignment (penalize misalignments)
    - 0.1: Coverage (penalize uncovered files)

    Args:
        alignment_ratio: Verification-test alignment percentage
        pm_consistent: Package manager consistency
        file_matches: Number of tasks with matching files
        total_tasks: Total number of tasks
        risk_misalignments: Number of risk misalignments
        uncovered_count: Number of uncovered files

    Returns:
        Consistency score 0-1
    """
    if total_tasks == 0:
        return 0.0

    # Alignment component (0-0.4)
    alignment_component = (alignment_ratio / 100.0) * 0.4

    # Package manager component (0-0.2)
    pm_component = 0.2 if pm_consistent else 0.0

    # File matching component (0-0.2)
    file_match_ratio = file_matches / total_tasks
    file_component = file_match_ratio * 0.2

    # Risk alignment component (0-0.1)
    risk_component = max(0.0, 0.1 - (risk_misalignments * 0.05))

    # Coverage component (0-0.1)
    coverage_component = max(0.0, 0.1 - (uncovered_count * 0.02))

    score = (
        alignment_component +
        pm_component +
        file_component +
        risk_component +
        coverage_component
    )

    return round(max(0.0, min(1.0, score)), 3)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
