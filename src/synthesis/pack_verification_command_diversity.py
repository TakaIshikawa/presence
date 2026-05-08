"""Pack verification command diversity analyzer for verification strategy quality.

Analyzes verification command coverage across execution packs to measure
verification strategy strength. Evaluates command types, diversity scores,
multi-stage verification adoption, and correlation with pack success rates.

Verification metrics:
- Unique commands: Count of distinct verification commands used
- Command types: Distribution of test/lint/build/typecheck commands
- Multi-stage rate: Percentage of packs using multiple verification types
- Diversity score: Measure of verification breadth across command types
- Success correlation: Relationship between verification diversity and pack outcomes

Verification patterns:
- Comprehensive: Multi-stage verification with diverse command types
- Single-stage: Only one verification command type used
- Weak: Minimal or no verification commands
- Test-only: Only test commands, missing lint/typecheck/build
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


# Command type classifications
COMMAND_TYPE_TEST = "test"
COMMAND_TYPE_LINT = "lint"
COMMAND_TYPE_BUILD = "build"
COMMAND_TYPE_TYPECHECK = "typecheck"
COMMAND_TYPE_OTHER = "other"

# Diversity thresholds
MIN_DIVERSITY_SCORE = 25.0
GOOD_DIVERSITY_SCORE = 50.0
EXCELLENT_DIVERSITY_SCORE = 75.0


def analyze_pack_verification_command_diversity(records: object) -> dict[str, Any]:
    """Analyze verification command diversity across execution packs.

    Evaluates verification strategy strength by analyzing command types,
    diversity scores, and correlation with pack outcomes.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - verification_commands: List of verification command strings
            - success: Boolean indicating pack success/failure
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - packs_with_verification: Count of packs with verification commands
            - verification_rate: Percentage of packs with verification
            - unique_commands: Count of unique verification commands across all packs
            - command_type_distribution: Dict mapping command types to counts
            - multi_stage_packs: Count of packs using multiple command types
            - multi_stage_rate: Percentage of packs with multi-stage verification
            - avg_diversity_score: Average diversity score across packs
            - success_rate_by_diversity: Dict mapping diversity levels to success rates
            - weak_verification_packs: Examples of packs with weak verification
            - examples: Example packs with different verification patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    packs_with_verification = 0
    all_commands: set[str] = set()
    command_type_counts: Counter[str] = Counter()
    multi_stage_packs = 0
    diversity_scores: list[float] = []
    weak_verification_packs: list[dict[str, Any]] = []
    examples: list[dict[str, Any]] = []

    # For success correlation
    diversity_buckets: dict[str, dict[str, int]] = {
        "none": {"success": 0, "failure": 0},
        "low": {"success": 0, "failure": 0},
        "medium": {"success": 0, "failure": 0},
        "high": {"success": 0, "failure": 0},
    }

    for record in records:
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id"))
        verification_commands = _normalize_commands(record.get("verification_commands"))
        success = bool(record.get("success", True))
        task_title = _string(record.get("task_title"))

        total_packs += 1

        # Track verification presence
        if verification_commands:
            packs_with_verification += 1
            all_commands.update(verification_commands)

        # Classify commands by type
        command_types = [_classify_command(cmd) for cmd in verification_commands]
        unique_types = set(command_types)

        for cmd_type in command_types:
            command_type_counts[cmd_type] += 1

        # Check for multi-stage verification
        if len(unique_types) >= 2:
            multi_stage_packs += 1

        # Calculate diversity score for this pack
        diversity_score = _calculate_diversity_score(unique_types)
        diversity_scores.append(diversity_score)

        # Categorize diversity level
        diversity_level = _categorize_diversity(diversity_score)
        if success:
            diversity_buckets[diversity_level]["success"] += 1
        else:
            diversity_buckets[diversity_level]["failure"] += 1

        # Identify weak verification strategies
        if diversity_score < MIN_DIVERSITY_SCORE and len(weak_verification_packs) < 5:
            weak_verification_packs.append({
                "pack_id": pack_id,
                "task_title": task_title or "unknown",
                "verification_commands": verification_commands,
                "command_types": sorted(unique_types),
                "diversity_score": diversity_score,
            })

        # Collect diverse examples
        if len(examples) < 10:
            examples.append({
                "pack_id": pack_id,
                "task_title": task_title or "unknown",
                "verification_commands": verification_commands,
                "command_types": sorted(unique_types),
                "diversity_score": diversity_score,
                "multi_stage": len(unique_types) >= 2,
            })

    # Calculate metrics
    verification_rate = _percentage(packs_with_verification, total_packs)
    multi_stage_rate = _percentage(multi_stage_packs, packs_with_verification)
    avg_diversity_score = _average(sum(diversity_scores), len(diversity_scores))

    # Calculate success rates by diversity level
    success_rate_by_diversity = {}
    for level, counts in diversity_buckets.items():
        total_in_level = counts["success"] + counts["failure"]
        success_rate_by_diversity[level] = _percentage(counts["success"], total_in_level)

    return {
        "total_packs": total_packs,
        "packs_with_verification": packs_with_verification,
        "verification_rate": verification_rate,
        "unique_commands": len(all_commands),
        "command_type_distribution": dict(command_type_counts),
        "multi_stage_packs": multi_stage_packs,
        "multi_stage_rate": multi_stage_rate,
        "avg_diversity_score": avg_diversity_score,
        "success_rate_by_diversity": success_rate_by_diversity,
        "weak_verification_packs": weak_verification_packs,
        "examples": examples[:5],  # Limit to 5 examples
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_commands(value: object) -> list[str]:
    """Normalize verification command list."""
    if isinstance(value, str):
        commands = [value]
    elif isinstance(value, (list, tuple)):
        commands = [c for c in value if isinstance(c, str)]
    else:
        return []

    # Normalize command strings
    normalized = []
    for cmd in commands:
        cmd = cmd.strip()
        if cmd:
            normalized.append(cmd)

    return normalized


def _classify_command(command: str) -> str:
    """Classify verification command by type.

    Args:
        command: Verification command string

    Returns:
        Command type: test, lint, build, typecheck, or other
    """
    cmd_lower = command.lower()

    # Lint commands (check before test to avoid "test" in path matching)
    lint_keywords = ["ruff", "lint", "eslint", "pylint", "flake8", "black", "prettier"]
    if any(keyword in cmd_lower for keyword in lint_keywords):
        return COMMAND_TYPE_LINT

    # Typecheck commands (check before test to avoid "test" in path matching)
    typecheck_keywords = ["mypy", "typecheck", "type-check", "pyright", "tsc"]
    if any(keyword in cmd_lower for keyword in typecheck_keywords):
        return COMMAND_TYPE_TYPECHECK

    # Build commands
    build_keywords = ["build", "compile", "make", "cargo build", "npm run build"]
    if any(keyword in cmd_lower for keyword in build_keywords):
        return COMMAND_TYPE_BUILD

    # Test commands (check last to avoid matching paths like "tests/")
    test_keywords = ["pytest", "jest", "mocha", "vitest", "unittest", "npm test", " test"]
    if any(keyword in cmd_lower for keyword in test_keywords):
        return COMMAND_TYPE_TEST

    return COMMAND_TYPE_OTHER


def _calculate_diversity_score(command_types: set[str]) -> float:
    """Calculate diversity score based on command type variety.

    Diversity score is a percentage representing how many different
    verification types are used out of the four main categories.

    Args:
        command_types: Set of command type strings

    Returns:
        Diversity score from 0.0 to 100.0
    """
    if not command_types:
        return 0.0

    # Count how many of the four main types are present
    main_types = {
        COMMAND_TYPE_TEST,
        COMMAND_TYPE_LINT,
        COMMAND_TYPE_BUILD,
        COMMAND_TYPE_TYPECHECK,
    }

    present_main_types = command_types & main_types
    score = (len(present_main_types) / len(main_types)) * 100.0

    return round(score, 2)


def _categorize_diversity(diversity_score: float) -> str:
    """Categorize diversity score into levels.

    Args:
        diversity_score: Diversity score from 0.0 to 100.0

    Returns:
        Diversity level: none, low, medium, or high
    """
    if diversity_score == 0.0:
        return "none"
    elif diversity_score < GOOD_DIVERSITY_SCORE:
        return "low"
    elif diversity_score < EXCELLENT_DIVERSITY_SCORE:
        return "medium"
    else:
        return "high"


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)
