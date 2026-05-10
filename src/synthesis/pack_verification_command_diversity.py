<<<<<<< HEAD
"""Pack verification command diversity analyzer for quality assurance.

Analyzes verification command coverage and diversity across execution packs
to identify weak verification strategies and correlate diversity with success.

Command diversity metrics:
- Unique verification commands: Count of distinct commands used
- Command types: Classification (test/lint/build/typecheck/other)
- Multi-stage verification: Percentage using multiple command types
- Verification breadth: File coverage across commands
- Diversity-success correlation: Relationship between diversity and outcomes

Verification strategies:
- Comprehensive: Multiple command types with broad coverage
- Single-stage: One type of verification (e.g., only tests)
- Weak: Minimal or no verification commands
- Redundant: Multiple similar commands without diversity
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
"""

from __future__ import annotations

<<<<<<< HEAD
import re
from collections import Counter, defaultdict
from typing import Any, Mapping

# Command type patterns for classification
COMMAND_PATTERNS = {
    "test": [
        r"\bpytest\b", r"\btest\b", r"\bjest\b", r"\bmocha\b",
        r"\bvitest\b", r"\bava\b", r"\btap\b"
    ],
    "lint": [
        r"\bruff\b", r"\bpylint\b", r"\beslint\b", r"\bflake8\b",
        r"\blint\b", r"\bblack\b", r"\bprettier\b"
    ],
    "typecheck": [
        r"\bmypy\b", r"\bpyright\b", r"\btsc\b", r"\bflow\b",
        r"\btype-?check\b"
    ],
    "build": [
        r"\bbuild\b", r"\bcompile\b", r"\bmake\b", r"\bcargo\b",
        r"\bnpm\s+run\s+build\b", r"\byarn\s+build\b"
    ],
}
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY


def analyze_pack_verification_command_diversity(records: object) -> dict[str, Any]:
    """Analyze verification command diversity across execution packs.

<<<<<<< HEAD
    Evaluates verification strategies by examining command variety, types,
    multi-stage usage, and correlation with pack success rates.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - verification_command: Verification command string
            - expected_files: Optional list of files expected to be modified
            - success: Optional boolean indicating pack success
            - tasks: Optional list of task dictionaries
=======
    Evaluates verification strategy strength by analyzing command types,
    diversity scores, and correlation with pack outcomes.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - verification_commands: List of verification command strings
            - success: Boolean indicating pack success/failure
            - task_title: Optional task title
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
<<<<<<< HEAD
            - packs_with_verification: Packs with non-empty verification
            - unique_commands: Count of distinct verification commands
            - avg_commands_per_pack: Average commands per pack
            - command_type_distribution: Breakdown by type (test/lint/etc)
            - multi_stage_percentage: Percentage using multiple types
            - single_stage_count: Packs with only one command type
            - no_verification_count: Packs without verification
            - avg_file_coverage: Average file coverage per command
            - success_by_diversity: Success rates by diversity level
            - weak_verification_packs: Packs with insufficient verification
            - common_command_patterns: Most frequent command combinations
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

<<<<<<< HEAD
    if not records:
        return _empty_result()

    total_packs = 0
    packs_with_verification = 0
    all_commands: list[str] = []
    commands_per_pack: list[int] = []

    command_type_counts: Counter[str] = Counter()
    multi_stage_count = 0
    single_stage_count = 0
    no_verification_count = 0

    file_coverage_ratios: list[float] = []

    # Track success by diversity level
    diversity_success: defaultdict[int, list[bool]] = defaultdict(list)

    weak_packs: list[dict[str, Any]] = []
    command_combinations: Counter[tuple[str, ...]] = Counter()
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    for record in records:
        if not isinstance(record, Mapping):
            continue

<<<<<<< HEAD
        total_packs += 1
        pack_id = _string(record.get("pack_id", "unknown"))
        verification_cmd = _string(record.get("verification_command", ""))
        expected_files = record.get("expected_files", [])
        success = record.get("success")

        if not verification_cmd:
            no_verification_count += 1
            diversity_success[0].append(success if isinstance(success, bool) else False)
            weak_packs.append({
                "pack_id": pack_id,
                "reason": "No verification command",
                "verification_command": "",
            })
            continue

        packs_with_verification += 1

        # Extract individual commands (split by && or ;)
        commands = _split_commands(verification_cmd)
        all_commands.extend(commands)
        commands_per_pack.append(len(commands))

        # Classify command types
        types_used = set()
        for cmd in commands:
            cmd_type = _classify_command(cmd)
            command_type_counts[cmd_type] += 1
            types_used.add(cmd_type)

        # Check multi-stage vs single-stage
        if len(types_used) > 1:
            multi_stage_count += 1
        elif len(types_used) == 1:
            single_stage_count += 1

        # Track diversity level for correlation
        diversity_level = len(types_used)
        if isinstance(success, bool):
            diversity_success[diversity_level].append(success)

        # Calculate file coverage
        if isinstance(expected_files, list) and expected_files:
            # Check how many expected files are covered by verification
            covered_count = sum(
                1 for file in expected_files
                if any(_file_in_command(file, cmd) for cmd in commands)
            )
            coverage_pct = _percentage(covered_count, len(expected_files))
            file_coverage_ratios.append(coverage_pct)

        # Track command type combinations
        if types_used:
            combination = tuple(sorted(types_used))
            command_combinations[combination] += 1

        # Identify weak verification (only "other" type or very generic)
        if types_used == {"other"} or (len(commands) == 1 and len(commands[0]) < 10):
            weak_packs.append({
                "pack_id": pack_id,
                "reason": "Weak verification strategy",
                "verification_command": verification_cmd,
            })

    # Calculate metrics
    unique_commands_count = len(set(all_commands))
    avg_commands_per_pack = _average(commands_per_pack)
    multi_stage_pct = _percentage(multi_stage_count, packs_with_verification)
    avg_file_coverage = _average(file_coverage_ratios)

    # Calculate success rates by diversity
    success_by_diversity = []
    for diversity in sorted(diversity_success.keys()):
        successes = diversity_success[diversity]
        if successes:
            success_rate = _percentage(sum(successes), len(successes))
            success_by_diversity.append({
                "diversity_level": diversity,
                "total_packs": len(successes),
                "success_rate": success_rate,
            })

    # Format command type distribution
    total_commands = sum(command_type_counts.values())
    type_distribution = [
        {
            "type": cmd_type,
            "count": count,
            "percentage": _percentage(count, total_commands),
        }
        for cmd_type, count in command_type_counts.most_common()
    ]

    # Format common patterns
    common_patterns = [
        {"types": list(combo), "count": count}
        for combo, count in command_combinations.most_common(10)
    ]
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    return {
        "total_packs": total_packs,
        "packs_with_verification": packs_with_verification,
<<<<<<< HEAD
        "unique_commands": unique_commands_count,
        "avg_commands_per_pack": avg_commands_per_pack,
        "command_type_distribution": type_distribution,
        "multi_stage_percentage": multi_stage_pct,
        "single_stage_count": single_stage_count,
        "no_verification_count": no_verification_count,
        "avg_file_coverage": avg_file_coverage,
        "success_by_diversity": success_by_diversity,
        "weak_verification_packs": weak_packs[:20],  # Limit to 20 examples
        "common_command_patterns": common_patterns,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_packs": 0,
        "packs_with_verification": 0,
        "unique_commands": 0,
        "avg_commands_per_pack": 0.0,
        "command_type_distribution": [],
        "multi_stage_percentage": 0.0,
        "single_stage_count": 0,
        "no_verification_count": 0,
        "avg_file_coverage": 0.0,
        "success_by_diversity": [],
        "weak_verification_packs": [],
        "common_command_patterns": [],
=======
        "verification_rate": verification_rate,
        "unique_commands": len(all_commands),
        "command_type_distribution": dict(command_type_counts),
        "multi_stage_packs": multi_stage_packs,
        "multi_stage_rate": multi_stage_rate,
        "avg_diversity_score": avg_diversity_score,
        "success_rate_by_diversity": success_rate_by_diversity,
        "weak_verification_packs": weak_verification_packs,
        "examples": examples[:5],  # Limit to 5 examples
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
def _split_commands(verification_cmd: str) -> list[str]:
    """Split verification command into individual commands.

    Splits on && or ; while preserving quoted sections.
    """
    if not verification_cmd:
        return []

    # Simple split on && and ; (more sophisticated parsing would handle quotes)
    parts = re.split(r'\s*(?:&&|;)\s*', verification_cmd)
    return [p.strip() for p in parts if p.strip()]


def _classify_command(command: str) -> str:
    """Classify command by type (test/lint/typecheck/build/other).

    Returns the first matching type or "other" if no match.
    """
    cmd_lower = command.lower()

    for cmd_type, patterns in COMMAND_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, cmd_lower):
                return cmd_type

    return "other"


def _file_in_command(filename: str, command: str) -> bool:
    """Check if filename appears in command string."""
    # Simple substring check (could be enhanced)
    return filename in command
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


<<<<<<< HEAD
def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
=======
def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
