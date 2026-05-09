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
"""

from __future__ import annotations

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


def analyze_pack_verification_command_diversity(records: object) -> dict[str, Any]:
    """Analyze verification command diversity across execution packs.

    Evaluates verification strategies by examining command variety, types,
    multi-stage usage, and correlation with pack success rates.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - verification_command: Verification command string
            - expected_files: Optional list of files expected to be modified
            - success: Optional boolean indicating pack success
            - tasks: Optional list of task dictionaries

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
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

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

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

    for record in records:
        if not isinstance(record, Mapping):
            continue

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

    return {
        "total_packs": total_packs,
        "packs_with_verification": packs_with_verification,
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
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
