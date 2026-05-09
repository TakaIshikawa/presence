"""Pack verification command pattern analyzer for test execution strategies.

Analyzes verification command patterns across pack executions to identify testing
strategies, command complexity, and consistency in verification approaches.

Command pattern metrics:
- Command type distribution: Breakdown of pytest/npm/cargo/etc usage
- Test scope patterns: Single file vs package vs full suite testing
- Common flags: Verbose, coverage, watch, and other common options
- Command complexity: Score based on flags, chaining, and options
- Strategy consistency: Similarity of commands across pack tasks

Verification patterns:
- Targeted testing: Commands targeting specific files or modules
- Suite testing: Commands running entire test suites
- Multi-tool: Commands combining multiple tools (e.g., test + lint)
- Complex flags: Commands with advanced options and configurations
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Mapping

# Command type patterns for classification
COMMAND_TYPE_PATTERNS = {
    "pytest": r"\bpytest\b",
    "npm": r"\bnpm\s+(test|run|ci)\b",
    "cargo": r"\bcargo\s+test\b",
    "jest": r"\bjest\b",
    "mocha": r"\bmocha\b",
    "vitest": r"\bvitest\b",
    "go_test": r"\bgo\s+test\b",
    "unittest": r"\bpython\s+-m\s+unittest\b",
    "phpunit": r"\bphpunit\b",
    "rspec": r"\brspec\b",
}

# Common flags and options to track
COMMON_FLAGS = {
    "verbose": [r"-v\b", r"--verbose\b", r"-vv\b", r"-vvv\b"],
    "coverage": [r"--cov\b", r"--coverage\b", r"--collect-coverage\b"],
    "parallel": [r"-n\s+\d+", r"--parallel\b", r"-j\s*\d+"],
    "watch": [r"--watch\b", r"-w\b"],
    "failfast": [r"-x\b", r"--exitfirst\b", r"--fail-fast\b"],
    "quiet": [r"-q\b", r"--quiet\b", r"--silent\b"],
    "debug": [r"--debug\b", r"--pdb\b", r"--inspect\b"],
    "markers": [r"-m\s+\w+", r"--markers?\b"],
}

# Test scope patterns
SCOPE_PATTERNS = {
    "single_file": r"(?:tests?/[^\s]+\.py|test_\w+\.(?:js|ts|py))\b",
    "package": r"(?:tests?/\w+/|tests?/\w+\b(?!\.))",
    "full_suite": r"(?:^|\s)(?:tests?/?|pytest|npm test|cargo test)(?:\s|$)",
}


def analyze_pack_verification_command_pattern(records: object) -> dict[str, Any]:
    """Analyze verification command patterns across pack executions.

    Evaluates test execution strategies by examining command types, scope,
    flags, complexity, and consistency across pack tasks.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Pack identifier
            - verification_command: Pack-level verification command
            - task_verification_command: Optional task-level command
            - tasks: Optional list of task dictionaries with verification_command

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - command_type_distribution: Breakdown by tool (pytest/npm/etc)
            - test_scope_patterns: Distribution of file/package/suite testing
            - common_flags: Frequency of flags across commands
            - avg_command_complexity_score: Average complexity (0.0-10.0)
            - high_complexity_packs: Count with score >7.0
            - low_complexity_packs: Count with score <3.0
            - verification_strategy_consistency: Similarity score (0.0-1.0)
            - strategy_patterns: Common command pattern combinations
            - packs_without_verification: Count of packs with no commands

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    command_type_counts: Counter[str] = Counter()
    scope_pattern_counts: Counter[str] = Counter()
    flag_counts: Counter[str] = Counter()

    complexity_scores: list[float] = []
    high_complexity_count = 0
    low_complexity_count = 0

    # Track all commands for consistency analysis
    all_pack_commands: list[list[str]] = []
    packs_without_verification = 0

    # Strategy pattern tracking
    strategy_patterns: Counter[tuple[str, ...]] = Counter()

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        # Extract verification commands
        pack_commands = _extract_all_commands(record)

        if not pack_commands:
            packs_without_verification += 1
            continue

        all_pack_commands.append(pack_commands)

        # Analyze each command
        pack_types: set[str] = set()
        pack_scopes: set[str] = set()
        pack_flags: set[str] = set()
        pack_complexity_factors = 0

        for command in pack_commands:
            # Classify command type
            cmd_type = _classify_command_type(command)
            command_type_counts[cmd_type] += 1
            pack_types.add(cmd_type)

            # Identify scope pattern
            scope = _identify_scope_pattern(command)
            if scope:
                scope_pattern_counts[scope] += 1
                pack_scopes.add(scope)

            # Extract flags
            flags = _extract_flags(command)
            for flag in flags:
                flag_counts[flag] += 1
                pack_flags.add(flag)

            # Calculate complexity factors
            pack_complexity_factors += _count_complexity_factors(command)

        # Calculate pack complexity score
        complexity_score = _calculate_complexity_score(
            len(pack_commands),
            len(pack_types),
            len(pack_scopes),
            len(pack_flags),
            pack_complexity_factors
        )
        complexity_scores.append(complexity_score)

        if complexity_score > 7.0:
            high_complexity_count += 1
        elif complexity_score < 3.0:
            low_complexity_count += 1

        # Track strategy pattern
        if pack_types and pack_scopes:
            pattern = tuple(sorted(list(pack_types) + list(pack_scopes)))
            strategy_patterns[pattern] += 1

    # Calculate averages and distributions
    avg_complexity = _average(complexity_scores)
    consistency_score = _calculate_consistency_score(all_pack_commands)

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

    # Format scope patterns
    total_scopes = sum(scope_pattern_counts.values())
    scope_distribution = [
        {
            "scope": scope,
            "count": count,
            "percentage": _percentage(count, total_scopes),
        }
        for scope, count in scope_pattern_counts.most_common()
    ]

    # Format common flags
    flag_distribution = [
        {"flag": flag, "count": count}
        for flag, count in flag_counts.most_common(10)
    ]

    # Format strategy patterns
    top_patterns = [
        {"pattern": list(pattern), "count": count}
        for pattern, count in strategy_patterns.most_common(10)
    ]

    return {
        "total_packs": total_packs,
        "command_type_distribution": type_distribution,
        "test_scope_patterns": scope_distribution,
        "common_flags": flag_distribution,
        "avg_command_complexity_score": avg_complexity,
        "high_complexity_packs": high_complexity_count,
        "low_complexity_packs": low_complexity_count,
        "verification_strategy_consistency": consistency_score,
        "strategy_patterns": top_patterns,
        "packs_without_verification": packs_without_verification,
    }


def _extract_all_commands(record: Mapping[str, Any]) -> list[str]:
    """Extract all verification commands from a pack record.

    Includes pack-level and task-level verification commands.
    """
    commands: list[str] = []

    # Check pack-level verification command
    for key in ("verification_command", "pack_verification_command"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            commands.extend(_split_commands(value.strip()))

    # Check task-level commands
    tasks = record.get("tasks", [])
    if isinstance(tasks, list):
        for task in tasks:
            if isinstance(task, Mapping):
                for key in ("verification_command", "test_command"):
                    value = task.get(key)
                    if isinstance(value, str) and value.strip():
                        commands.extend(_split_commands(value.strip()))

    # Also check task_verification_command at record level
    task_cmd = record.get("task_verification_command")
    if isinstance(task_cmd, str) and task_cmd.strip():
        commands.extend(_split_commands(task_cmd.strip()))

    # Deduplicate while preserving order
    seen: set[str] = set()
    unique_commands: list[str] = []
    for cmd in commands:
        normalized = " ".join(cmd.split())
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_commands.append(normalized)

    return unique_commands


def _split_commands(command_str: str) -> list[str]:
    """Split command string into individual commands.

    Splits on && or ; separators.
    """
    parts = re.split(r'\s*(?:&&|;)\s*', command_str)
    return [p.strip() for p in parts if p.strip()]


def _classify_command_type(command: str) -> str:
    """Classify command by tool type.

    Returns the matching tool type or "other" if no match.
    """
    cmd_lower = command.lower()

    for cmd_type, pattern in COMMAND_TYPE_PATTERNS.items():
        if re.search(pattern, cmd_lower):
            return cmd_type

    return "other"


def _identify_scope_pattern(command: str) -> str | None:
    """Identify test scope pattern in command.

    Returns the scope type (single_file/package/full_suite) or None.
    """
    # Check patterns in priority order (most specific first)
    if re.search(SCOPE_PATTERNS["single_file"], command):
        return "single_file"

    if re.search(SCOPE_PATTERNS["package"], command):
        return "package"

    if re.search(SCOPE_PATTERNS["full_suite"], command):
        return "full_suite"

    return None


def _extract_flags(command: str) -> list[str]:
    """Extract common flags from command string.

    Returns list of flag types found (e.g., ["verbose", "coverage"]).
    """
    flags_found: list[str] = []

    for flag_name, patterns in COMMON_FLAGS.items():
        for pattern in patterns:
            if re.search(pattern, command):
                flags_found.append(flag_name)
                break  # Only count each flag type once per command

    return flags_found


def _count_complexity_factors(command: str) -> int:
    """Count complexity factors in a command.

    Factors include: pipes, redirects, subshells, multiple flags, etc.
    """
    complexity = 0

    # Pipes increase complexity
    complexity += command.count("|")

    # Redirects
    complexity += command.count(">")
    complexity += command.count("<")

    # Subshells
    complexity += command.count("$(")
    complexity += command.count("`")

    # Multiple flags (rough heuristic: count dash-prefixed tokens)
    flags = re.findall(r'\s-+\w+', command)
    complexity += len(flags)

    # Environment variables
    complexity += len(re.findall(r'\w+=\w+', command))

    return complexity


def _calculate_complexity_score(
    num_commands: int,
    num_types: int,
    num_scopes: int,
    num_flags: int,
    complexity_factors: int
) -> float:
    """Calculate command complexity score from 0.0 to 10.0.

    Higher scores indicate more complex verification strategies.

    Scoring factors:
    - Number of commands: 0.6 per command
    - Command type diversity: 1.0 per type
    - Scope diversity: 0.5 per scope type
    - Flag usage: 0.4 per flag
    - Additional complexity factors: 0.2 per factor
    """
    score = 0.0

    # Base complexity from number of commands
    score += min(num_commands * 0.6, 3.0)

    # Type diversity
    score += min(num_types * 1.0, 3.0)

    # Scope diversity
    score += min(num_scopes * 0.5, 1.5)

    # Flag usage
    score += min(num_flags * 0.4, 2.0)

    # Additional complexity factors
    score += min(complexity_factors * 0.2, 1.0)

    # Clamp to [0.0, 10.0]
    return round(min(max(score, 0.0), 10.0), 2)


def _calculate_consistency_score(all_pack_commands: list[list[str]]) -> float:
    """Calculate strategy consistency across packs.

    Returns consistency score from 0.0 (no consistency) to 1.0 (identical).

    Measures how similar verification commands are across packs.
    """
    if not all_pack_commands or len(all_pack_commands) < 2:
        return 1.0  # Single pack or no packs: perfectly consistent

    # Extract command patterns (normalized)
    patterns: list[tuple[str, ...]] = []
    for pack_commands in all_pack_commands:
        # Normalize commands to command types for comparison
        types = tuple(sorted(set(_classify_command_type(cmd) for cmd in pack_commands)))
        patterns.append(types)

    # Calculate similarity: count identical patterns
    pattern_counts: Counter[tuple[str, ...]] = Counter(patterns)
    most_common_count = pattern_counts.most_common(1)[0][1] if pattern_counts else 0

    # Consistency is ratio of most common pattern
    consistency = most_common_count / len(patterns)

    return round(consistency, 3)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
