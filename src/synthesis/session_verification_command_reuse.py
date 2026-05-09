"""Session verification command reuse analyzer.

Analyzes how sessions reuse verification commands versus running one-off checks.
Tracks unique verification commands used, most frequent command patterns, and
ratio of targeted (single-file) to broad (workspace/package) commands. Measures
reuse efficiency based on command pattern consolidation.

Verification command reuse metrics:
- Unique commands: Distinct verification commands used
- Command frequency: Most commonly used verification patterns
- Targeted vs broad: Ratio of single-file to workspace commands
- Reuse efficiency: Command pattern consolidation score
- Command diversity: Variety of verification approaches

Quality indicators:
- High reuse efficiency (>70%): Commands reused frequently
- Balanced targeted/broad ratio (40-60%): Mix of focused and comprehensive checks
- Low unique command count (<10): Consistent verification patterns
- High frequency patterns: Same commands used repeatedly
- Strategic command selection: Right tool for the job
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_verification_command_reuse(records: object) -> dict[str, Any]:
    """Analyze verification command reuse patterns in sessions.

    Evaluates how sessions reuse verification commands and balance targeted
    versus broad verification approaches.

    Args:
        records: List of verification command dictionaries with keys:
            - command_index: Command execution number
            - command: The verification command executed
            - is_targeted: Boolean indicating single-file scope
            - is_broad: Boolean indicating workspace/package scope
            - command_pattern: Normalized command pattern

    Returns:
        Dict with:
            - total_commands: Total verification commands executed
            - unique_commands: Count of distinct commands used
            - most_frequent_commands: List of top command patterns with counts
            - targeted_commands: Count of single-file scope commands
            - broad_commands: Count of workspace/package scope commands
            - targeted_to_broad_ratio: Percentage of targeted commands
            - reuse_efficiency_score: Command consolidation efficiency (%)
            - avg_command_reuse: Average times each command is reused
            - single_use_commands: Count of commands used only once
            - highly_reused_commands: Count of commands used 5+ times

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of verification command dictionaries")

    if not records:
        return _empty_result()

    total_commands = 0
    targeted_commands = 0
    broad_commands = 0
    command_patterns: Counter[str] = Counter()
    command_occurrences: list[str] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_commands += 1

        command = record.get("command")
        command_pattern = record.get("command_pattern")
        is_targeted = record.get("is_targeted")
        is_broad = record.get("is_broad")

        # Track command patterns
        if command_pattern and isinstance(command_pattern, str):
            pattern = command_pattern.strip()
            if pattern:
                command_patterns[pattern] += 1
                command_occurrences.append(pattern)
        elif command and isinstance(command, str):
            # Fallback to raw command if no pattern
            cmd = command.strip()
            if cmd:
                command_patterns[cmd] += 1
                command_occurrences.append(cmd)

        # Track targeted vs broad
        if is_targeted is True:
            targeted_commands += 1
        if is_broad is True:
            broad_commands += 1

    # Calculate unique commands
    unique_commands = len(command_patterns)

    # Get most frequent commands (top 10)
    most_frequent = [
        {"command": pattern, "count": count}
        for pattern, count in command_patterns.most_common(10)
    ]

    # Calculate targeted to broad ratio
    targeted_ratio = _percentage(targeted_commands, total_commands)

    # Calculate reuse efficiency score
    # High efficiency = fewer unique commands relative to total
    # Perfect reuse: 1 unique command used N times = 100%
    # No reuse: N unique commands used once each = 0%
    if unique_commands > 0:
        reuse_efficiency = ((total_commands - unique_commands) / total_commands) * 100.0
        reuse_efficiency = round(reuse_efficiency, 2)
    else:
        reuse_efficiency = 0.0

    # Calculate average command reuse
    avg_reuse = 0.0
    if unique_commands > 0:
        avg_reuse = round(total_commands / unique_commands, 2)

    # Count single-use and highly-reused commands
    single_use = sum(1 for count in command_patterns.values() if count == 1)
    highly_reused = sum(1 for count in command_patterns.values() if count >= 5)

    return {
        "total_commands": total_commands,
        "unique_commands": unique_commands,
        "most_frequent_commands": most_frequent,
        "targeted_commands": targeted_commands,
        "broad_commands": broad_commands,
        "targeted_to_broad_ratio": targeted_ratio,
        "reuse_efficiency_score": reuse_efficiency,
        "avg_command_reuse": avg_reuse,
        "single_use_commands": single_use,
        "highly_reused_commands": highly_reused,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_commands": 0,
        "unique_commands": 0,
        "most_frequent_commands": [],
        "targeted_commands": 0,
        "broad_commands": 0,
        "targeted_to_broad_ratio": 0.0,
        "reuse_efficiency_score": 0.0,
        "avg_command_reuse": 0.0,
        "single_use_commands": 0,
        "highly_reused_commands": 0,
    }


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
