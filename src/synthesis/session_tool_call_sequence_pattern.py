"""Session tool call sequence pattern analyzer for workflow optimization.

Analyzes the sequence and patterns of tool calls within agent sessions. Identifies
common workflows, efficient patterns, and problematic sequences like excessive
re-reads or circular tool chains.

Sequence metrics:
- Common patterns: Frequently occurring tool sequences (e.g., Read→Edit→Read)
- Sequence length distribution: How many consecutive tool calls of same type
- Tool transition frequencies: Most common tool-to-tool transitions
- Inefficient patterns: Circular reads, redundant tool chains
- Workflow efficiency: Pattern-based workflow quality assessment

Pattern classifications:
- Efficient: Read→Edit→Verify, Grep→Read→Edit sequences
- Inefficient: Read→Read→Read (excessive re-reads), circular patterns
- Optimal: Targeted sequences with minimal redundancy
"""

from __future__ import annotations

from collections import Counter
from typing import Any


# Common efficient workflow patterns
EFFICIENT_PATTERNS = [
    ("Read", "Edit", "Read"),  # Edit with verification
    ("Grep", "Read", "Edit"),  # Search, read, modify
    ("Read", "Edit", "Bash"),  # Edit then test
    ("Glob", "Read", "Edit"),  # Find, read, modify
]

# Inefficient patterns to detect
INEFFICIENT_PATTERNS = [
    ("Read", "Read", "Read"),  # Excessive re-reads
    ("Grep", "Grep", "Grep"),  # Repeated searches
    ("Edit", "Edit", "Edit"),  # Rapid-fire edits without verification
]


def analyze_session_tool_call_sequence_pattern(records: object) -> dict[str, Any]:
    """Analyze tool call sequence patterns in a session.

    Examines the order and patterns of tool calls to identify common workflows
    and detect inefficient sequences.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, Edit, Bash, etc.)
            - turn_index: Turn number when tool was called
            - file_path: Optional file path for file operations

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls
            - unique_tools: Number of unique tools used
            - sequence_patterns: Most common 3-tool sequences
            - tool_transitions: Most common tool-to-tool transitions
            - consecutive_same_tool: Max consecutive calls to same tool
            - efficient_pattern_count: Count of efficient patterns detected
            - inefficient_pattern_count: Count of inefficient patterns
            - circular_reads: Count of file read multiple times in succession
            - workflow_efficiency: Classification of overall pattern quality

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    if not records:
        return _empty_result()

    # Extract tool sequence
    tools: list[str] = []
    file_paths: list[str] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tools.append(tool_name)
        file_paths.append(_string(record.get("file_path", "")))

    if not tools:
        return _empty_result()

    # Analyze sequences
    sequence_patterns = _find_sequence_patterns(tools, 3)
    tool_transitions = _find_transitions(tools)
    consecutive_same_tool = _max_consecutive_same(tools)
    efficient_count = _count_patterns(tools, EFFICIENT_PATTERNS)
    inefficient_count = _count_patterns(tools, INEFFICIENT_PATTERNS)
    circular_reads = _count_circular_reads(tools, file_paths)
    workflow_efficiency = _classify_workflow_efficiency(
        efficient_count,
        inefficient_count,
        circular_reads,
        consecutive_same_tool,
        len(tools),
    )

    return {
        "total_tool_calls": len(tools),
        "unique_tools": len(set(tools)),
        "sequence_patterns": sequence_patterns,
        "tool_transitions": tool_transitions,
        "consecutive_same_tool": consecutive_same_tool,
        "efficient_pattern_count": efficient_count,
        "inefficient_pattern_count": inefficient_count,
        "circular_reads": circular_reads,
        "workflow_efficiency": workflow_efficiency,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tool_calls": 0,
        "unique_tools": 0,
        "sequence_patterns": [],
        "tool_transitions": [],
        "consecutive_same_tool": 0,
        "efficient_pattern_count": 0,
        "inefficient_pattern_count": 0,
        "circular_reads": 0,
        "workflow_efficiency": "empty",
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _find_sequence_patterns(tools: list[str], length: int) -> list[dict[str, Any]]:
    """Find most common N-length sequences.

    Returns up to 5 most common patterns with their counts.
    """
    if len(tools) < length:
        return []

    sequences: Counter[tuple[str, ...]] = Counter()

    for i in range(len(tools) - length + 1):
        sequence = tuple(tools[i:i + length])
        sequences[sequence] = sequences.get(sequence, 0) + 1

    # Return top 5 patterns
    top_patterns = sequences.most_common(5)
    return [
        {"pattern": list(pattern), "count": count}
        for pattern, count in top_patterns
    ]


def _find_transitions(tools: list[str]) -> list[dict[str, Any]]:
    """Find most common tool-to-tool transitions.

    Returns up to 5 most common transitions.
    """
    if len(tools) < 2:
        return []

    transitions: Counter[tuple[str, str]] = Counter()

    for i in range(len(tools) - 1):
        transition = (tools[i], tools[i + 1])
        transitions[transition] = transitions.get(transition, 0) + 1

    # Return top 5 transitions
    top_transitions = transitions.most_common(5)
    return [
        {"from_tool": from_tool, "to_tool": to_tool, "count": count}
        for (from_tool, to_tool), count in top_transitions
    ]


def _max_consecutive_same(tools: list[str]) -> int:
    """Find maximum number of consecutive calls to same tool."""
    if not tools:
        return 0

    max_consecutive = 1
    current_consecutive = 1

    for i in range(1, len(tools)):
        if tools[i] == tools[i - 1]:
            current_consecutive += 1
            max_consecutive = max(max_consecutive, current_consecutive)
        else:
            current_consecutive = 1

    return max_consecutive


def _count_patterns(tools: list[str], patterns: list[tuple[str, ...]]) -> int:
    """Count occurrences of specific patterns in tool sequence."""
    count = 0

    for pattern in patterns:
        pattern_len = len(pattern)
        for i in range(len(tools) - pattern_len + 1):
            sequence = tuple(tools[i:i + pattern_len])
            if sequence == pattern:
                count += 1

    return count


def _count_circular_reads(tools: list[str], file_paths: list[str]) -> int:
    """Count instances where same file is read multiple times in succession.

    Circular read: Same file read 2+ times within 5 tool calls.
    """
    if len(tools) != len(file_paths):
        return 0

    circular_count = 0
    window_size = 6  # Window of 6 to include i + 5

    for i in range(len(tools)):
        if tools[i] not in ("Read", "Glob", "Grep"):
            continue

        file_path = file_paths[i]
        if not file_path:
            continue

        # Look ahead in window (up to 5 positions ahead)
        for j in range(i + 1, min(i + window_size, len(tools))):
            if tools[j] in ("Read", "Glob", "Grep") and file_paths[j] == file_path:
                circular_count += 1
                break

    return circular_count


def _classify_workflow_efficiency(
    efficient_count: int,
    inefficient_count: int,
    circular_reads: int,
    max_consecutive: int,
    total_calls: int,
) -> str:
    """Classify workflow efficiency based on pattern analysis.

    Classifications:
    - optimal: High efficient patterns, no inefficient patterns
    - efficient: More efficient than inefficient patterns
    - inefficient: More inefficient patterns, circular reads, or excessive consecutive
    - mixed: Mix of patterns
    - simple: Too few calls to classify
    - empty: No calls
    """
    if total_calls == 0:
        return "empty"

    if total_calls < 5:
        return "simple"

    # Check for serious inefficiencies (lowered thresholds)
    has_serious_issues = (
        inefficient_count >= 2
        or circular_reads > 2
        or max_consecutive > 4
    )

    if has_serious_issues:
        return "inefficient"

    # Check for optimal patterns
    if efficient_count > 2 and inefficient_count == 0 and circular_reads <= 1:
        return "optimal"

    # More efficient than inefficient
    if efficient_count > inefficient_count and circular_reads <= 2:
        return "efficient"

    # Default: mixed patterns
    return "mixed"
