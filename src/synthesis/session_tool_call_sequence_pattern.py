"""Session tool call sequence pattern analyzer for workflow detection.

Analyzes the sequence and patterns of tool calls within agent sessions to
identify common workflows, detect inefficient patterns, and measure sequence
characteristics. Tracks consecutive tool usage patterns like Read→Edit→Read
sequences and identifies circular tool chains.

Sequence metrics:
- Sequence length distribution: Min/max/avg consecutive tool calls
- Tool transition frequencies: Which tools commonly follow others
- Common patterns: Frequently occurring tool sequences
- Inefficient patterns: Circular reads, redundant tool chains

Pattern types:
- Read-Edit-Verify: Standard modification workflow
- Read-Edit-Read: Verification by re-reading
- Grep-Read-Edit: Search and modify workflow
- Circular reads: Same file read multiple times without edits
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any


# Pattern detection thresholds
MIN_PATTERN_LENGTH = 2
MAX_PATTERN_LENGTH = 5
MIN_PATTERN_FREQUENCY = 2

# Common efficient patterns
EFFICIENT_PATTERNS = [
    ("Read", "Edit"),
    ("Grep", "Read"),
    ("Read", "Edit", "Read"),
    ("Grep", "Read", "Edit"),
]


def analyze_session_tool_call_sequence_pattern(records: object) -> dict[str, Any]:
    """Analyze tool call sequences and patterns in agent sessions.

    Tracks tool call sequences, identifies common workflows, and detects
    inefficient patterns like excessive re-reads or circular tool chains.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, Edit, Bash, etc.)
            - turn_index: Turn number when tool was called
            - file_path: Optional file path for file-related tools

    Returns:
        Dict with:
            - total_tool_calls: Total number of tool calls
            - sequence_length_stats: Dict with min/max/avg sequence lengths
            - tool_transitions: Dict mapping (from_tool, to_tool) to count
            - common_patterns: List of frequently occurring tool sequences
            - efficient_pattern_count: Count of efficient pattern occurrences
            - inefficient_patterns: List of detected inefficient patterns
            - circular_reads: Count of files read multiple times without edits

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
    tool_sequence: list[str] = []
    file_reads: dict[str, list[int]] = defaultdict(list)  # Track file read positions
    file_edits: set[str] = set()  # Track which files were edited

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tool_sequence.append(tool_name)

        # Track file operations for circular read detection
        file_path = _string(record.get("file_path"))
        if file_path:
            if tool_name.lower() in ("read", "glob", "grep"):
                file_reads[file_path].append(index)
            elif tool_name.lower() in ("edit", "write"):
                file_edits.add(file_path)

    total_calls = len(tool_sequence)

    # Analyze sequences
    sequence_stats = _calculate_sequence_stats(tool_sequence)
    tool_transitions = _calculate_transitions(tool_sequence)
    common_patterns = _find_common_patterns(tool_sequence)
    efficient_count = _count_efficient_patterns(common_patterns)
    inefficient = _detect_inefficient_patterns(tool_sequence)
    circular_reads = _count_circular_reads(file_reads, file_edits)

    return {
        "total_tool_calls": total_calls,
        "sequence_length_stats": sequence_stats,
        "tool_transitions": tool_transitions,
        "common_patterns": common_patterns,
        "efficient_pattern_count": efficient_count,
        "inefficient_patterns": inefficient,
        "circular_reads": circular_reads,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tool_calls": 0,
        "sequence_length_stats": {"min": 0, "max": 0, "avg": 0.0},
        "tool_transitions": {},
        "common_patterns": [],
        "efficient_pattern_count": 0,
        "inefficient_patterns": [],
        "circular_reads": 0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _calculate_sequence_stats(sequence: list[str]) -> dict[str, Any]:
    """Calculate sequence length statistics.

    For now, we treat the entire sequence as one long sequence.
    Returns min/max/avg all equal to the total length.
    """
    if not sequence:
        return {"min": 0, "max": 0, "avg": 0.0}

    length = len(sequence)
    return {
        "min": length,
        "max": length,
        "avg": float(length),
    }


def _calculate_transitions(sequence: list[str]) -> dict[str, int]:
    """Calculate tool transition frequencies.

    Returns dict mapping "ToolA->ToolB" to count.
    """
    if len(sequence) < 2:
        return {}

    transitions: Counter[str] = Counter()
    for i in range(len(sequence) - 1):
        from_tool = sequence[i]
        to_tool = sequence[i + 1]
        transition = f"{from_tool}->{to_tool}"
        transitions[transition] += 1

    return dict(transitions)


def _find_common_patterns(sequence: list[str]) -> list[dict[str, Any]]:
    """Find frequently occurring tool sequences.

    Returns list of pattern dicts with sequence and count.
    """
    if len(sequence) < MIN_PATTERN_LENGTH:
        return []

    pattern_counts: Counter[tuple[str, ...]] = Counter()

    # Extract patterns of various lengths
    for length in range(MIN_PATTERN_LENGTH, min(MAX_PATTERN_LENGTH + 1, len(sequence) + 1)):
        for i in range(len(sequence) - length + 1):
            pattern = tuple(sequence[i:i + length])
            pattern_counts[pattern] += 1

    # Filter patterns that occur at least MIN_PATTERN_FREQUENCY times
    common = [
        {"pattern": list(pattern), "count": count}
        for pattern, count in pattern_counts.most_common()
        if count >= MIN_PATTERN_FREQUENCY
    ]

    return common[:10]  # Limit to top 10


def _count_efficient_patterns(common_patterns: list[dict[str, Any]]) -> int:
    """Count occurrences of known efficient patterns."""
    count = 0
    for item in common_patterns:
        pattern = tuple(item["pattern"])
        if pattern in EFFICIENT_PATTERNS:
            count += item["count"]
    return count


def _detect_inefficient_patterns(sequence: list[str]) -> list[str]:
    """Detect inefficient tool usage patterns.

    Returns list of inefficiency descriptions.
    """
    inefficiencies = []

    # Detect consecutive reads of same tool without edits
    consecutive_reads = 0
    for i in range(len(sequence)):
        if sequence[i].lower() == "read":
            consecutive_reads += 1
            if consecutive_reads >= 3:
                if "excessive_consecutive_reads" not in inefficiencies:
                    inefficiencies.append("excessive_consecutive_reads")
        else:
            consecutive_reads = 0

    # Detect read-read-read patterns without edits
    for i in range(len(sequence) - 2):
        if (sequence[i].lower() == "read" and
            sequence[i + 1].lower() == "read" and
            sequence[i + 2].lower() == "read"):
            if "triple_read_pattern" not in inefficiencies:
                inefficiencies.append("triple_read_pattern")
            break

    return inefficiencies


def _count_circular_reads(
    file_reads: dict[str, list[int]],
    file_edits: set[str],
) -> int:
    """Count files that were read multiple times without edits between reads.

    A circular read is when a file is read 2+ times but never edited.
    """
    circular = 0
    for file_path, read_positions in file_reads.items():
        if len(read_positions) >= 2 and file_path not in file_edits:
            circular += 1
    return circular
