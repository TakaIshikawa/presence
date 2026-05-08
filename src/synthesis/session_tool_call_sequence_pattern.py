<<<<<<< HEAD
"""Session tool call sequence pattern analyzer for workflow analysis.

Analyzes sequences and patterns of tool calls within agent sessions to identify
common workflows, inefficient patterns, and optimal sequences. Tracks consecutive
tool usage patterns and transition frequencies.

Sequence metrics:
- Common patterns: Read→Edit→Read, Grep→Read→Edit sequences
- Sequence lengths: Distribution of consecutive tool chains
- Tool transitions: Frequency matrix of tool-to-tool transitions
- Inefficient patterns: Excessive re-reads, circular tool chains
- Pattern efficiency scores: Quality ratings for common workflows
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
"""

from __future__ import annotations

from collections import Counter
from typing import Any


<<<<<<< HEAD
def analyze_session_tool_call_sequence_pattern(records: object) -> dict[str, Any]:
    """Analyze tool call sequences and patterns in agent sessions.

    Tracks consecutive tool usage patterns, identifies common workflows,
    and detects inefficient patterns like excessive re-reads or circular chains.

    Args:
        records: List of tool call dictionaries with keys:
            - tool_name: Name of the tool (Read, Write, Edit, Bash, etc.)
            - turn_index: Turn number when tool was called
            - file_path: Optional file path for file-based tools

    Returns:
        Dict with:
            - total_sequences: Total number of tool call sequences analyzed
            - common_patterns: Most frequent tool sequences (top 10)
            - sequence_length_distribution: Distribution of sequence lengths
            - tool_transitions: Matrix of tool-to-tool transition frequencies
            - inefficient_patterns: List of detected inefficient sequences
            - avg_sequence_length: Average length of tool sequences
            - most_common_workflow: Most frequently used workflow pattern
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

<<<<<<< HEAD
    if len(records) == 0:
        return {
            "total_sequences": 0,
            "common_patterns": [],
            "sequence_length_distribution": {},
            "tool_transitions": {},
            "inefficient_patterns": [],
            "avg_sequence_length": 0.0,
            "most_common_workflow": None,
        }

    # Extract tool sequence
    tool_sequence: list[str] = []
    file_paths: list[str | None] = []
=======
    if not records:
        return _empty_result()

    # Extract tool sequence
    tools: list[str] = []
    file_paths: list[str] = []
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

<<<<<<< HEAD
        tool_sequence.append(tool_name)
        file_paths.append(_string(record.get("file_path")) if "file_path" in record else None)

    if len(tool_sequence) == 0:
        return {
            "total_sequences": 0,
            "common_patterns": [],
            "sequence_length_distribution": {},
            "tool_transitions": {},
            "inefficient_patterns": [],
            "avg_sequence_length": 0.0,
            "most_common_workflow": None,
        }

    # Analyze patterns
    bigram_patterns = _extract_bigrams(tool_sequence)
    trigram_patterns = _extract_trigrams(tool_sequence)
    tool_transitions = _calculate_transitions(tool_sequence)
    sequence_lengths = _calculate_sequence_lengths(tool_sequence)
    inefficient_patterns = _detect_inefficient_patterns(tool_sequence, file_paths)

    # Get top patterns
    top_bigrams = bigram_patterns.most_common(10)
    top_trigrams = trigram_patterns.most_common(10)

    # Combine patterns
    common_patterns = [
        {"pattern": " → ".join(pattern), "count": count}
        for pattern, count in top_trigrams
    ] + [
        {"pattern": " → ".join(pattern), "count": count}
        for pattern, count in top_bigrams[:5]  # Add top 5 bigrams
    ]

    # Get most common workflow
    most_common_workflow = None
    if top_trigrams:
        most_common_workflow = " → ".join(top_trigrams[0][0])

    # Calculate average sequence length
    # Sum of (length * count) / total number of sequences
    total_sequences = sum(sequence_lengths.values())
    weighted_sum = sum(length * count for length, count in sequence_lengths.items())
    avg_sequence_length = round(weighted_sum / total_sequences, 2) if total_sequences > 0 else 0.0

    return {
        "total_sequences": len(tool_sequence),
        "common_patterns": common_patterns[:10],  # Limit to top 10
        "sequence_length_distribution": dict(sequence_lengths),
        "tool_transitions": tool_transitions,
        "inefficient_patterns": inefficient_patterns[:10],  # Limit to top 10
        "avg_sequence_length": avg_sequence_length,
        "most_common_workflow": most_common_workflow,
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
def _extract_bigrams(sequence: list[str]) -> Counter[tuple[str, str]]:
    """Extract consecutive pairs of tool calls (bigrams)."""
    bigrams: Counter[tuple[str, str]] = Counter()
    for i in range(len(sequence) - 1):
        bigrams[(sequence[i], sequence[i + 1])] += 1
    return bigrams


def _extract_trigrams(sequence: list[str]) -> Counter[tuple[str, str, str]]:
    """Extract consecutive triplets of tool calls (trigrams)."""
    trigrams: Counter[tuple[str, str, str]] = Counter()
    for i in range(len(sequence) - 2):
        trigrams[(sequence[i], sequence[i + 1], sequence[i + 2])] += 1
    return trigrams


def _calculate_transitions(sequence: list[str]) -> dict[str, dict[str, int]]:
    """Calculate tool-to-tool transition frequency matrix."""
    transitions: dict[str, dict[str, int]] = {}

    for i in range(len(sequence) - 1):
        from_tool = sequence[i]
        to_tool = sequence[i + 1]

        if from_tool not in transitions:
            transitions[from_tool] = {}

        transitions[from_tool][to_tool] = transitions[from_tool].get(to_tool, 0) + 1

    return transitions


def _calculate_sequence_lengths(sequence: list[str]) -> Counter[int]:
    """Calculate distribution of consecutive same-tool sequences."""
    lengths: Counter[int] = Counter()

    if not sequence:
        return lengths

    current_tool = sequence[0]
    current_length = 1

    for i in range(1, len(sequence)):
        if sequence[i] == current_tool:
            current_length += 1
        else:
            lengths[current_length] += 1
            current_tool = sequence[i]
            current_length = 1

    # Add final sequence
    lengths[current_length] += 1

    return lengths


def _detect_inefficient_patterns(
    sequence: list[str],
    file_paths: list[str | None],
) -> list[dict[str, Any]]:
    """Detect inefficient patterns in tool call sequences.

    Patterns detected:
    - Excessive re-reads: Same file read multiple times consecutively
    - Circular reads: Read → Edit → Read on same file
    - Read chains: Many consecutive reads without edits
    - Edit without prior read: Editing file without reading first
    """
    patterns: list[dict[str, Any]] = []

    # Detect excessive re-reads (same file read 3+ times in a row)
    for i in range(len(sequence) - 2):
        if (
            sequence[i] == "Read"
            and sequence[i + 1] == "Read"
            and sequence[i + 2] == "Read"
            and file_paths[i]
            and file_paths[i] == file_paths[i + 1] == file_paths[i + 2]
        ):
            patterns.append({
                "type": "excessive_re_reads",
                "position": i,
                "file": file_paths[i],
            })

    # Detect circular read patterns (Read → Edit → Read on same file)
    for i in range(len(sequence) - 2):
        if (
            sequence[i] == "Read"
            and sequence[i + 1] == "Edit"
            and sequence[i + 2] == "Read"
            and file_paths[i]
            and file_paths[i] == file_paths[i + 2]
        ):
            patterns.append({
                "type": "circular_read_edit_read",
                "position": i,
                "file": file_paths[i],
            })

    # Detect long read chains (5+ consecutive reads)
    consecutive_reads = 0
    for i, tool in enumerate(sequence):
        if tool == "Read":
            consecutive_reads += 1
        else:
            if consecutive_reads >= 5:
                patterns.append({
                    "type": "excessive_read_chain",
                    "position": i - consecutive_reads,
                    "count": consecutive_reads,
                })
            consecutive_reads = 0

    # Check final sequence
    if consecutive_reads >= 5:
        patterns.append({
            "type": "excessive_read_chain",
            "position": len(sequence) - consecutive_reads,
            "count": consecutive_reads,
        })

    # Detect edit without prior read
    for i in range(len(sequence)):
        if sequence[i] in ("Edit", "Write") and file_paths[i]:
            # Check if file was read before
            file_read_before = False
            for j in range(i):
                if sequence[j] == "Read" and file_paths[j] == file_paths[i]:
                    file_read_before = True
                    break

            if not file_read_before:
                patterns.append({
                    "type": "edit_without_read",
                    "position": i,
                    "file": file_paths[i],
                })

    return patterns
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
