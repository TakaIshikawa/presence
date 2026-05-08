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
"""

from __future__ import annotations

from collections import Counter
from typing import Any


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

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

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

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

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
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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
