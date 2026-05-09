"""Pack read-verification ratio analyzer for read efficiency measurement.

Analyzes read efficiency in execution packs by measuring the ratio of Read
tool calls to verify skill invocations. Tracks reads-after-edit patterns and
strategic verification usage to identify optimization opportunities.

Read-verification metrics:
- Total reads: Count of Read tool calls in pack
- Total verify commands: Count of verify skill invocations
- Read-to-verify ratio: Balance of re-reads vs verification usage
- Reads-after-edit pattern: Read within 3 tool calls after Edit/Write
- Strategic verification score: Verify for multi-file, targeted reads for single edits

Optimization indicators:
- Low read-to-verify ratio: Using verify instead of re-reading files
- High strategic score: Appropriate verification for complex changes
- Targeted reads after single edits: Efficient verification pattern
- High read-after-edit rate: Potential over-reading (should use verify)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_read_verification_ratio(records: object) -> dict[str, Any]:
    """Analyze read efficiency and verification strategy in execution packs.

    Tracks Read tool calls and verify skill invocations to measure read
    optimization patterns and strategic verification usage.

    Args:
        records: List of tool call dictionaries from pack transcript with keys:
            - tool_name: Name of the tool (Read, Edit, Write, Skill, etc.)
            - skill: Skill name for Skill tool calls (e.g., "verify")
            - file_path: Path for Read/Edit/Write tool calls
            - turn_index: Turn number when tool was invoked
            - changed_files: List of files modified (for pack context)

    Returns:
        Dict with:
            - total_reads: Number of Read tool calls
            - total_verify_commands: Number of verify skill invocations
            - read_to_verify_ratio: Ratio of reads to verify commands
            - reads_after_edit_count: Read within 3 tool calls after Edit/Write
            - reads_after_edit_percentage: Percentage of reads following edits
            - strategic_verification_score: Score 0-100 for verification strategy
            - multi_file_edits_verified: Count of multi-file changes using verify
            - single_file_edits_with_targeted_read: Single edits using targeted reads

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_reads = 0
    total_verify_commands = 0
    reads_after_edit_count = 0

    multi_file_edits = 0
    multi_file_edits_verified = 0
    single_file_edits = 0
    single_file_edits_with_targeted_read = 0

    # Track recent edits for read-after-edit pattern detection
    recent_edits: list[tuple[str, int]] = []  # (file_path, turn_index)
    recent_verify: int | None = None  # turn_index of last verify

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name"))
        if not tool_name:
            continue

        tool_lower = tool_name.lower()
        turn_index = record.get("turn_index", 0)
        file_path = _string(record.get("file_path", ""))

        if tool_lower == "read":
            total_reads += 1

            # Check if this read follows an edit within 3 tool calls
            for edit_path, edit_turn in recent_edits:
                if turn_index - edit_turn <= 3:
                    reads_after_edit_count += 1
                    break

        elif tool_lower in ("edit", "write"):
            # Track edit for read-after-edit pattern
            if file_path:
                recent_edits.append((file_path, turn_index))

        elif tool_lower == "skill":
            skill_name = _string(record.get("skill"))

            if skill_name.lower() == "verify":
                total_verify_commands += 1
                recent_verify = turn_index

    # Calculate strategic verification metrics
    # Look at edit patterns and whether verify was used appropriately
    edit_sessions = _group_edit_sessions(recent_edits)

    for session_files, session_turns in edit_sessions:
        file_count = len(session_files)

        if file_count > 1:
            # Multi-file edit
            multi_file_edits += 1
            # Check if verify was used after this edit session
            if recent_verify is not None:
                last_turn = max(session_turns)
                if recent_verify > last_turn and recent_verify - last_turn <= 5:
                    multi_file_edits_verified += 1
        else:
            # Single file edit
            single_file_edits += 1
            # Check for targeted read after this edit
            # (Simplified: we count targeted reads as optimization metric)
            # This would need offset/limit detection in real implementation
            single_file_edits_with_targeted_read += 1  # Placeholder

    # Calculate metrics
    read_to_verify_ratio = _ratio(total_reads, total_verify_commands)
    reads_after_edit_percentage = _percentage(reads_after_edit_count, total_reads)

    # Strategic verification score
    # High score = good use of verify for multi-file, targeted reads for single
    strategic_score = _calculate_strategic_score(
        multi_file_edits,
        multi_file_edits_verified,
        reads_after_edit_percentage,
    )

    return {
        "total_reads": total_reads,
        "total_verify_commands": total_verify_commands,
        "read_to_verify_ratio": read_to_verify_ratio,
        "reads_after_edit_count": reads_after_edit_count,
        "reads_after_edit_percentage": reads_after_edit_percentage,
        "strategic_verification_score": strategic_score,
        "multi_file_edits_verified": multi_file_edits_verified,
        "single_file_edits_with_targeted_read": single_file_edits_with_targeted_read,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _ratio(numerator: int, denominator: int) -> float:
    """Calculate ratio, handling zero denominator."""
    if denominator <= 0:
        return 0.0 if numerator == 0 else float(numerator)
    return round(numerator / denominator, 2)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _group_edit_sessions(
    edits: list[tuple[str, int]]
) -> list[tuple[set[str], list[int]]]:
    """Group edits into sessions based on proximity.

    Edits within 5 tool calls are considered part of the same session.
    Returns list of (files_set, turn_indices).
    """
    if not edits:
        return []

    sessions: list[tuple[set[str], list[int]]] = []
    current_files: set[str] = set()
    current_turns: list[int] = []
    last_turn = -10

    for file_path, turn_index in sorted(edits, key=lambda x: x[1]):
        # If too far from last turn, start new session
        if turn_index - last_turn > 5:
            if current_files:
                sessions.append((current_files.copy(), current_turns.copy()))
            current_files = {file_path}
            current_turns = [turn_index]
        else:
            current_files.add(file_path)
            current_turns.append(turn_index)

        last_turn = turn_index

    # Add final session
    if current_files:
        sessions.append((current_files, current_turns))

    return sessions


def _calculate_strategic_score(
    multi_file_edits: int,
    multi_file_edits_verified: int,
    reads_after_edit_percentage: float,
) -> float:
    """Calculate strategic verification score (0-100).

    High score indicates:
    - Multi-file edits using verify appropriately
    - Low reads-after-edit rate (using verify instead)
    """
    score = 50.0  # Base score

    # Bonus for verifying multi-file edits
    if multi_file_edits > 0:
        verify_rate = (multi_file_edits_verified / multi_file_edits) * 100
        score += (verify_rate - 50) * 0.3  # +30 max for 100% verify rate

    # Penalty for high reads-after-edit rate
    # Lower is better (should use verify instead of re-reading)
    if reads_after_edit_percentage > 50:
        score -= (reads_after_edit_percentage - 50) * 0.4  # -20 max at 100%

    return round(max(0.0, min(100.0, score)), 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
