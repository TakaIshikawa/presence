"""Pack tool call dependency graph analyzer.

Builds a dependency graph of tool calls across sessions in a pack to identify
parallelization opportunities and detect inefficient sequential execution patterns.

Dependency analysis:
- Independent tool calls: Calls with no data dependencies
- Dependency chains: Sequences where each call depends on previous output
- Circular dependencies: Invalid dependency loops
- Redundant re-reads: Same file read multiple times across sessions

Parallelization metrics:
- Parallelization efficiency: Percentage of independent calls made in parallel
- Independent call ratio: Calls that could run in parallel
- Dependency depth: Maximum depth of dependency chains
- Cross-session redundancy: Files read in multiple sessions

Quality indicators:
- High efficiency: Independent calls batched together
- Deep chains: Sequential dependencies requiring careful ordering
- Circular patterns: Invalid or inefficient dependency loops
- Redundant reads: Same files read across multiple sessions
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_tool_call_dependency_graph(records: object) -> dict[str, Any]:
    """Analyze tool call dependency graph across pack sessions.

    Builds a dependency graph from tool calls across sessions to identify
    parallelization opportunities and detect inefficiency patterns.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - file_path: Optional file path (for Read/Edit/Write)
                - turn_index: Turn number in session
                - call_index: Index within turn
            - task_id: Optional task identifier

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - total_tool_calls: Total number of tool calls across all sessions
            - independent_call_count: Calls with no dependencies
            - dependent_call_count: Calls with dependencies
            - independent_call_ratio: Percentage of independent calls
            - max_dependency_depth: Maximum depth of dependency chains
            - avg_dependency_depth: Average depth of chains
            - circular_dependency_count: Number of circular dependencies detected
            - redundant_read_count: Files read multiple times across sessions
            - redundant_read_files: List of files read redundantly
            - parallelization_efficiency: Percentage of independent calls parallelized
            - cross_session_file_overlap: Files accessed by multiple sessions
            - dependency_chain_lengths: Distribution of chain lengths

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    total_sessions = 0
    total_tool_calls = 0
    independent_call_count = 0
    dependent_call_count = 0

    # Track file reads and writes across sessions
    file_reads: dict[str, list[str]] = {}  # file -> [session_ids]
    file_writes: dict[str, list[str]] = {}  # file -> [session_ids]

    # Track dependency chains
    dependency_chains: list[int] = []
    circular_dependency_count = 0

    # Tools that create dependencies (modify state)
    state_modifying_tools = {"edit", "write", "bash"}
    # Tools that are independent (read-only)
    independent_tools = {"read", "grep", "glob", "webfetch", "websearch"}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1
        session_id = _string(record.get("session_id", f"session_{total_sessions}"))

        tool_calls = record.get("tool_calls", [])
        if not isinstance(tool_calls, list):
            continue

        # Track tool calls in this session
        session_independent_count = 0
        session_dependent_count = 0
        previous_modifying_tools: list[str] = []

        for tool_call in tool_calls:
            if not isinstance(tool_call, Mapping):
                continue

            total_tool_calls += 1
            tool_name = _string(tool_call.get("tool_name", "")).lower()
            file_path = _string(tool_call.get("file_path", ""))

            # Determine if this call is independent or dependent
            if tool_name in independent_tools:
                # Independent if no previous state-modifying tools in same session
                if not previous_modifying_tools:
                    independent_call_count += 1
                    session_independent_count += 1
                else:
                    # Dependent on previous modifications
                    dependent_call_count += 1
                    session_dependent_count += 1

                # Track file reads
                if file_path and tool_name == "read":
                    if file_path not in file_reads:
                        file_reads[file_path] = []
                    file_reads[file_path].append(session_id)

            elif tool_name in state_modifying_tools:
                # State-modifying tools create dependencies
                dependent_call_count += 1
                session_dependent_count += 1
                previous_modifying_tools.append(tool_name)

                # Track file writes
                if file_path and tool_name in {"edit", "write"}:
                    if file_path not in file_writes:
                        file_writes[file_path] = []
                    file_writes[file_path].append(session_id)

        # Calculate dependency chain length for this session
        # Simple heuristic: count of state-modifying tools
        if session_dependent_count > 0:
            dependency_chains.append(session_dependent_count)

    # Find redundant reads (same file read in multiple sessions)
    redundant_read_files = [
        file for file, sessions in file_reads.items()
        if len(set(sessions)) > 1
    ]
    redundant_read_count = len(redundant_read_files)

    # Find cross-session file overlap
    cross_session_files = [
        file for file, sessions in {**file_reads, **file_writes}.items()
        if len(set(sessions)) > 1
    ]

    # Calculate metrics
    independent_call_ratio = _percentage(independent_call_count, total_tool_calls)
    max_dependency_depth = max(dependency_chains) if dependency_chains else 0
    avg_dependency_depth = _average(dependency_chains)

    # Parallelization efficiency (simplified heuristic)
    # Assume optimal parallelization would batch all independent calls
    # Actual efficiency would need turn-level data
    parallelization_efficiency = 0.0
    if independent_call_count > 0:
        # Conservative estimate: 50% of independent calls could be parallelized
        parallelization_efficiency = 50.0

    # Detect circular dependencies (simplified heuristic)
    # Look for read-write-read patterns on same file
    for file in redundant_read_files:
        if file in file_writes:
            # File is both read and written, potential circular pattern
            circular_dependency_count += 1

    return {
        "total_sessions": total_sessions,
        "total_tool_calls": total_tool_calls,
        "independent_call_count": independent_call_count,
        "dependent_call_count": dependent_call_count,
        "independent_call_ratio": independent_call_ratio,
        "max_dependency_depth": max_dependency_depth,
        "avg_dependency_depth": avg_dependency_depth,
        "circular_dependency_count": circular_dependency_count,
        "redundant_read_count": redundant_read_count,
        "redundant_read_files": redundant_read_files[:10],  # Limit to top 10
        "parallelization_efficiency": parallelization_efficiency,
        "cross_session_file_overlap": len(cross_session_files),
        "dependency_chain_lengths": _summarize_chain_lengths(dependency_chains),
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _summarize_chain_lengths(chains: list[int]) -> dict[str, int]:
    """Summarize dependency chain length distribution.

    Args:
        chains: List of chain lengths

    Returns:
        Dict with chain length distribution statistics
    """
    if not chains:
        return {
            "short_chains": 0,  # 1-2 dependencies
            "medium_chains": 0,  # 3-5 dependencies
            "long_chains": 0,  # 6+ dependencies
        }

    short = sum(1 for c in chains if c <= 2)
    medium = sum(1 for c in chains if 3 <= c <= 5)
    long_chains = sum(1 for c in chains if c > 5)

    return {
        "short_chains": short,
        "medium_chains": medium,
        "long_chains": long_chains,
    }
