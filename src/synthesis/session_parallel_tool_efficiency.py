"""Session parallel tool call efficiency analyzer for optimization detection.

Analyzes how effectively agents use parallel tool calls within single messages.
Tracks parallelization patterns, calculates efficiency metrics, and identifies
missed opportunities where independent sequential calls could be parallelized.

Efficiency metrics:
- Parallelization rate: Percentage of turns with parallel tool calls
- Average parallel group size: Mean number of tools called together
- Sequential groups: Consecutive tool calls that could be parallelized
- Common parallel patterns: Frequently parallelized tool combinations

Parallel execution benefits:
- Reduced latency through concurrent operations
- Better resource utilization
- Faster session completion
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_parallel_tool_efficiency(records: object) -> dict[str, Any]:
    """Analyze parallel tool call patterns and efficiency in a session.

    Detects parallel tool invocations within single turns, calculates
    parallelization metrics, and identifies missed opportunities for
    parallel execution.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - parallel_group_id: Optional ID grouping parallel calls
            OR
            - tool_call_count: Number of tool calls (backward compat)
            - is_parallel: Whether turn has parallel calls (backward compat)

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - turns_with_tool_calls: Number of turns containing tool calls
            - parallel_turns: Number of turns with parallel tool calls
            - sequential_turns: Number of turns with only sequential calls
            - parallelization_rate: Percentage of tool-using turns with parallel calls
            - total_parallel_groups: Total number of parallel execution groups
            - average_group_size: Mean number of tools per parallel group
            - max_group_size: Largest parallel group observed
            - common_parallel_patterns: Most frequent tool combinations
            - missed_opportunities: Estimated sequential groups that could parallelize

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    total_turns = len(records)
    turns_with_tool_calls = 0
    parallel_turns = 0
    sequential_turns = 0
    parallel_groups: list[int | float] = []
    parallel_patterns: Counter[tuple[str, ...]] = Counter()
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_calls = _extract_tool_calls(record)
        if not tool_calls:
            continue

        turns_with_tool_calls += 1

        # Analyze parallel groups in this turn
        groups = _identify_parallel_groups(tool_calls)

        # Check if any parallel execution occurred
        has_parallel = any(len(group) > 1 for group in groups)

        if has_parallel:
            # Turn has at least one parallel group
            parallel_turns += 1
            # Record parallel group sizes and patterns
            for group in groups:
                if len(group) > 1:
                    parallel_groups.append(len(group))
                    # Record pattern (sorted tool names)
                    pattern = tuple(sorted(tool["tool_name"] for tool in group))
                    parallel_patterns[pattern] += 1
        else:
            # All tool calls are sequential
            sequential_turns += 1
            # Check if they could have been parallelized
            if len(tool_calls) > 1 and _could_be_parallelized(tool_calls):
                missed_opportunities += 1

    # Calculate metrics
    parallelization_rate = _percentage(parallel_turns, turns_with_tool_calls)
    average_group_size = _average(parallel_groups)
    max_group_size = max(parallel_groups) if parallel_groups else 0

    # Format common patterns
    common_patterns = [
        {
            "tools": list(pattern),
            "count": count,
        }
        for pattern, count in parallel_patterns.most_common(5)
    ]

    return {
        "total_turns": total_turns,
        "turns_with_tool_calls": turns_with_tool_calls,
        "parallel_turns": parallel_turns,
        "sequential_turns": sequential_turns,
        "parallelization_rate": parallelization_rate,
        "total_parallel_groups": len(parallel_groups),
        "average_group_size": average_group_size,
        "max_group_size": max_group_size,
        "common_parallel_patterns": common_patterns,
        "missed_opportunities": missed_opportunities,
    }


def _extract_tool_calls(record: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Extract tool calls from turn record.

    Supports multiple formats:
    - tool_calls: List of tool call dicts
    - tool_call_count + is_parallel: Backward compatibility
    """
    # Primary format: tool_calls list
    tool_calls = record.get("tool_calls")
    if isinstance(tool_calls, list):
        result = []
        for call in tool_calls:
            if isinstance(call, Mapping) and "tool_name" in call:
                result.append(dict(call))
        return result

    # Backward compatibility: tool_call_count
    tool_count = record.get("tool_call_count")
    if isinstance(tool_count, int) and tool_count > 0:
        is_parallel = record.get("is_parallel") is True
        # Synthetic tool calls
        return [
            {"tool_name": f"Tool{i}", "parallel_group_id": 0 if is_parallel else i}
            for i in range(tool_count)
        ]

    return []


def _identify_parallel_groups(tool_calls: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """Identify parallel execution groups within tool calls.

    Tool calls with the same parallel_group_id are in the same parallel group.
    If no parallel_group_id, each call is its own sequential group.
    """
    if not tool_calls:
        return []

    # Group by parallel_group_id
    groups_by_id: dict[Any, list[dict[str, Any]]] = {}
    sequential_index = 0

    for call in tool_calls:
        group_id = call.get("parallel_group_id")
        if group_id is None:
            # Sequential call - unique group
            groups_by_id[f"seq_{sequential_index}"] = [call]
            sequential_index += 1
        else:
            # Parallel call - group by ID
            if group_id not in groups_by_id:
                groups_by_id[group_id] = []
            groups_by_id[group_id].append(call)

    return list(groups_by_id.values())


def _could_be_parallelized(tool_calls: list[dict[str, Any]]) -> bool:
    """Heuristic to detect if sequential tool calls could be parallelized.

    Common patterns that can be parallelized:
    - Multiple Read calls
    - Multiple Grep calls
    - Multiple Glob calls
    - Mix of Read/Grep/Glob (information gathering)
    """
    if len(tool_calls) < 2:
        return False

    tool_names = [call.get("tool_name", "").lower() for call in tool_calls]

    # All reads, greps, or globs
    parallelizable_tools = {"read", "grep", "glob", "webfetch"}
    all_parallelizable = all(name in parallelizable_tools for name in tool_names)

    return all_parallelizable


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
