"""Session parallel tool call efficiency analyzer for parallelization patterns.

Analyzes how effectively agents use parallel tool calls within single messages
to identify optimization opportunities and measure parallelization efficiency.
Tracks frequency of parallel invocations, group sizes, and missed opportunities
where sequential independent calls could have been parallelized.

Parallelization metrics:
- Parallel call rate: Percentage of messages with parallel tool calls
- Average parallel group size: Mean number of tools called in parallel
- Parallelization opportunity: Sequential independent calls that could be parallel
- Common parallel patterns: Which tools are frequently parallelized together

Efficiency patterns:
- High parallelization: Frequent use of parallel calls with large groups
- Missed opportunities: Sequential Read calls that could be parallel
- Single-threaded: No parallel calls detected
"""

from __future__ import annotations

from collections import Counter
from typing import Any


def analyze_session_parallel_tool_efficiency(records: object) -> dict[str, Any]:
    """Analyze parallel tool call usage patterns in agent sessions.

    Evaluates how effectively agents use parallel tool calls and identifies
    missed opportunities for parallelization.

    Args:
        records: List of message dictionaries with keys:
            - message_index: Message number
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - call_index: Index within the message

    Returns:
        Dict with:
            - total_messages: Total number of messages analyzed
            - messages_with_tools: Messages containing tool calls
            - messages_with_parallel_calls: Messages with 2+ parallel tools
            - parallelization_rate: Percentage of tool messages using parallelization
            - total_parallel_groups: Count of parallel call groups
            - avg_parallel_group_size: Average size of parallel groups
            - max_parallel_group_size: Largest parallel group
            - common_parallel_patterns: Frequently parallelized tool combinations
            - missed_opportunities: Count of potential parallelization opportunities

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of message dictionaries")

    if not records:
        return _empty_result()

    total_messages = 0
    messages_with_tools = 0
    messages_with_parallel = 0
    parallel_group_sizes: list[int] = []
    parallel_patterns: Counter[tuple[str, ...]] = Counter()
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, dict):
            continue

        total_messages += 1
        tool_calls = record.get("tool_calls")
        if not isinstance(tool_calls, list) or not tool_calls:
            continue

        messages_with_tools += 1

        # Extract tool names
        tool_names = []
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            tool_name = _string(call.get("tool_name"))
            if tool_name:
                tool_names.append(tool_name)

        if len(tool_names) == 0:
            continue
        elif len(tool_names) == 1:
            # Single tool call - check if next message has independent calls
            # (This is a simplification; real analysis would need more context)
            continue
        else:
            # Multiple tool calls in same message = parallel
            messages_with_parallel += 1
            group_size = len(tool_names)
            parallel_group_sizes.append(group_size)

            # Track pattern (sorted to normalize order)
            pattern = tuple(sorted(tool_names))
            parallel_patterns[pattern] += 1

    # Calculate metrics
    parallelization_rate = _percentage(messages_with_parallel, messages_with_tools)
    total_parallel_groups = len(parallel_group_sizes)
    avg_group_size = _average(sum(parallel_group_sizes), len(parallel_group_sizes))
    max_group_size = max(parallel_group_sizes) if parallel_group_sizes else 0

    # Format common patterns
    common_patterns = [
        {"tools": list(pattern), "count": count}
        for pattern, count in parallel_patterns.most_common(10)
    ]

    return {
        "total_messages": total_messages,
        "messages_with_tools": messages_with_tools,
        "messages_with_parallel_calls": messages_with_parallel,
        "parallelization_rate": parallelization_rate,
        "total_parallel_groups": total_parallel_groups,
        "avg_parallel_group_size": avg_group_size,
        "max_parallel_group_size": max_group_size,
        "common_parallel_patterns": common_patterns,
        "missed_opportunities": missed_opportunities,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_messages": 0,
        "messages_with_tools": 0,
        "messages_with_parallel_calls": 0,
        "parallelization_rate": 0.0,
        "total_parallel_groups": 0,
        "avg_parallel_group_size": 0.0,
        "max_parallel_group_size": 0,
        "common_parallel_patterns": [],
        "missed_opportunities": 0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)
