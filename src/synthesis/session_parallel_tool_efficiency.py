"""Session parallel tool call efficiency analyzer for performance optimization.

Analyzes how effectively agents use parallel tool calls within single messages.
Parallel execution allows multiple independent operations to run concurrently,
significantly reducing session latency and improving user experience.

Parallelization metrics:
- Parallel call frequency: How often tools are invoked in parallel
- Parallel group sizes: Number of tools called together
- Tool combinations: Which tools are commonly parallelized
- Sequential opportunities: Independent calls that could have been parallel
- Efficiency gains: Estimated time savings from parallelization

Efficiency patterns:
- Optimal: High parallelization rate with appropriate grouping
- Underutilized: Missed opportunities for parallel execution
- Effective: Moderate parallelization where applicable
- Sequential: Minimal or no parallel execution
"""

from __future__ import annotations

from typing import Any, Mapping


# Tools that are commonly parallelizable when independent
PARALLELIZABLE_TOOLS = {
    "Read",
    "Grep",
    "Glob",
    "WebFetch",
    "Bash",  # Independent bash commands
}


def analyze_session_parallel_tool_efficiency(records: object) -> dict[str, Any]:
    """Analyze parallel tool call efficiency in a session.

    Measures how effectively agents use parallel tool invocations and identifies
    missed opportunities for concurrent execution.

    Args:
        records: List of message dictionaries with keys:
            - message_index: Message number in session
            - tool_calls: List of tool calls in this message
                - tool_name: Name of the tool
                - are_independent: Whether calls could run in parallel
            - turn_index: Turn number

    Returns:
        Dict with:
            - total_messages: Total messages with tool calls
            - parallel_messages: Messages with 2+ parallel tool calls
            - parallelization_rate: Percentage of messages with parallel calls
            - total_tool_calls: Total tool invocations
            - parallel_tool_calls: Tool calls made in parallel
            - parallel_call_rate: Percentage of calls made in parallel
            - average_parallel_group_size: Average tools per parallel group
            - max_parallel_group_size: Largest parallel group
            - common_parallel_patterns: Most frequent tool combinations
            - missed_opportunities: Sequential independent calls count
            - efficiency_pattern: Classification of parallelization usage

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
    parallel_messages = 0
    total_tool_calls = 0
    parallel_tool_calls = 0
    parallel_group_sizes: list[int] = []
    parallel_patterns: dict[tuple[str, ...], int] = {}
    missed_opportunities = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_calls = record.get("tool_calls")
        if not isinstance(tool_calls, list) or not tool_calls:
            continue

        total_messages += 1
        call_count = len(tool_calls)
        total_tool_calls += call_count

        # Check if this is a parallel invocation (2+ tools in one message)
        if call_count >= 2:
            parallel_messages += 1
            parallel_tool_calls += call_count
            parallel_group_sizes.append(call_count)

            # Track tool combination pattern
            tool_names = tuple(sorted(_get_tool_name(call) for call in tool_calls))
            parallel_patterns[tool_names] = parallel_patterns.get(tool_names, 0) + 1
        else:
            # Single tool call - check if it could have been parallelized
            # with previous/next calls
            are_independent = _get_bool(tool_calls[0], "are_independent")
            if are_independent:
                missed_opportunities += 1

    parallelization_rate = _percentage(parallel_messages, total_messages)
    parallel_call_rate = _percentage(parallel_tool_calls, total_tool_calls)
    average_parallel_group_size = _average(parallel_group_sizes)
    max_parallel_group_size = max(parallel_group_sizes) if parallel_group_sizes else 0

    # Get top 5 common patterns
    common_patterns = _format_patterns(parallel_patterns)

    efficiency_pattern = _classify_efficiency_pattern(
        parallelization_rate,
        parallel_call_rate,
        missed_opportunities,
        total_messages,
    )

    return {
        "total_messages": total_messages,
        "parallel_messages": parallel_messages,
        "parallelization_rate": parallelization_rate,
        "total_tool_calls": total_tool_calls,
        "parallel_tool_calls": parallel_tool_calls,
        "parallel_call_rate": parallel_call_rate,
        "average_parallel_group_size": average_parallel_group_size,
        "max_parallel_group_size": max_parallel_group_size,
        "common_parallel_patterns": common_patterns,
        "missed_opportunities": missed_opportunities,
        "efficiency_pattern": efficiency_pattern,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_messages": 0,
        "parallel_messages": 0,
        "parallelization_rate": 0.0,
        "total_tool_calls": 0,
        "parallel_tool_calls": 0,
        "parallel_call_rate": 0.0,
        "average_parallel_group_size": 0.0,
        "max_parallel_group_size": 0,
        "common_parallel_patterns": [],
        "missed_opportunities": 0,
        "efficiency_pattern": "empty",
    }


def _get_tool_name(tool_call: Any) -> str:
    """Extract tool name from tool call."""
    if isinstance(tool_call, Mapping):
        return _string(tool_call.get("tool_name"))
    return ""


def _get_bool(tool_call: Any, key: str) -> bool:
    """Extract boolean value from tool call."""
    if isinstance(tool_call, Mapping):
        return tool_call.get(key) is True
    return False


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of values, returning 0.0 if empty."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _format_patterns(patterns: dict[tuple[str, ...], int]) -> list[dict[str, Any]]:
    """Format patterns for output, returning top 5."""
    sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
    return [
        {"tools": list(tools), "count": count}
        for tools, count in sorted_patterns[:5]
    ]


def _classify_efficiency_pattern(
    parallelization_rate: float,
    parallel_call_rate: float,
    missed_opportunities: int,
    total_messages: int,
) -> str:
    """Classify parallel tool usage efficiency pattern.

    Patterns:
    - optimal: High parallelization (>40%) with high call rate (>50%)
    - effective: Moderate parallelization (20-40%) with good usage
    - underutilized: Low parallelization (<20%) with missed opportunities
    - sequential: Very low or no parallelization
    - simple: Too few messages to classify
    - empty: No messages
    """
    if total_messages == 0:
        return "empty"

    if total_messages < 5:
        return "simple"

    # Optimal: high parallelization across messages and calls
    if parallelization_rate > 40.0 and parallel_call_rate > 50.0:
        return "optimal"

    # Effective: moderate parallelization
    if parallelization_rate >= 20.0 and parallel_call_rate >= 30.0:
        return "effective"

    # Underutilized: low parallelization with missed opportunities
    if parallelization_rate < 20.0 and missed_opportunities > 3:
        return "underutilized"

    # Sequential: minimal parallelization
    if parallelization_rate < 10.0:
        return "sequential"

    # Default: some parallelization but not optimal
    return "moderate"
