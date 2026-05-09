"""Session tool execution parallelization analyzer.

Measures when the agent calls multiple independent tools in a single message
(parallel) vs making sequential calls. Tracks parallelization opportunities
and compliance with agent guidelines.

Parallelization metrics:
- Multi-tool messages: Messages containing 2+ tool calls
- Average tools per message: Mean number of tools in multi-tool messages
- Parallelization ratio: Percentage of parallelizable calls actually parallelized
- Missed opportunities: Independent Read/Grep/Glob calls made sequentially

Agent guidelines context:
Per the agent guidelines: "You can call multiple tools in a single response.
When multiple independent pieces of information are requested and all commands
are likely to succeed, run multiple tool calls in parallel for optimal performance."

Detection patterns:
- Independent reads: Sequential Read calls to different files
- Independent searches: Sequential Grep/Glob calls with different patterns
- Parallelizable tools: Read, Grep, Glob, WebFetch (no data dependencies)
- Sequential tools: Edit, Write, Bash (often have dependencies)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_tool_execution_parallelization(records: object) -> dict[str, Any]:
    """Analyze tool execution parallelization in agent sessions.

    Measures how effectively the agent uses parallel tool calls vs sequential
    calls, and identifies missed parallelization opportunities.

    Args:
        records: List of message dictionaries with keys:
            - message_index: Index of the message in the session
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - file_path: Optional file path (for Read/Edit)
                - pattern: Optional pattern (for Grep/Glob)
                - call_index: Optional index within message
            - turn_index: Optional turn number

    Returns:
        Dict with:
            - total_messages: Total number of messages analyzed
            - messages_with_tools: Messages containing tool calls
            - total_tool_calls: Total number of tool calls
            - multi_tool_messages: Messages with 2+ tool calls
            - single_tool_messages: Messages with exactly 1 tool call
            - avg_tools_per_message: Average tools in multi-tool messages
            - max_tools_per_message: Largest tool call batch
            - parallel_tool_calls: Tool calls made in parallel (2+ per message)
            - sequential_tool_calls: Tool calls made sequentially (1 per message)
            - parallelization_ratio: Percentage of tool calls made in parallel
            - missed_opportunities: Sequential parallelizable calls
            - parallelizable_sequential_calls: Independent calls made sequentially
            - parallel_efficiency_score: 0-100 score based on parallelization
            - common_parallel_patterns: Most frequent parallel tool combinations

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of message dictionaries")

    total_messages = 0
    messages_with_tools = 0
    total_tool_calls = 0
    multi_tool_messages = 0
    single_tool_messages = 0

    parallel_tool_calls = 0
    sequential_tool_calls = 0

    tool_counts: list[int] = []  # For calculating average
    max_tools = 0

    # Track missed opportunities
    missed_opportunities = 0
    parallelizable_sequential_calls = 0

    # Track common patterns
    parallel_patterns: dict[tuple[str, ...], int] = {}

    # Tools that are commonly parallelizable (no data dependencies)
    parallelizable_tools = {"read", "grep", "glob", "webfetch", "websearch"}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_messages += 1

        tool_calls = record.get("tool_calls", [])
        if not isinstance(tool_calls, list):
            continue

        num_tools = len(tool_calls)

        if num_tools == 0:
            continue

        messages_with_tools += 1
        total_tool_calls += num_tools

        if num_tools == 1:
            single_tool_messages += 1
            sequential_tool_calls += 1

            # Check if this single call could have been parallelized with previous
            # This is a simplified heuristic - we look for consecutive single-tool
            # messages with parallelizable tools
            tool_name = _string(tool_calls[0].get("tool_name", "")).lower()
            if tool_name in parallelizable_tools:
                parallelizable_sequential_calls += 1

        else:  # num_tools >= 2
            multi_tool_messages += 1
            parallel_tool_calls += num_tools
            tool_counts.append(num_tools)
            max_tools = max(max_tools, num_tools)

            # Track the pattern of parallel tools
            tool_names = tuple(sorted(
                _string(tc.get("tool_name", "")).lower()
                for tc in tool_calls
            ))
            parallel_patterns[tool_names] = parallel_patterns.get(tool_names, 0) + 1

    # Detect missed opportunities: consecutive single-tool messages with
    # parallelizable tools could have been batched
    # This is a heuristic based on the pattern
    if parallelizable_sequential_calls >= 2:
        # Estimate missed opportunities as pairs of parallelizable sequential calls
        missed_opportunities = parallelizable_sequential_calls // 2

    # Calculate metrics
    avg_tools_per_message = _average(tool_counts)
    parallelization_ratio = _percentage(parallel_tool_calls, total_tool_calls)

    # Calculate efficiency score (0-100)
    # Based on: parallelization ratio, avg batch size, and missed opportunities
    efficiency_score = _calculate_efficiency_score(
        parallelization_ratio,
        avg_tools_per_message,
        missed_opportunities,
        total_tool_calls,
    )

    # Format common patterns
    common_patterns = _format_common_patterns(parallel_patterns)

    return {
        "total_messages": total_messages,
        "messages_with_tools": messages_with_tools,
        "total_tool_calls": total_tool_calls,
        "multi_tool_messages": multi_tool_messages,
        "single_tool_messages": single_tool_messages,
        "avg_tools_per_message": avg_tools_per_message,
        "max_tools_per_message": max_tools,
        "parallel_tool_calls": parallel_tool_calls,
        "sequential_tool_calls": sequential_tool_calls,
        "parallelization_ratio": parallelization_ratio,
        "missed_opportunities": missed_opportunities,
        "parallelizable_sequential_calls": parallelizable_sequential_calls,
        "parallel_efficiency_score": efficiency_score,
        "common_parallel_patterns": common_patterns,
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


def _calculate_efficiency_score(
    parallelization_ratio: float,
    avg_batch_size: float,
    missed_opportunities: int,
    total_calls: int,
) -> float:
    """Calculate parallel efficiency score (0-100).

    Factors:
    - Parallelization ratio: Higher is better (weight: 60%)
    - Average batch size: Larger batches are better (weight: 30%)
    - Missed opportunities: Fewer is better (weight: 10%)

    Args:
        parallelization_ratio: Percentage of parallel tool calls
        avg_batch_size: Average tools per parallel message
        missed_opportunities: Count of missed parallelization opportunities
        total_calls: Total tool calls for context

    Returns:
        Efficiency score from 0.0 to 100.0
    """
    if total_calls == 0:
        return 0.0

    # Component 1: Parallelization ratio (0-100, weight 60%)
    ratio_score = parallelization_ratio * 0.6

    # Component 2: Batch size efficiency (weight 30%)
    # Ideal batch size is 3-5 tools. Score peaks at 3, then gradually decreases
    if avg_batch_size == 0:
        batch_score = 0.0
    elif avg_batch_size <= 3:
        batch_score = (avg_batch_size / 3) * 30.0
    else:
        # Gradually decrease for larger batches (too many may indicate inefficiency)
        batch_score = 30.0 * (1.0 - min((avg_batch_size - 3) / 7, 1.0))

    # Component 3: Missed opportunity penalty (weight 10%)
    # Normalize by total calls to get a ratio
    missed_ratio = missed_opportunities / total_calls if total_calls > 0 else 0
    missed_penalty = max(0, 10.0 - (missed_ratio * 100))

    total_score = ratio_score + batch_score + missed_penalty
    return round(total_score, 2)


def _format_common_patterns(patterns: dict[tuple[str, ...], int]) -> list[dict[str, Any]]:
    """Format common parallel patterns for output.

    Args:
        patterns: Dict mapping tool name tuples to occurrence counts

    Returns:
        List of pattern dicts sorted by frequency, limited to top 5
    """
    if not patterns:
        return []

    sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)

    result = []
    for tools, count in sorted_patterns[:5]:  # Top 5 patterns
        result.append({
            "tools": list(tools),
            "count": count,
        })

    return result
