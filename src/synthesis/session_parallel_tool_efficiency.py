<<<<<<< HEAD
"""Session parallel tool call efficiency analyzer for parallelization patterns.

Analyzes how effectively agents use parallel tool calls within single messages.
Tracks frequency of parallel invocations, identifies missed parallelization
opportunities, and measures efficiency gains from parallel execution.

Parallel execution metrics:
- Parallelization rate: Percentage of tool calls made in parallel
- Average parallel group size: Mean number of tools called together
- Common parallel patterns: Frequent combinations (multiple Reads, etc.)
- Missed opportunities: Sequential independent calls that could be parallel
- Parallel success rate: Percentage of parallel calls that succeed
- Efficiency score: Overall parallelization effectiveness
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
"""

from __future__ import annotations

<<<<<<< HEAD
from collections import Counter
from typing import Any


def analyze_session_parallel_tool_efficiency(records: object) -> dict[str, Any]:
    """Analyze parallel tool call usage patterns and efficiency.

    Evaluates how effectively agents use parallel tool calls within single
    messages, identifies opportunities for parallelization, and calculates
    efficiency metrics.

    Args:
        records: List of message dictionaries with keys:
            - message_index: Message number
            - tool_calls: List of tool calls in this message (parallel if > 1)
            - Each tool call has: tool_name, success (bool), file_path (optional)

    Returns:
        Dict with:
            - total_messages: Total number of messages analyzed
            - messages_with_parallel_calls: Count of messages with parallel calls
            - parallelization_rate: Percentage of tool calls made in parallel
            - avg_parallel_group_size: Average number of tools in parallel groups
            - total_parallel_calls: Total number of parallel tool invocations
            - parallel_patterns: Common parallel tool combinations
            - missed_opportunities: Potential parallelization opportunities
            - parallel_success_rate: Success rate of parallel calls
            - efficiency_score: Overall parallelization effectiveness (0-100)
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of message dictionaries")

<<<<<<< HEAD
    total_messages = 0
    messages_with_parallel = 0
    total_tool_calls = 0
    total_parallel_calls = 0
    parallel_group_sizes: list[int] = []
    parallel_patterns: Counter[tuple[str, ...]] = Counter()
    missed_opportunities: list[dict[str, Any]] = []
    parallel_successes = 0
    total_parallel_groups = 0

    # Track sequential calls that could be parallelized
    sequential_reads: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_calls = record.get("tool_calls", [])
        if not isinstance(tool_calls, list):
            continue

        total_messages += 1
        num_tools = len(tool_calls)
        total_tool_calls += num_tools

        # Analyze parallel calls
        if num_tools > 1:
            messages_with_parallel += 1
            total_parallel_calls += num_tools
            parallel_group_sizes.append(num_tools)
            total_parallel_groups += 1

            # Extract tool names for pattern analysis
            tool_names = tuple(sorted([
                _string(call.get("tool_name"))
                for call in tool_calls
                if isinstance(call, dict) and call.get("tool_name")
            ]))
            if tool_names:
                parallel_patterns[tool_names] += 1

            # Calculate success rate
            successful_calls = sum(
                1 for call in tool_calls
                if isinstance(call, dict) and call.get("success", True)
            )
            parallel_successes += successful_calls

        # Track sequential calls for missed opportunity detection
        elif num_tools == 1:
            tool_call = tool_calls[0]
            if isinstance(tool_call, dict):
                sequential_reads.append({
                    "message_index": record.get("message_index"),
                    "tool_name": _string(tool_call.get("tool_name")),
                    "file_path": _string(tool_call.get("file_path")) if "file_path" in tool_call else None,
                })

    # Detect missed parallelization opportunities
    missed_opportunities = _detect_missed_opportunities(sequential_reads)

    # Calculate metrics
    parallelization_rate = round(
        (total_parallel_calls / total_tool_calls * 100.0) if total_tool_calls > 0 else 0.0,
        2
    )

    avg_parallel_group_size = round(
        sum(parallel_group_sizes) / len(parallel_group_sizes) if parallel_group_sizes else 0.0,
        2
    )

    parallel_success_rate = round(
        (parallel_successes / total_parallel_calls * 100.0) if total_parallel_calls > 0 else 100.0,
        2
    )

    # Calculate efficiency score (0-100)
    # Based on: parallelization rate (50%), avg group size (30%), success rate (20%)
    # Only include success rate if there were actually parallel calls
    if total_parallel_calls > 0:
        efficiency_score = round(
            (parallelization_rate * 0.5) +
            (min(avg_parallel_group_size / 5.0, 1.0) * 30.0) +
            (parallel_success_rate * 0.2),
            2
        )
    else:
        efficiency_score = 0.0

    # Format parallel patterns
    top_patterns = [
        {"tools": list(pattern), "count": count}
        for pattern, count in parallel_patterns.most_common(10)
    ]

    return {
        "total_messages": total_messages,
        "messages_with_parallel_calls": messages_with_parallel,
        "parallelization_rate": parallelization_rate,
        "avg_parallel_group_size": avg_parallel_group_size,
        "total_parallel_calls": total_parallel_calls,
        "parallel_patterns": top_patterns,
        "missed_opportunities": missed_opportunities[:10],  # Limit to 10
        "parallel_success_rate": parallel_success_rate,
        "efficiency_score": efficiency_score,
    }


=======
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


>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
def _detect_missed_opportunities(
    sequential_calls: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Detect missed parallelization opportunities.

    Looks for:
    - Consecutive Read calls (could be parallelized)
    - Consecutive Grep calls (could be parallelized)
    - Consecutive Glob calls (could be parallelized)
    """
    opportunities: list[dict[str, Any]] = []

    # Group consecutive calls by tool type
    i = 0
    while i < len(sequential_calls):
        current_tool = sequential_calls[i]["tool_name"]

        # Only check Read, Grep, Glob (naturally parallelizable)
        if current_tool not in ("Read", "Grep", "Glob"):
            i += 1
            continue

        # Find consecutive calls of same tool type
        consecutive = [sequential_calls[i]]
        j = i + 1
        while j < len(sequential_calls) and sequential_calls[j]["tool_name"] == current_tool:
            # Only consider truly consecutive (adjacent messages)
            if (sequential_calls[j].get("message_index", 0) -
                sequential_calls[j - 1].get("message_index", 0) == 1):
                consecutive.append(sequential_calls[j])
                j += 1
            else:
                break

        # If we found 2+ consecutive calls of same type, it's a missed opportunity
        if len(consecutive) >= 2:
            opportunities.append({
                "type": f"consecutive_{current_tool.lower()}",
                "count": len(consecutive),
                "start_message": consecutive[0].get("message_index"),
                "tool_name": current_tool,
            })

        i = j if j > i + 1 else i + 1

    return opportunities
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
