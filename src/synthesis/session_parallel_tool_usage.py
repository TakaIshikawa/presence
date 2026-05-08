"""Session parallel tool usage analyzer for agent efficiency measurement.

Analyzes agent efficiency in parallel tool execution by tracking percentage
of tool calls made in parallel vs sequential, identifying missed parallelization
opportunities, and comparing parallelization patterns across optimization modes.

Parallelization metrics:
- Parallel usage rate: Percentage of tool call opportunities used in parallel
- Missed opportunities: Independent tool calls made sequentially
- Average parallel batch size: Mean number of tools called together
- Parallelization by tool type: Which tools are frequently parallelized
- Mode comparison: Parallel usage in optimized vs baseline modes

Efficiency patterns:
- High parallelization: Frequent parallel batches with large sizes
- Sequential execution: Minimal parallel usage despite opportunities
- Tool-specific patterns: Some tools parallelized more than others
- Optimization impact: Improved parallelization in optimized mode
"""

from __future__ import annotations

from collections import Counter
from typing import Any


def analyze_session_parallel_tool_usage(records: object) -> dict[str, Any]:
    """Analyze parallel tool usage patterns in agent sessions.

    Tracks parallel execution efficiency, missed opportunities, and
    parallelization patterns by tool type and optimization mode.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
                - timestamp: Optional call timestamp
            - optimization_mode: Optional mode (baseline/optimized)

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - turns_with_tools: Turns containing tool calls
            - total_tool_calls: Total number of tool calls
            - parallel_turns: Count of turns with 2+ parallel tools
            - parallel_usage_rate: Percentage of tool turns using parallelization
            - total_parallel_batches: Count of parallel execution batches
            - avg_parallel_batch_size: Average size of parallel batches
            - max_parallel_batch_size: Largest parallel batch observed
            - missed_opportunities: Estimated sequential calls that could be parallel
            - tool_parallelization: Dict mapping tool names to parallel usage counts
            - mode_comparison: Dict comparing baseline vs optimized parallelization
            - examples: Example turns with different parallelization patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    turns_with_tools = 0
    total_tool_calls = 0
    parallel_turns = 0
    parallel_batch_sizes: list[int] = []
    tool_parallelization: Counter[str] = Counter()
    missed_opportunities = 0
    examples: list[dict[str, Any]] = []

    # Mode comparison tracking
    mode_stats: dict[str, dict[str, int]] = {
        "baseline": {"turns_with_tools": 0, "parallel_turns": 0, "total_tool_calls": 0},
        "optimized": {"turns_with_tools": 0, "parallel_turns": 0, "total_tool_calls": 0},
    }

    # Track consecutive sequential turns for opportunity detection
    previous_turn_tools: list[str] = []

    for record in records:
        if not isinstance(record, dict):
            continue

        total_turns += 1
        tool_calls = record.get("tool_calls")
        optimization_mode = _string(record.get("optimization_mode", "")).lower()

        if not isinstance(tool_calls, list) or not tool_calls:
            previous_turn_tools = []
            continue

        # Extract tool names from this turn
        tool_names: list[str] = []
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            tool_name = _string(call.get("tool_name"))
            if tool_name:
                tool_names.append(tool_name)

        if not tool_names:
            previous_turn_tools = []
            continue

        turns_with_tools += 1
        num_tools = len(tool_names)
        total_tool_calls += num_tools

        # Track mode-specific stats
        if optimization_mode in mode_stats:
            mode_stats[optimization_mode]["turns_with_tools"] += 1
            mode_stats[optimization_mode]["total_tool_calls"] += num_tools

        # Check if this turn uses parallelization
        is_parallel = num_tools >= 2
        if is_parallel:
            parallel_turns += 1
            parallel_batch_sizes.append(num_tools)

            # Track which tools are parallelized
            for tool_name in set(tool_names):
                tool_parallelization[tool_name] += 1

            # Track mode-specific parallel usage
            if optimization_mode in mode_stats:
                mode_stats[optimization_mode]["parallel_turns"] += 1

            # Collect example
            if len(examples) < 10:
                examples.append({
                    "turn_index": record.get("turn_index", total_turns),
                    "tools": tool_names,
                    "batch_size": num_tools,
                    "optimization_mode": optimization_mode or "unknown",
                })
        else:
            # Single tool call - check for missed parallelization opportunity
            # If previous turn also had single tool call with different tool, potential opportunity
            if len(previous_turn_tools) == 1 and previous_turn_tools[0] != tool_names[0]:
                # Different tools in consecutive turns = missed opportunity
                missed_opportunities += 1

        previous_turn_tools = tool_names

    # Calculate metrics
    parallel_usage_rate = _percentage(parallel_turns, turns_with_tools)
    total_parallel_batches = len(parallel_batch_sizes)
    avg_batch_size = _average(sum(parallel_batch_sizes), len(parallel_batch_sizes))
    max_batch_size = max(parallel_batch_sizes) if parallel_batch_sizes else 0

    # Calculate mode comparison
    mode_comparison = {}
    for mode, stats in mode_stats.items():
        mode_comparison[mode] = {
            "turns_with_tools": stats["turns_with_tools"],
            "parallel_turns": stats["parallel_turns"],
            "parallel_usage_rate": _percentage(stats["parallel_turns"], stats["turns_with_tools"]),
            "total_tool_calls": stats["total_tool_calls"],
        }

    return {
        "total_turns": total_turns,
        "turns_with_tools": turns_with_tools,
        "total_tool_calls": total_tool_calls,
        "parallel_turns": parallel_turns,
        "parallel_usage_rate": parallel_usage_rate,
        "total_parallel_batches": total_parallel_batches,
        "avg_parallel_batch_size": avg_batch_size,
        "max_parallel_batch_size": max_batch_size,
        "missed_opportunities": missed_opportunities,
        "tool_parallelization": dict(tool_parallelization.most_common(10)),
        "mode_comparison": mode_comparison,
        "examples": examples[:5],  # Limit to 5 examples
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "turns_with_tools": 0,
        "total_tool_calls": 0,
        "parallel_turns": 0,
        "parallel_usage_rate": 0.0,
        "total_parallel_batches": 0,
        "avg_parallel_batch_size": 0.0,
        "max_parallel_batch_size": 0,
        "missed_opportunities": 0,
        "tool_parallelization": {},
        "mode_comparison": {
            "baseline": {
                "turns_with_tools": 0,
                "parallel_turns": 0,
                "parallel_usage_rate": 0.0,
                "total_tool_calls": 0,
            },
            "optimized": {
                "turns_with_tools": 0,
                "parallel_turns": 0,
                "parallel_usage_rate": 0.0,
                "total_tool_calls": 0,
            },
        },
        "examples": [],
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
