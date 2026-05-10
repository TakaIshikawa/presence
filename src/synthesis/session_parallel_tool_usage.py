<<<<<<< HEAD
"""Session parallel tool usage analyzer for parallel execution efficiency.

Analyzes agent efficiency in parallel tool execution by tracking parallel vs
sequential tool calls, detecting missed parallelization opportunities, and
measuring effectiveness across optimization modes.

Parallel execution metrics:
- Parallel call rate: Percentage of tool calls made in parallel vs sequential
- Opportunity detection: Independent sequential calls that could be parallel
- Average parallel batch size: Mean number of tools in parallel groups
- Parallelization by tool type: Which tools are frequently parallelized
- Optimization mode comparison: Parallel usage in baseline vs optimized modes

Efficiency patterns:
- High efficiency: Frequent parallel batches with large group sizes
- Missed opportunities: Sequential Read/Grep calls that could be parallel
- Optimization improvement: Higher parallelization in optimized mode
- Tool-specific patterns: Read and Grep commonly parallelized together

Opportunity detection heuristics:
- Sequential Read calls within same turn likely independent
- Sequential Grep calls with different patterns likely independent
- Sequential Bash commands without dependencies could be parallel
- Tool calls separated by non-dependent operations are candidates
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
"""

from __future__ import annotations

<<<<<<< HEAD
from collections import Counter, defaultdict
from typing import Any

# Tools commonly parallelizable when used sequentially
PARALLELIZABLE_TOOLS = {"Read", "Grep", "Glob", "WebFetch"}

# Tools that often have dependencies (less likely to be parallel opportunities)
SEQUENTIAL_TOOLS = {"Edit", "Write", "Bash"}


def analyze_session_parallel_tool_usage(records: object) -> dict[str, Any]:
    """Analyze parallel tool execution efficiency in agent sessions.

    Evaluates how effectively agents use parallel tool calls, detects missed
    parallelization opportunities, and compares efficiency across optimization modes.
=======
from collections import Counter
from typing import Any


def analyze_session_parallel_tool_usage(records: object) -> dict[str, Any]:
    """Analyze parallel tool usage patterns in agent sessions.

    Tracks parallel execution efficiency, missed opportunities, and
    parallelization patterns by tool type and optimization mode.
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_calls: List of tool call dicts with:
                - tool_name: Name of the tool
<<<<<<< HEAD
                - call_index: Index within the turn
            - optimization_mode: Optional mode (baseline/optimized)
            - turn_duration: Optional duration in seconds
=======
                - timestamp: Optional call timestamp
            - optimization_mode: Optional mode (baseline/optimized)
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - turns_with_tools: Turns containing tool calls
            - total_tool_calls: Total number of tool calls
<<<<<<< HEAD
            - parallel_tool_calls: Tool calls made in parallel (2+ per turn)
            - sequential_tool_calls: Tool calls made sequentially (1 per turn)
            - parallel_usage_percentage: Percentage of calls made in parallel
            - turns_with_parallel: Number of turns with parallel calls
            - avg_parallel_batch_size: Average size of parallel batches
            - max_parallel_batch_size: Largest parallel batch
            - missed_opportunities: Count of sequential calls that could be parallel
            - parallelization_by_tool: Breakdown by tool type
            - common_parallel_patterns: Frequently parallelized tool combinations
            - optimization_mode_comparison: Metrics by baseline/optimized mode
            - parallel_efficiency_score: Overall efficiency (0-100)
=======
            - parallel_turns: Count of turns with 2+ parallel tools
            - parallel_usage_rate: Percentage of tool turns using parallelization
            - total_parallel_batches: Count of parallel execution batches
            - avg_parallel_batch_size: Average size of parallel batches
            - max_parallel_batch_size: Largest parallel batch observed
            - missed_opportunities: Estimated sequential calls that could be parallel
            - tool_parallelization: Dict mapping tool names to parallel usage counts
            - mode_comparison: Dict comparing baseline vs optimized parallelization
            - examples: Example turns with different parallelization patterns
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

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
<<<<<<< HEAD
    parallel_tool_calls = 0
    sequential_tool_calls = 0
    turns_with_parallel = 0

    parallel_batch_sizes: list[int] = []
    parallel_patterns: Counter[tuple[str, ...]] = Counter()
    missed_opportunities = 0

    # Track parallelization by tool type
    tool_parallel_counts: defaultdict[str, int] = defaultdict(int)
    tool_sequential_counts: defaultdict[str, int] = defaultdict(int)

    # Track by optimization mode
    mode_stats: defaultdict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "turns": 0,
            "parallel_calls": 0,
            "sequential_calls": 0,
            "parallel_turns": 0,
            "total_duration": 0.0,
        }
    )

    # Track previous turn for opportunity detection
    previous_tool_calls: list[str] = []
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    for record in records:
        if not isinstance(record, dict):
            continue

        total_turns += 1
        tool_calls = record.get("tool_calls")
<<<<<<< HEAD
        if not isinstance(tool_calls, list) or not tool_calls:
            previous_tool_calls = []
            continue

        # Extract tool names
        tool_names = []
=======
        optimization_mode = _string(record.get("optimization_mode", "")).lower()

        if not isinstance(tool_calls, list) or not tool_calls:
            previous_turn_tools = []
            continue

        # Extract tool names from this turn
        tool_names: list[str] = []
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            tool_name = _string(call.get("tool_name"))
            if tool_name:
                tool_names.append(tool_name)

        if not tool_names:
<<<<<<< HEAD
            previous_tool_calls = []
            continue

        turns_with_tools += 1
        call_count = len(tool_names)
        total_tool_calls += call_count

        # Get optimization mode and duration
        opt_mode = _string(record.get("optimization_mode", "unknown"))
        duration = _float(record.get("turn_duration", 0.0))

        mode_stats[opt_mode]["turns"] += 1
        mode_stats[opt_mode]["total_duration"] += duration

        # Determine if parallel or sequential
        if call_count == 1:
            # Sequential call
            sequential_tool_calls += 1
            tool_sequential_counts[tool_names[0]] += 1
            mode_stats[opt_mode]["sequential_calls"] += 1

            # Check for missed parallelization opportunity
            if previous_tool_calls:
                # If previous turn had single call and current has single call,
                # and both are parallelizable tools, it's a missed opportunity
                if (len(previous_tool_calls) == 1 and
                    tool_names[0] in PARALLELIZABLE_TOOLS and
                    previous_tool_calls[0] in PARALLELIZABLE_TOOLS):
                    missed_opportunities += 1
        else:
            # Parallel calls
            parallel_tool_calls += call_count
            turns_with_parallel += 1
            parallel_batch_sizes.append(call_count)
            mode_stats[opt_mode]["parallel_calls"] += call_count
            mode_stats[opt_mode]["parallel_turns"] += 1

            # Track tool-specific parallelization
            for tool_name in tool_names:
                tool_parallel_counts[tool_name] += 1

            # Track pattern (sorted to normalize order)
            pattern = tuple(sorted(tool_names))
            parallel_patterns[pattern] += 1

        previous_tool_calls = tool_names

    # Calculate aggregate metrics
    parallel_usage_pct = _percentage(parallel_tool_calls, total_tool_calls)
    avg_batch_size = _average(parallel_batch_sizes)
    max_batch_size = max(parallel_batch_sizes) if parallel_batch_sizes else 0

    # Format parallelization by tool
    parallelization_by_tool = []
    all_tools = set(tool_parallel_counts.keys()) | set(tool_sequential_counts.keys())
    for tool in sorted(all_tools):
        parallel_count = tool_parallel_counts[tool]
        sequential_count = tool_sequential_counts[tool]
        total_count = parallel_count + sequential_count
        parallel_pct = _percentage(parallel_count, total_count)

        parallelization_by_tool.append({
            "tool": tool,
            "parallel_count": parallel_count,
            "sequential_count": sequential_count,
            "total_count": total_count,
            "parallel_percentage": parallel_pct,
        })

    # Format common patterns
    common_patterns = [
        {"tools": list(pattern), "count": count}
        for pattern, count in parallel_patterns.most_common(10)
    ]

    # Format optimization mode comparison
    mode_comparison = []
    for mode in sorted(mode_stats.keys()):
        stats = mode_stats[mode]
        mode_parallel_calls = stats["parallel_calls"]
        mode_sequential_calls = stats["sequential_calls"]
        mode_total_calls = mode_parallel_calls + mode_sequential_calls
        mode_parallel_pct = _percentage(mode_parallel_calls, mode_total_calls)
        mode_avg_duration = (
            stats["total_duration"] / stats["turns"] if stats["turns"] > 0 else 0.0
        )

        mode_comparison.append({
            "mode": mode,
            "turns": stats["turns"],
            "parallel_calls": mode_parallel_calls,
            "sequential_calls": mode_sequential_calls,
            "parallel_percentage": mode_parallel_pct,
            "parallel_turns": stats["parallel_turns"],
            "avg_turn_duration": round(mode_avg_duration, 2),
        })

    # Calculate efficiency score (0-100)
    # Higher parallel usage, larger batches, fewer missed opportunities = higher score
    efficiency_score = _calculate_efficiency_score(
        parallel_usage_pct,
        avg_batch_size,
        missed_opportunities,
        total_tool_calls,
    )
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY

    return {
        "total_turns": total_turns,
        "turns_with_tools": turns_with_tools,
        "total_tool_calls": total_tool_calls,
<<<<<<< HEAD
        "parallel_tool_calls": parallel_tool_calls,
        "sequential_tool_calls": sequential_tool_calls,
        "parallel_usage_percentage": parallel_usage_pct,
        "turns_with_parallel": turns_with_parallel,
        "avg_parallel_batch_size": avg_batch_size,
        "max_parallel_batch_size": max_batch_size,
        "missed_opportunities": missed_opportunities,
        "parallelization_by_tool": parallelization_by_tool,
        "common_parallel_patterns": common_patterns,
        "optimization_mode_comparison": mode_comparison,
        "parallel_efficiency_score": efficiency_score,
=======
        "parallel_turns": parallel_turns,
        "parallel_usage_rate": parallel_usage_rate,
        "total_parallel_batches": total_parallel_batches,
        "avg_parallel_batch_size": avg_batch_size,
        "max_parallel_batch_size": max_batch_size,
        "missed_opportunities": missed_opportunities,
        "tool_parallelization": dict(tool_parallelization.most_common(10)),
        "mode_comparison": mode_comparison,
        "examples": examples[:5],  # Limit to 5 examples
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "turns_with_tools": 0,
        "total_tool_calls": 0,
<<<<<<< HEAD
        "parallel_tool_calls": 0,
        "sequential_tool_calls": 0,
        "parallel_usage_percentage": 0.0,
        "turns_with_parallel": 0,
        "avg_parallel_batch_size": 0.0,
        "max_parallel_batch_size": 0,
        "missed_opportunities": 0,
        "parallelization_by_tool": [],
        "common_parallel_patterns": [],
        "optimization_mode_comparison": [],
        "parallel_efficiency_score": 0.0,
=======
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
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


<<<<<<< HEAD
def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


=======
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


<<<<<<< HEAD
def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_efficiency_score(
    parallel_pct: float,
    avg_batch_size: float,
    missed_opps: int,
    total_calls: int,
) -> float:
    """Calculate overall parallel efficiency score (0-100).

    Score components:
    - 50 points: Parallel usage percentage (higher is better)
    - 30 points: Average batch size (higher is better, capped at 5)
    - 20 points: Missed opportunities penalty (fewer is better)
    """
    # Parallel usage component (0-50)
    parallel_component = (parallel_pct / 100.0) * 50.0

    # Batch size component (0-30), optimal at 5+ tools per batch
    batch_component = min(avg_batch_size / 5.0, 1.0) * 30.0

    # Missed opportunities penalty (0-20)
    # Penalize based on ratio of missed opportunities to total calls
    if total_calls > 0:
        missed_ratio = missed_opps / total_calls
        # Each 10% of missed opportunities loses 2 points
        missed_penalty = min(missed_ratio * 20.0, 20.0)
    else:
        missed_penalty = 0.0

    opportunity_component = 20.0 - missed_penalty

    score = parallel_component + batch_component + opportunity_component
    return round(max(0.0, min(100.0, score)), 2)
=======
def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)
>>>>>>> relay/claude-code/add-pack-verification-command-diversity-analyzer-01KR3TTY
