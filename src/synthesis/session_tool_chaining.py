"""Session tool chaining depth analyzer for tool usage patterns.

Analyzes tool call chaining patterns in sessions to identify sequential and
parallel tool usage strategies. Measures chain depth, identifies common patterns,
and calculates chain efficiency.

Tool chaining metrics:
- Max chain depth: Longest sequential chain of tool calls
- Average chain depth: Mean chain length across session
- Common chain patterns: Frequent sequences (e.g., Grep->Read->Edit)
- Parallel vs sequential ratio: Balance of parallel and sequential calls
- Chain efficiency score: Successful chains vs broken chains

Chain patterns:
- Linear chains: Sequential tool calls building on previous results
- Parallel calls: Multiple tools invoked simultaneously
- Mixed patterns: Combination of parallel and sequential
- Failed chains: Chains broken by errors or context loss
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


def analyze_session_tool_chaining(records: object) -> dict[str, Any]:
    """Analyze tool call chaining patterns in a session.

    Evaluates how tools are chained together, measuring depth, identifying
    common patterns, and calculating efficiency of chaining strategies.

    Args:
        records: List of session turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_calls: List of tool calls in the turn
            - Each tool call has: tool_name, success (optional)
            - parallel: Optional boolean indicating parallel tool usage

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - total_tool_calls: Total number of tool calls
            - max_chain_depth: Longest sequential chain
            - avg_chain_depth: Average chain length
            - common_chain_patterns: List of frequent tool sequences
            - parallel_vs_sequential_ratio: Ratio of parallel to sequential
            - chain_efficiency_score: Success rate of tool chains (0.0-1.0)
            - parallel_call_count: Number of parallel tool invocations
            - sequential_call_count: Number of sequential tool calls

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session turn dictionaries")

    total_turns = 0
    total_tool_calls = 0

    # Track chains
    chain_depths: list[int] = []
    current_chain: list[str] = []
    chain_patterns: Counter[tuple[str, ...]] = Counter()

    parallel_call_count = 0
    sequential_call_count = 0

    # Track chain success
    successful_chains = 0
    broken_chains = 0
    chain_has_failure = False

    prev_tools: list[str] = []

    for turn in records:
        if not isinstance(turn, Mapping):
            continue

        total_turns += 1
        tool_calls = turn.get("tool_calls", [])

        if not isinstance(tool_calls, list):
            continue

        # Check if this turn has parallel tool calls
        is_parallel = turn.get("parallel", False) or len(tool_calls) > 1

        if is_parallel:
            parallel_call_count += len(tool_calls)
        else:
            sequential_call_count += len(tool_calls)

        # Extract tool names from this turn
        current_tools: list[str] = []
        turn_has_failure = False

        for tool_call in tool_calls:
            if isinstance(tool_call, Mapping):
                tool_name = tool_call.get("tool_name", "")
                if tool_name:
                    current_tools.append(str(tool_name))
                    total_tool_calls += 1

                    # Check success
                    success = tool_call.get("success")
                    if success is False:
                        turn_has_failure = True
                        chain_has_failure = True
            elif isinstance(tool_call, str):
                current_tools.append(tool_call)
                total_tool_calls += 1

        # Build chains
        if prev_tools and current_tools:
            # This is a continuation of a chain
            if is_parallel:
                # Parallel calls don't extend chain depth linearly
                # Record current chain and start fresh
                if current_chain:
                    chain_depths.append(len(current_chain))
                    if len(current_chain) >= 2:
                        pattern = tuple(current_chain[-min(len(current_chain), 5):])
                        chain_patterns[pattern] += 1
                    if chain_has_failure:
                        broken_chains += 1
                    else:
                        successful_chains += 1
                current_chain = current_tools[:]
                chain_has_failure = turn_has_failure
            else:
                # Sequential call extends the chain
                current_chain.extend(current_tools)
        elif current_tools:
            # Start of new chain
            current_chain = current_tools[:]
            chain_has_failure = turn_has_failure
        else:
            # No tools in this turn, chain ends
            if current_chain:
                chain_depths.append(len(current_chain))
                if len(current_chain) >= 2:
                    pattern = tuple(current_chain[-min(len(current_chain), 5):])
                    chain_patterns[pattern] += 1
                if chain_has_failure:
                    broken_chains += 1
                else:
                    successful_chains += 1
                current_chain = []
                chain_has_failure = False

        prev_tools = current_tools

    # Close any remaining chain
    if current_chain:
        chain_depths.append(len(current_chain))
        if len(current_chain) >= 2:
            pattern = tuple(current_chain[-min(len(current_chain), 5):])
            chain_patterns[pattern] += 1
        if chain_has_failure:
            broken_chains += 1
        else:
            successful_chains += 1

    # Calculate metrics
    max_depth = max(chain_depths) if chain_depths else 0
    avg_depth = _average(chain_depths)

    # Parallel vs sequential ratio
    total_categorized = parallel_call_count + sequential_call_count
    parallel_ratio = _percentage(parallel_call_count, total_categorized)

    # Chain efficiency
    total_chains = successful_chains + broken_chains
    efficiency_score = _ratio(successful_chains, total_chains)

    # Format common patterns
    common_patterns = [
        {"pattern": list(pattern), "count": count}
        for pattern, count in chain_patterns.most_common(10)
    ]

    return {
        "total_turns": total_turns,
        "total_tool_calls": total_tool_calls,
        "max_chain_depth": max_depth,
        "avg_chain_depth": avg_depth,
        "common_chain_patterns": common_patterns,
        "parallel_vs_sequential_ratio": parallel_ratio,
        "chain_efficiency_score": efficiency_score,
        "parallel_call_count": parallel_call_count,
        "sequential_call_count": sequential_call_count,
    }


def _average(values: list[int]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _ratio(numerator: int, denominator: int) -> float:
    """Calculate ratio from 0.0 to 1.0, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 3)
