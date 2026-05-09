"""Session tool call batching efficiency analyzer.

Analyzes how efficiently sessions batch independent tool calls together for
parallel execution versus sequential execution. Tracks batch size distribution,
identifies missed opportunities for parallelism, and measures batching improvement
over time.

Tool call batching metrics:
- Batch size distribution: Frequency of single vs multi-call turns
- Batching efficiency score: Ratio of batched calls to total batchable calls
- Sequential independent calls: Opportunities for parallelism
- Batching improvement: Change in batching rate over session lifetime
- Average batch size: Mean number of tools called per batched turn

Quality indicators:
- High batching efficiency (>80%): Most batchable calls are batched
- Large average batch size (>3): Effective parallel execution
- Low missed opportunities (<10%): Few sequential independent calls
- Positive improvement trend: Batching rate increases over time
- Balanced distribution: Mix of single and batched calls when appropriate
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_tool_call_batching(records: object) -> dict[str, Any]:
    """Analyze tool call batching efficiency in agent sessions.

    Evaluates how effectively sessions batch independent tool calls for parallel
    execution and identifies missed opportunities for parallelization.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_calls_count: Number of tool calls in this turn
            - tools_called: List of tool names called in this turn
            - batchable_calls: Number of calls that could be batched
            - batched_calls: Number of calls actually batched
            - is_sequential_independent: Boolean indicating missed batching opportunity

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - turns_with_tool_calls: Turns containing any tool calls
            - single_call_turns: Turns with exactly 1 tool call
            - batched_turns: Turns with 2+ tool calls (parallel execution)
            - batching_rate: Percentage of tool turns using batching
            - total_tool_calls: Total number of tool calls made
            - total_batchable_calls: Total calls that could be batched
            - total_batched_calls: Total calls actually batched
            - batching_efficiency_score: Ratio of batched to batchable calls (%)
            - avg_batch_size: Average number of tools in batched turns
            - max_batch_size: Largest batch observed
            - missed_batching_opportunities: Count of sequential independent calls
            - batching_improvement_score: Change in batching rate (early vs late session)

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
    single_call_turns = 0
    batched_turns = 0
    total_tool_calls = 0
    total_batchable_calls = 0
    total_batched_calls = 0
    batch_sizes: list[int | float] = []
    missed_opportunities = 0

    # Track batching rate over time for improvement score
    early_batched = 0
    early_tool_turns = 0
    late_batched = 0
    late_tool_turns = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1

        tool_count = _extract_number(record.get("tool_calls_count"))
        batchable = _extract_number(record.get("batchable_calls"))
        batched = _extract_number(record.get("batched_calls"))
        is_sequential_independent = record.get("is_sequential_independent")

        # Skip turns with no tool calls
        if tool_count is None or tool_count <= 0:
            continue

        turns_with_tools += 1
        total_tool_calls += int(tool_count)

        # Track batchable and batched calls
        if batchable is not None and batchable > 0:
            total_batchable_calls += int(batchable)

        if batched is not None and batched > 0:
            total_batched_calls += int(batched)

        # Classify turn by batch size
        if tool_count == 1:
            single_call_turns += 1
        else:
            batched_turns += 1
            batch_sizes.append(int(tool_count))

        # Track missed opportunities
        if is_sequential_independent is True:
            missed_opportunities += 1

        # Track early vs late session batching for improvement score
        # Split at midpoint of tool-using turns
        # We'll calculate the split point after the loop
        # For now, just store turn info

    # Calculate early vs late batching rate
    if turns_with_tools > 0:
        midpoint = turns_with_tools // 2
        tool_turn_idx = 0

        for record in records:
            if not isinstance(record, Mapping):
                continue

            tool_count = _extract_number(record.get("tool_calls_count"))
            if tool_count is None or tool_count <= 0:
                continue

            is_batched = tool_count > 1

            if tool_turn_idx < midpoint:
                # Early session
                early_tool_turns += 1
                if is_batched:
                    early_batched += 1
            else:
                # Late session
                late_tool_turns += 1
                if is_batched:
                    late_batched += 1

            tool_turn_idx += 1

    # Calculate aggregate metrics
    batching_rate = _percentage(batched_turns, turns_with_tools)
    batching_efficiency = _percentage(total_batched_calls, total_batchable_calls)
    avg_batch_size = _average(batch_sizes)
    max_batch_size = max(batch_sizes) if batch_sizes else 0

    # Calculate improvement score
    early_rate = _percentage(early_batched, early_tool_turns)
    late_rate = _percentage(late_batched, late_tool_turns)
    improvement = late_rate - early_rate

    return {
        "total_turns": total_turns,
        "turns_with_tool_calls": turns_with_tools,
        "single_call_turns": single_call_turns,
        "batched_turns": batched_turns,
        "batching_rate": batching_rate,
        "total_tool_calls": total_tool_calls,
        "total_batchable_calls": total_batchable_calls,
        "total_batched_calls": total_batched_calls,
        "batching_efficiency_score": batching_efficiency,
        "avg_batch_size": avg_batch_size,
        "max_batch_size": max_batch_size,
        "missed_batching_opportunities": missed_opportunities,
        "batching_improvement_score": round(improvement, 2),
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "turns_with_tool_calls": 0,
        "single_call_turns": 0,
        "batched_turns": 0,
        "batching_rate": 0.0,
        "total_tool_calls": 0,
        "total_batchable_calls": 0,
        "total_batched_calls": 0,
        "batching_efficiency_score": 0.0,
        "avg_batch_size": 0.0,
        "max_batch_size": 0,
        "missed_batching_opportunities": 0,
        "batching_improvement_score": 0.0,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
