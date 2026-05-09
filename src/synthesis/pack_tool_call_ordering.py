"""Pack tool call ordering and dependency awareness analyzer.

Analyzes execution pack transcripts for tool call sequencing discipline, measuring
parallel tool call opportunities, sequential dependency correctness, premature calls
with missing parameters, blocking sequential chains, and batching efficiency.

Tool ordering dimensions:
1. Parallel tool call opportunities:
   - Independent reads/greps in single message
   - Maximizes throughput

2. Sequential dependency correctness:
   - Edit→Read verification sequences
   - Write→Bash git operation chains
   - Proper ordering for dependent operations

3. Premature tool calls:
   - Calls with missing/placeholder parameters
   - Guessing instead of waiting for dependencies

4. Blocking sequential chains:
   - Sequential calls that could be parallel
   - Unnecessary serialization

5. Tool call batching efficiency:
   - Multiple tools in single message
   - Efficiency across pack tasks

Quality indicators:
- High parallelism rate (>60%)
- No dependency violations
- No premature calls with placeholders
- Low blocking chain rate (<20%)
- High batching efficiency (>75%)
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_tool_call_ordering(records: object) -> dict[str, Any]:
    """Analyze tool call ordering discipline across pack transcripts.

    Args:
        records: List of session dictionaries with keys:
            - parallel_opportunities_count: Independent calls in single message
            - sequential_dependency_correct: Properly ordered dependent calls
            - sequential_dependency_violations: Incorrectly ordered dependent calls
            - premature_calls_count: Calls with missing/placeholder parameters
            - blocking_sequential_chains: Serialized calls that could be parallel
            - tool_call_batches: Total batched tool call groups
            - total_tool_calls: Total tool calls in pack

    Returns:
        Dict with metrics about tool call ordering efficiency
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    parallel_opportunities = 0
    sequential_correct = 0
    sequential_violations = 0
    premature_calls = 0
    blocking_chains = 0
    tool_call_batches = 0
    total_tool_calls = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        parallel_opportunities += _int(record.get("parallel_opportunities_count", 0))
        sequential_correct += _int(record.get("sequential_dependency_correct", 0))
        sequential_violations += _int(
            record.get("sequential_dependency_violations", 0)
        )
        premature_calls += _int(record.get("premature_calls_count", 0))
        blocking_chains += _int(record.get("blocking_sequential_chains", 0))
        tool_call_batches += _int(record.get("tool_call_batches", 0))
        total_tool_calls += _int(record.get("total_tool_calls", 0))

    # Calculate rates
    total_dependencies = sequential_correct + sequential_violations
    dependency_correctness_rate = _percentage(sequential_correct, total_dependencies)

    parallelism_rate = _percentage(parallel_opportunities, total_tool_calls)
    blocking_rate = _percentage(blocking_chains, total_tool_calls)
    batching_efficiency = _percentage(parallel_opportunities, tool_call_batches)

    # Ordering score
    ordering_score = _calculate_ordering_score(
        parallelism_rate,
        dependency_correctness_rate,
        premature_calls,
        blocking_rate,
        batching_efficiency,
    )

    return {
        "total_sessions": total_sessions,
        "parallel_opportunities_count": parallel_opportunities,
        "sequential_dependency_correct": sequential_correct,
        "sequential_dependency_violations": sequential_violations,
        "dependency_correctness_rate": dependency_correctness_rate,
        "premature_calls_count": premature_calls,
        "blocking_sequential_chains": blocking_chains,
        "tool_call_batches": tool_call_batches,
        "total_tool_calls": total_tool_calls,
        "parallelism_rate": parallelism_rate,
        "blocking_rate": blocking_rate,
        "batching_efficiency": batching_efficiency,
        "ordering_score": ordering_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "parallel_opportunities_count": 0,
        "sequential_dependency_correct": 0,
        "sequential_dependency_violations": 0,
        "dependency_correctness_rate": 0.0,
        "premature_calls_count": 0,
        "blocking_sequential_chains": 0,
        "tool_call_batches": 0,
        "total_tool_calls": 0,
        "parallelism_rate": 0.0,
        "blocking_rate": 0.0,
        "batching_efficiency": 0.0,
        "ordering_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_ordering_score(
    parallelism_rate: float,
    dependency_correctness_rate: float,
    premature_calls: int,
    blocking_rate: float,
    batching_efficiency: float,
) -> float:
    """Calculate overall tool call ordering score (0-1).

    Scoring components:
    - Parallelism (0-0.30)
    - Dependency correctness (0-0.30)
    - No premature calls (0-0.15)
    - Low blocking (0-0.15)
    - Batching efficiency (0-0.10)
    """
    parallelism_component = (parallelism_rate / 100.0) * 0.30
    dependency_component = (dependency_correctness_rate / 100.0) * 0.30

    premature_penalty = min(premature_calls * 0.03, 0.15)
    premature_component = max(0.0, 0.15 - premature_penalty)

    blocking_component = max(0.0, 0.15 - (blocking_rate / 100.0) * 0.15)
    batching_component = (batching_efficiency / 100.0) * 0.10

    score = (
        parallelism_component
        + dependency_component
        + premature_component
        + blocking_component
        + batching_component
    )
    return round(max(0.0, min(1.0, score)), 3)
