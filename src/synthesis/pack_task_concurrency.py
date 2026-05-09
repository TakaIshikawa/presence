"""Pack Task agent parallel execution efficiency analyzer.

Analyzes Task agent invocation patterns across all sessions in an execution pack
to measure parallel execution efficiency, identify sequential bottlenecks, and detect
missed parallelization opportunities. Evaluates how effectively agents leverage
concurrent Task execution to minimize wall-clock time.

Task concurrency metrics:
- Concurrent Task invocations: Count of parallel Task calls in single message
- Sequential Task chains: Count of dependent Task sequences
- Time savings from parallelization: Estimated speedup vs sequential execution
- Ideal vs actual parallelization ratio: Potential vs realized concurrency
- Missed parallelization opportunities: Independent tasks executed sequentially

Quality indicators:
- High concurrent invocations: >50% of multi-Task workflows use parallelization
- Low sequential chains: Minimal unnecessary sequential execution
- High time savings: >40% reduction vs sequential baseline
- High parallelization ratio: >0.7 (70% of potential concurrency realized)
- Low missed opportunities: <20% of independent tasks executed sequentially
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_task_concurrency(records: object) -> dict[str, Any]:
    """Analyze Task agent parallel execution efficiency across pack sessions.

    Evaluates concurrent vs sequential Task invocation patterns, measures time savings,
    and identifies opportunities for better parallelization.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_task_invocations: Total Task tool calls
            - concurrent_task_calls: Task calls in parallel (same message)
            - sequential_task_chains: Count of sequential Task sequences
            - parallel_execution_time_seconds: Wall-clock time for parallel Tasks
            - sequential_equivalent_time_seconds: Estimated sequential execution time
            - independent_tasks_count: Tasks that could run in parallel
            - actually_parallelized_tasks: Tasks that were actually parallelized
            - multi_task_workflows: Number of workflows with 2+ Tasks

    Returns:
        Dict with:
            - total_sessions: Total number of sessions analyzed
            - total_task_invocations: Sum of Task calls across all sessions
            - concurrent_task_calls: Total parallel Task invocations
            - sequential_task_chains: Total sequential Task sequences
            - concurrent_ratio: Percentage of Tasks executed concurrently
            - parallel_execution_time_seconds: Total parallel execution time
            - sequential_equivalent_time_seconds: Estimated sequential time
            - time_savings_seconds: Time saved through parallelization
            - time_savings_percentage: Percentage time saved
            - independent_tasks_count: Total tasks that could be parallel
            - actually_parallelized_tasks: Tasks actually parallelized
            - parallelization_ratio: Actual / ideal parallelization (0-1)
            - missed_opportunities: Independent tasks not parallelized
            - missed_opportunity_ratio: Percentage of missed opportunities
            - sessions_with_parallelization: Sessions using concurrent Tasks
            - parallelization_adoption_rate: Percentage of sessions using it
            - concurrency_efficiency_score: 0-1 overall efficiency score

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    total_task_invocations = 0
    concurrent_task_calls = 0
    sequential_task_chains = 0

    parallel_execution_time = 0.0
    sequential_equivalent_time = 0.0

    independent_tasks_count = 0
    actually_parallelized_tasks = 0

    sessions_with_parallelization = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        # Count Task invocations
        task_invocations = _int(record.get("total_task_invocations", 0))
        total_task_invocations += task_invocations

        # Count concurrent and sequential patterns
        concurrent = _int(record.get("concurrent_task_calls", 0))
        sequential = _int(record.get("sequential_task_chains", 0))
        concurrent_task_calls += concurrent
        sequential_task_chains += sequential

        # Track execution time
        parallel_time = _float(record.get("parallel_execution_time_seconds", 0))
        sequential_time = _float(record.get("sequential_equivalent_time_seconds", 0))
        parallel_execution_time += parallel_time
        sequential_equivalent_time += sequential_time

        # Track parallelization potential
        independent = _int(record.get("independent_tasks_count", 0))
        parallelized = _int(record.get("actually_parallelized_tasks", 0))
        independent_tasks_count += independent
        actually_parallelized_tasks += parallelized

        # Check if session uses parallelization
        if concurrent > 0:
            sessions_with_parallelization += 1

    # Calculate aggregate metrics
    concurrent_ratio = _percentage(concurrent_task_calls, total_task_invocations)

    # Calculate time savings
    time_savings = sequential_equivalent_time - parallel_execution_time
    time_savings_percentage = _percentage(time_savings, sequential_equivalent_time)

    # Calculate parallelization ratio
    parallelization_ratio = (
        actually_parallelized_tasks / independent_tasks_count
        if independent_tasks_count > 0
        else 0.0
    )
    parallelization_ratio = round(min(1.0, parallelization_ratio), 3)

    # Calculate missed opportunities
    missed_opportunities = max(0, independent_tasks_count - actually_parallelized_tasks)
    missed_opportunity_ratio = _percentage(missed_opportunities, independent_tasks_count)

    # Calculate adoption rate
    parallelization_adoption_rate = _percentage(
        sessions_with_parallelization, total_sessions
    )

    # Calculate efficiency score
    efficiency_score = _calculate_efficiency_score(
        concurrent_ratio,
        time_savings_percentage,
        parallelization_ratio,
        missed_opportunity_ratio,
    )

    return {
        "total_sessions": total_sessions,
        "total_task_invocations": total_task_invocations,
        "concurrent_task_calls": concurrent_task_calls,
        "sequential_task_chains": sequential_task_chains,
        "concurrent_ratio": concurrent_ratio,
        "parallel_execution_time_seconds": round(parallel_execution_time, 2),
        "sequential_equivalent_time_seconds": round(sequential_equivalent_time, 2),
        "time_savings_seconds": round(time_savings, 2),
        "time_savings_percentage": time_savings_percentage,
        "independent_tasks_count": independent_tasks_count,
        "actually_parallelized_tasks": actually_parallelized_tasks,
        "parallelization_ratio": parallelization_ratio,
        "missed_opportunities": missed_opportunities,
        "missed_opportunity_ratio": missed_opportunity_ratio,
        "sessions_with_parallelization": sessions_with_parallelization,
        "parallelization_adoption_rate": parallelization_adoption_rate,
        "concurrency_efficiency_score": efficiency_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "total_task_invocations": 0,
        "concurrent_task_calls": 0,
        "sequential_task_chains": 0,
        "concurrent_ratio": 0.0,
        "parallel_execution_time_seconds": 0.0,
        "sequential_equivalent_time_seconds": 0.0,
        "time_savings_seconds": 0.0,
        "time_savings_percentage": 0.0,
        "independent_tasks_count": 0,
        "actually_parallelized_tasks": 0,
        "parallelization_ratio": 0.0,
        "missed_opportunities": 0,
        "missed_opportunity_ratio": 0.0,
        "sessions_with_parallelization": 0,
        "parallelization_adoption_rate": 0.0,
        "concurrency_efficiency_score": 0.0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(float(value))
        except ValueError:
            return 0
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int] | list[float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_efficiency_score(
    concurrent_ratio: float,
    time_savings_percentage: float,
    parallelization_ratio: float,
    missed_opportunity_ratio: float,
) -> float:
    """Calculate overall concurrency efficiency score (0-1).

    Score components:
    - 0.25: Concurrent execution ratio (higher is better)
    - 0.35: Time savings percentage (higher is better)
    - 0.25: Parallelization ratio (closer to 1.0 is better)
    - 0.15: Missed opportunities penalty (lower is better)
    """
    # Concurrent ratio component (0-0.25)
    # Target: >50% concurrent
    if concurrent_ratio >= 50.0:
        concurrent_component = 0.25
    else:
        concurrent_component = (concurrent_ratio / 50.0) * 0.25

    # Time savings component (0-0.35)
    # Target: >40% time savings
    if time_savings_percentage >= 40.0:
        time_savings_component = 0.35
    else:
        time_savings_component = (time_savings_percentage / 40.0) * 0.35

    # Parallelization ratio component (0-0.25)
    # Ratio is already 0-1, just scale
    parallelization_component = parallelization_ratio * 0.25

    # Missed opportunities penalty (0-0.15)
    # Lower missed ratio is better
    missed_penalty = (missed_opportunity_ratio / 100.0) * 0.15
    missed_component = 0.15 - missed_penalty

    score = (
        concurrent_component +
        time_savings_component +
        parallelization_component +
        missed_component
    )
    return round(max(0.0, min(1.0, score)), 3)
