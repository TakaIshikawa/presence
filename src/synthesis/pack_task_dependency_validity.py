"""Pack task dependency validity analyzer.

Analyzes dependsOn relationships in execution packs to validate task dependency
correctness. Checks for valid references, circular dependencies, dependency chain
depth, and independence ratios for optimal parallelization.

Task dependency metrics:
- Valid dependency references: All dependsOn tasks exist in pack
- Circular dependency detection: Tasks depending on each other cyclically
- Dependency chain depth: Maximum depth of dependency chains
- Independence ratio: Percentage of tasks with no dependencies
- Dependency order vs execution order: Alignment with actual execution
- Deep dependency chains: Chains deeper than recommended threshold

Quality indicators:
- No circular dependencies: All dependency graphs are acyclic
- Valid references (100%): All dependsOn tasks exist
- High independence ratio (>70%): Most tasks can run in parallel
- Shallow dependency chains (<3 levels): Simple dependency structure
- Aligned execution order (>90%): Dependencies match execution sequence
- Low deep chains (<10%): Few tasks with complex dependencies
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_task_dependency_validity(records: object) -> dict[str, Any]:
    """Analyze task dependency validity and structure in execution packs.

    Validates dependsOn relationships and calculates dependency metrics.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_tasks: Total tasks in pack
            - tasks_with_dependencies: Tasks with dependsOn specified
            - tasks_independent: Tasks with no dependencies
            - valid_dependency_references: References to existing tasks
            - invalid_dependency_references: References to missing tasks
            - circular_dependencies_detected: Number of circular deps found
            - max_dependency_chain_depth: Deepest dependency chain
            - avg_dependency_chain_depth: Average chain depth
            - deep_dependency_chains: Chains deeper than 3 levels
            - execution_order_aligned: Dependencies match execution order
            - execution_order_misaligned: Dependencies don't match execution
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_tasks: Average tasks per pack
            - avg_independence_ratio: Average % tasks without dependencies
            - avg_valid_reference_rate: Average % valid dependency references
            - avg_circular_dependency_rate: Average % packs with circular deps
            - avg_max_chain_depth: Average maximum chain depth
            - avg_deep_chain_ratio: Average % tasks with deep chains
            - avg_execution_alignment_rate: Average % aligned dependencies
            - packs_with_circular_deps: Count with circular dependencies
            - packs_with_invalid_refs: Count with invalid references
            - high_independence_packs: Count with >70% independent tasks
            - low_independence_packs: Count with <30% independent tasks
            - deep_chain_packs: Count with chains >3 levels

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tasks_list: list[int | float] = []
    independence_ratios: list[float] = []
    valid_reference_rates: list[float] = []
    circular_dependency_rates: list[float] = []
    max_chain_depths: list[int | float] = []
    deep_chain_ratios: list[float] = []
    execution_alignment_rates: list[float] = []

    packs_with_circular_deps = 0
    packs_with_invalid_refs = 0
    high_independence_packs = 0  # >70% independent
    low_independence_packs = 0   # <30% independent
    deep_chain_packs = 0  # Max depth >3

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_tasks = _extract_number(record.get("total_tasks"))
        tasks_with_deps = _extract_number(record.get("tasks_with_dependencies"))
        tasks_independent = _extract_number(record.get("tasks_independent"))
        valid_refs = _extract_number(record.get("valid_dependency_references"))
        invalid_refs = _extract_number(record.get("invalid_dependency_references"))
        circular_deps = _extract_number(record.get("circular_dependencies_detected"))
        max_depth = _extract_number(record.get("max_dependency_chain_depth"))
        avg_depth = _extract_number(record.get("avg_dependency_chain_depth"))
        deep_chains = _extract_number(record.get("deep_dependency_chains"))
        aligned = _extract_number(record.get("execution_order_aligned"))
        misaligned = _extract_number(record.get("execution_order_misaligned"))

        # Track total tasks
        if total_tasks is not None:
            total_tasks_list.append(total_tasks)

            # Calculate independence ratio
            if tasks_independent is not None:
                independence = _percentage(tasks_independent, total_tasks)
                independence_ratios.append(independence)

                if independence > 70.0:
                    high_independence_packs += 1
                elif independence < 30.0:
                    low_independence_packs += 1

            # Calculate deep chain ratio
            if deep_chains is not None:
                deep_chain_ratios.append(_percentage(deep_chains, total_tasks))

        # Calculate valid reference rate
        if valid_refs is not None and invalid_refs is not None:
            total_refs = valid_refs + invalid_refs
            if total_refs > 0:
                valid_reference_rates.append(_percentage(valid_refs, total_refs))

            if invalid_refs > 0:
                packs_with_invalid_refs += 1

        # Track circular dependencies
        if circular_deps is not None and circular_deps > 0:
            packs_with_circular_deps += 1
            circular_dependency_rates.append(100.0)
        else:
            circular_dependency_rates.append(0.0)

        # Track max chain depth
        if max_depth is not None:
            max_chain_depths.append(max_depth)

            if max_depth > 3:
                deep_chain_packs += 1

        # Calculate execution alignment rate
        if aligned is not None and misaligned is not None:
            total_alignment_checks = aligned + misaligned
            if total_alignment_checks > 0:
                execution_alignment_rates.append(_percentage(aligned, total_alignment_checks))

    # Calculate aggregate metrics
    avg_tasks = _average(total_tasks_list)
    avg_independence = _average(independence_ratios)
    avg_valid_refs = _average(valid_reference_rates)
    avg_circular = _average(circular_dependency_rates)
    avg_max_depth = _average(max_chain_depths)
    avg_deep_chains = _average(deep_chain_ratios)
    avg_alignment = _average(execution_alignment_rates)

    return {
        "total_packs": total_packs,
        "avg_total_tasks": avg_tasks,
        "avg_independence_ratio": avg_independence,
        "avg_valid_reference_rate": avg_valid_refs,
        "avg_circular_dependency_rate": avg_circular,
        "avg_max_chain_depth": avg_max_depth,
        "avg_deep_chain_ratio": avg_deep_chains,
        "avg_execution_alignment_rate": avg_alignment,
        "packs_with_circular_deps": packs_with_circular_deps,
        "packs_with_invalid_refs": packs_with_invalid_refs,
        "high_independence_packs": high_independence_packs,
        "low_independence_packs": low_independence_packs,
        "deep_chain_packs": deep_chain_packs,
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
