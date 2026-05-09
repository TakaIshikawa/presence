"""Pack dependency chain depth analyzer.

Analyzes task dependency chains within execution packs. Tracks maximum dependency
chain depth, percentage of root tasks (no dependencies), average tasks per chain,
and circular dependency detection. Measures parallelization potential based on
the ratio of root tasks to total tasks.

Dependency chain metrics:
- Maximum chain depth: Longest dependency chain in pack
- Root tasks: Tasks with no dependencies (can start immediately)
- Average chain length: Mean number of tasks per dependency chain
- Circular dependencies: Detection of dependency cycles
- Parallelization potential: Ratio of root tasks to total tasks

Quality indicators:
- High parallelization potential (>70%): Most tasks can run in parallel
- Low maximum depth (<5): Shallow dependency chains
- High root task percentage (>50%): Many independent tasks
- No circular dependencies: Clean dependency graph
- Balanced chains: Even distribution of dependencies
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_dependency_chain_depth(records: object) -> dict[str, Any]:
    """Analyze task dependency chains within execution packs.

    Evaluates dependency chain depth, root tasks, and parallelization potential.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - dependencies: List of task IDs this task depends on
            - chain_depth: Depth of this task in dependency chain
            - is_root_task: Boolean indicating no dependencies
            - has_circular_dependency: Boolean indicating cycle detected

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - root_tasks: Count of tasks with no dependencies
            - root_task_percentage: Percentage of root tasks
            - max_dependency_chain_depth: Longest dependency chain
            - avg_chain_depth: Average depth across all tasks
            - tasks_with_dependencies: Count of non-root tasks
            - circular_dependencies_detected: Count of circular deps
            - parallelization_potential_score: Root tasks / total tasks (%)
            - tasks_at_depth_0: Count of root tasks
            - tasks_at_depth_1: Count of tasks 1 level deep
            - tasks_at_depth_2_plus: Count of tasks 2+ levels deep

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    if not records:
        return _empty_result()

    total_tasks = 0
    root_tasks = 0
    tasks_with_deps = 0
    chain_depths: list[int | float] = []
    max_depth = 0
    circular_deps = 0

    # Track distribution of tasks by depth
    depth_0_count = 0
    depth_1_count = 0
    depth_2plus_count = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        dependencies = record.get("dependencies")
        chain_depth = _extract_number(record.get("chain_depth"))
        is_root = record.get("is_root_task")
        has_circular = record.get("has_circular_dependency")

        # Track root tasks
        if is_root is True:
            root_tasks += 1
            depth_0_count += 1
        elif isinstance(dependencies, list) and len(dependencies) == 0:
            # Also count as root if dependencies list is empty
            root_tasks += 1
            depth_0_count += 1
        else:
            tasks_with_deps += 1

        # Track chain depth
        if chain_depth is not None:
            depth_value = int(chain_depth)
            chain_depths.append(depth_value)
            max_depth = max(max_depth, depth_value)

            # Categorize by depth
            if depth_value == 0:
                if not is_root:
                    depth_0_count += 1
            elif depth_value == 1:
                depth_1_count += 1
            elif depth_value >= 2:
                depth_2plus_count += 1

        # Track circular dependencies
        if has_circular is True:
            circular_deps += 1

    # Calculate aggregate metrics
    root_percentage = _percentage(root_tasks, total_tasks)
    avg_depth = _average(chain_depths)

    # Parallelization potential score (target >=70%)
    parallelization_score = _percentage(root_tasks, total_tasks)

    return {
        "total_tasks": total_tasks,
        "root_tasks": root_tasks,
        "root_task_percentage": root_percentage,
        "max_dependency_chain_depth": max_depth,
        "avg_chain_depth": avg_depth,
        "tasks_with_dependencies": tasks_with_deps,
        "circular_dependencies_detected": circular_deps,
        "parallelization_potential_score": parallelization_score,
        "tasks_at_depth_0": depth_0_count,
        "tasks_at_depth_1": depth_1_count,
        "tasks_at_depth_2_plus": depth_2plus_count,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tasks": 0,
        "root_tasks": 0,
        "root_task_percentage": 0.0,
        "max_dependency_chain_depth": 0,
        "avg_chain_depth": 0.0,
        "tasks_with_dependencies": 0,
        "circular_dependencies_detected": 0,
        "parallelization_potential_score": 0.0,
        "tasks_at_depth_0": 0,
        "tasks_at_depth_1": 0,
        "tasks_at_depth_2_plus": 0,
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
