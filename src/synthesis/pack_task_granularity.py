"""Pack task granularity analyzer for optimal task sizing assessment.

Analyzes task granularity within execution packs to identify optimal task
sizing. Evaluates task scope distribution, dependency chain depths, file
overlap patterns, and the balance between independent and dependent tasks.

Granularity metrics:
- Scope distribution: How estimated scope values are distributed
- Dependency depth: Maximum depth of task dependency chains
- File overlap: Tasks with overlapping expectedFiles within packs
- Independence ratio: Balance of independent vs dependent tasks

Sizing patterns:
- Over-granular: Too many small tasks, excessive coordination overhead
- Under-granular: Too few large tasks, limited parallelization opportunity
- Well-balanced: Appropriate task sizes with good parallelization potential
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Iterable, Mapping


def analyze_pack_task_granularity(records: object) -> dict[str, Any]:
    """Analyze task granularity and sizing within execution packs.

    Evaluates task scope distribution, dependency patterns, and file
    overlap to assess whether tasks are appropriately sized.

    Args:
        records: List of task dictionaries with keys:
            - pack_id: Execution pack identifier
            - task_id: Task identifier
            - estimated_scope: Scope estimate (tiny, small, medium, large)
            - expected_files: List of files task expects to modify
            - depends_on: List of task IDs this task depends on
            - changed_files_count: Optional count of actually changed files

    Returns:
        Dict with:
            - total_packs: Number of unique execution packs analyzed
            - total_tasks: Total number of tasks analyzed
            - scope_distribution: Counter of scope estimates
            - average_tasks_per_pack: Mean number of tasks per pack
            - max_dependency_depth: Maximum depth of dependency chains
            - average_dependency_depth: Mean depth of dependency chains
            - file_overlap_count: Number of file overlaps within packs
            - independent_task_count: Tasks with no dependencies
            - dependent_task_count: Tasks with dependencies
            - independence_ratio: Percentage of independent tasks
            - granularity_rating: Classification of overall granularity

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    # Group tasks by pack
    packs: dict[str, list[dict[str, Any]]] = defaultdict(list)
    scope_counter: Counter[str] = Counter()
    independent_count = 0
    dependent_count = 0
    dependency_depths: list[int | float] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        task_id = _string(record.get("task_id")) or f"task_{index}"
        scope = _string(record.get("estimated_scope")).lower() or "unknown"
        expected_files = _normalize_files(record.get("expected_files"))
        depends_on = _normalize_list(record.get("depends_on"))

        # Track scope distribution
        scope_counter[scope] += 1

        # Track independence
        if not depends_on:
            independent_count += 1
        else:
            dependent_count += 1

        # Store task info
        packs[pack_id].append({
            "task_id": task_id,
            "scope": scope,
            "expected_files": expected_files,
            "depends_on": depends_on,
        })

    # Analyze each pack
    file_overlap_count = 0
    for pack_id, tasks in packs.items():
        # Calculate dependency depths for this pack
        task_map = {task["task_id"]: task for task in tasks}
        for task in tasks:
            depth = _calculate_dependency_depth(task["task_id"], task_map)
            dependency_depths.append(depth)

        # Check for file overlaps
        file_to_tasks: dict[str, list[str]] = defaultdict(list)
        for task in tasks:
            for file_path in task["expected_files"]:
                file_to_tasks[file_path].append(task["task_id"])

        # Count overlapping files
        for file_path, task_ids in file_to_tasks.items():
            if len(task_ids) > 1:
                file_overlap_count += 1

    # Calculate metrics
    total_packs = len(packs)
    total_tasks = sum(len(tasks) for tasks in packs.values())
    average_tasks_per_pack = _average_int(total_tasks, total_packs)
    max_dependency_depth = max(dependency_depths) if dependency_depths else 0
    average_dependency_depth = _average_float(dependency_depths)
    independence_ratio = _percentage(independent_count, total_tasks)

    # Format scope distribution
    scope_distribution = dict(scope_counter)

    # Rate granularity
    granularity_rating = _rate_granularity(
        average_tasks_per_pack,
        independence_ratio,
        max_dependency_depth,
    )

    return {
        "total_packs": total_packs,
        "total_tasks": total_tasks,
        "scope_distribution": scope_distribution,
        "average_tasks_per_pack": average_tasks_per_pack,
        "max_dependency_depth": max_dependency_depth,
        "average_dependency_depth": average_dependency_depth,
        "file_overlap_count": file_overlap_count,
        "independent_task_count": independent_count,
        "dependent_task_count": dependent_count,
        "independence_ratio": independence_ratio,
        "granularity_rating": granularity_rating,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_files(value: object) -> list[str]:
    """Normalize file list, handling various input types."""
    if isinstance(value, str):
        files = [value]
    elif isinstance(value, (list, tuple)):
        files = [f for f in value if isinstance(f, str)]
    else:
        return []

    # Normalize file paths
    normalized = []
    for file in files:
        file = file.strip()
        if not file:
            continue
        # Convert backslashes to forward slashes
        file = file.replace("\\", "/")
        # Remove leading ./
        if file.startswith("./"):
            file = file[2:]
        normalized.append(file)

    return normalized


def _normalize_list(value: object) -> list[str]:
    """Normalize a list of strings."""
    if isinstance(value, str):
        return [value] if value.strip() else []
    elif isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [str(item).strip() for item in value if str(item).strip()]
    return []


def _calculate_dependency_depth(
    task_id: str,
    task_map: dict[str, dict[str, Any]],
    visited: set[str] | None = None,
) -> int:
    """Calculate dependency depth for a task (recursive).

    Returns the maximum depth of the dependency chain.
    Depth 0 = no dependencies, Depth 1 = depends on independent tasks, etc.
    """
    if visited is None:
        visited = set()

    # Prevent circular dependencies
    if task_id in visited:
        return 0

    task = task_map.get(task_id)
    if not task:
        return 0

    depends_on = task.get("depends_on", [])
    if not depends_on:
        return 0

    visited.add(task_id)

    # Calculate max depth from dependencies
    max_depth = 0
    for dep_id in depends_on:
        dep_depth = _calculate_dependency_depth(dep_id, task_map, visited.copy())
        max_depth = max(max_depth, dep_depth + 1)

    return max_depth


def _rate_granularity(
    avg_tasks: float,
    independence_ratio: float,
    max_depth: int,
) -> str:
    """Rate task granularity based on metrics.

    Ratings:
    - over_granular: Too many small tasks (avg > 8, low independence)
    - under_granular: Too few large tasks (avg < 3, high independence)
    - well_balanced: Good balance of task sizes and dependencies
    - deep_dependencies: Long dependency chains (depth > 5)
    """
    if max_depth > 5:
        return "deep_dependencies"
    elif avg_tasks > 8 and independence_ratio < 50.0:
        return "over_granular"
    elif avg_tasks < 3 and independence_ratio > 80.0:
        return "under_granular"
    else:
        return "well_balanced"


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average_int(total: int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def _average_float(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
