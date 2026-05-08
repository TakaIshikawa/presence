"""Pack task granularity analyzer for optimal task sizing.

Analyzes task granularity and sizing within execution packs to identify
optimal task composition. Measures task scope distribution, dependency
patterns, and file overlap to detect over-granular (too small) or
under-granular (too large) tasks.

Granularity metrics:
- Scope distribution: Distribution of estimatedScope values
- File count per task: Relationship between scope and expectedFiles
- Dependency depth: Length of task dependency chains
- File overlap: Tasks with overlapping expectedFiles
- Task independence: Ratio of independent vs dependent tasks

Sizing patterns:
- Optimal: Well-balanced task sizes with clear boundaries
- Over-granular: Too many small tasks with excessive dependencies
- Under-granular: Few large tasks doing too much
- Unbalanced: Mix of very small and very large tasks
"""

from __future__ import annotations

from typing import Any, Mapping


# Scope size thresholds
SCOPE_SIZES = {
    "tiny": (0, 50),
    "small": (50, 200),
    "medium": (200, 500),
    "large": (500, 1000),
    "xlarge": (1000, float("inf")),
}


def analyze_pack_task_granularity(records: object) -> dict[str, Any]:
    """Analyze task granularity and sizing in execution packs.

    Measures task scope distribution, dependency patterns, and file overlap
    to assess optimal task composition.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Unique task identifier
            - estimated_scope: Estimated lines of change
            - expected_files: List of files task expects to modify
            - dependencies: List of task IDs this task depends on
            - is_independent: Whether task has no dependencies

    Returns:
        Dict with:
            - total_tasks: Total number of tasks
            - scope_distribution: Count of tasks by size category
            - average_scope: Average estimated scope
            - median_scope: Median estimated scope
            - average_files_per_task: Average expectedFiles count
            - max_dependency_depth: Longest dependency chain
            - independent_task_count: Tasks with no dependencies
            - dependent_task_count: Tasks with dependencies
            - independence_ratio: Percentage of independent tasks
            - overlapping_file_pairs: Count of task pairs with file overlap
            - granularity_pattern: Classification of task sizing

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task dictionaries")

    if not records:
        return _empty_result()

    scopes: list[int] = []
    file_counts: list[int] = []
    independent_count = 0
    dependent_count = 0
    task_files: dict[str, set[str]] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        task_id = _string(record.get("task_id"))
        estimated_scope = _number(record.get("estimated_scope"))
        expected_files = _normalize_files(record.get("expected_files"))
        is_independent = record.get("is_independent") is True

        if estimated_scope is not None and estimated_scope > 0:
            scopes.append(estimated_scope)

        file_counts.append(len(expected_files))

        if task_id and expected_files:
            task_files[task_id] = set(expected_files)

        if is_independent:
            independent_count += 1
        else:
            dependent_count += 1

    total_tasks = len([r for r in records if isinstance(r, Mapping)])
    scope_distribution = _calculate_scope_distribution(scopes)
    average_scope = _average_int(scopes)
    median_scope = _median(scopes)
    average_files_per_task = _average_float(file_counts)
    max_dependency_depth = _calculate_max_dependency_depth(records)
    independence_ratio = _percentage(independent_count, total_tasks)
    overlapping_file_pairs = _count_overlapping_files(task_files)

    granularity_pattern = _classify_granularity_pattern(
        scope_distribution,
        average_scope,
        max_dependency_depth,
        independence_ratio,
        total_tasks,
    )

    return {
        "total_tasks": total_tasks,
        "scope_distribution": scope_distribution,
        "average_scope": average_scope,
        "median_scope": median_scope,
        "average_files_per_task": average_files_per_task,
        "max_dependency_depth": max_dependency_depth,
        "independent_task_count": independent_count,
        "dependent_task_count": dependent_count,
        "independence_ratio": independence_ratio,
        "overlapping_file_pairs": overlapping_file_pairs,
        "granularity_pattern": granularity_pattern,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tasks": 0,
        "scope_distribution": {},
        "average_scope": 0,
        "median_scope": 0,
        "average_files_per_task": 0.0,
        "max_dependency_depth": 0,
        "independent_task_count": 0,
        "dependent_task_count": 0,
        "independence_ratio": 0.0,
        "overlapping_file_pairs": 0,
        "granularity_pattern": "empty",
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _number(value: object) -> int | None:
    """Extract integer from value."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _normalize_files(value: object) -> list[str]:
    """Normalize file list."""
    if isinstance(value, str):
        return [value] if value.strip() else []
    elif isinstance(value, (list, tuple)):
        return [f.strip() for f in value if isinstance(f, str) and f.strip()]
    return []


def _percentage(numerator: int, denominator: int) -> float:
    """Calculate percentage."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average_int(values: list[int]) -> int:
    """Calculate average of integers, returning 0 if empty."""
    if not values:
        return 0
    return round(sum(values) / len(values))


def _average_float(values: list[int]) -> float:
    """Calculate average as float."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _median(values: list[int]) -> int:
    """Calculate median of integers."""
    if not values:
        return 0
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2
    if n % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) // 2
    return sorted_values[mid]


def _calculate_scope_distribution(scopes: list[int]) -> dict[str, int]:
    """Calculate distribution of scopes by size category."""
    distribution: dict[str, int] = {
        "tiny": 0,
        "small": 0,
        "medium": 0,
        "large": 0,
        "xlarge": 0,
    }

    for scope in scopes:
        for size_name, (min_val, max_val) in SCOPE_SIZES.items():
            if min_val <= scope < max_val:
                distribution[size_name] += 1
                break

    return distribution


def _calculate_max_dependency_depth(records: list[Any]) -> int:
    """Calculate maximum dependency chain depth.

    Uses iterative approach to find longest dependency path.
    """
    if not records:
        return 0

    # Build dependency map
    dependencies: dict[str, list[str]] = {}
    for record in records:
        if not isinstance(record, Mapping):
            continue

        task_id = _string(record.get("task_id"))
        deps = record.get("dependencies")

        if task_id:
            if isinstance(deps, list):
                dependencies[task_id] = [
                    _string(d) for d in deps if isinstance(d, str)
                ]
            else:
                dependencies[task_id] = []

    # Calculate depth for each task
    max_depth = 0
    for task_id in dependencies:
        depth = _get_dependency_depth(task_id, dependencies, set())
        max_depth = max(max_depth, depth)

    return max_depth


def _get_dependency_depth(
    task_id: str,
    dependencies: dict[str, list[str]],
    visited: set[str],
) -> int:
    """Get dependency depth for a task (recursive with cycle detection)."""
    if task_id in visited or task_id not in dependencies:
        return 0

    visited.add(task_id)
    deps = dependencies.get(task_id, [])

    if not deps:
        return 0

    max_child_depth = 0
    for dep in deps:
        depth = _get_dependency_depth(dep, dependencies, visited.copy())
        max_child_depth = max(max_child_depth, depth)

    return max_child_depth + 1


def _count_overlapping_files(task_files: dict[str, set[str]]) -> int:
    """Count pairs of tasks with overlapping expectedFiles."""
    if not task_files:
        return 0

    overlap_count = 0
    task_ids = list(task_files.keys())

    for i in range(len(task_ids)):
        for j in range(i + 1, len(task_ids)):
            files_i = task_files[task_ids[i]]
            files_j = task_files[task_ids[j]]

            if files_i & files_j:  # Set intersection
                overlap_count += 1

    return overlap_count


def _classify_granularity_pattern(
    scope_distribution: dict[str, int],
    average_scope: int,
    max_dependency_depth: int,
    independence_ratio: float,
    total_tasks: int,
) -> str:
    """Classify task granularity pattern.

    Patterns:
    - optimal: Balanced scope distribution, moderate dependencies
    - over_granular: Many tiny tasks with deep dependencies
    - under_granular: Few large tasks
    - unbalanced: Wide variance in task sizes
    - simple: Too few tasks to classify
    - empty: No tasks
    """
    if total_tasks == 0:
        return "empty"

    if total_tasks < 3:
        return "simple"

    tiny_count = scope_distribution.get("tiny", 0)
    small_count = scope_distribution.get("small", 0)
    medium_count = scope_distribution.get("medium", 0)
    large_count = scope_distribution.get("large", 0)
    xlarge_count = scope_distribution.get("xlarge", 0)

    # Over-granular: many tiny/small tasks with deep dependencies
    if (tiny_count + small_count) > total_tasks * 0.7 and max_dependency_depth > 3:
        return "over_granular"

    # Under-granular: mostly large/xlarge tasks
    if (large_count + xlarge_count) > total_tasks * 0.6:
        return "under_granular"

    # Unbalanced: high variance (both tiny and xlarge tasks)
    if tiny_count > 0 and xlarge_count > 0 and total_tasks > 3:
        return "unbalanced"

    # Optimal: balanced distribution, reasonable dependencies
    if medium_count > total_tasks * 0.4 and max_dependency_depth <= 3:
        return "optimal"

    # Default: moderate/balanced
    return "balanced"
