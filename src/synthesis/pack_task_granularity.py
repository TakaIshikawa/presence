"""Pack task granularity analyzer for execution pack composition.

Analyzes task sizing and granularity within execution packs to optimize for
agent efficiency and parallelization opportunities. Evaluates the distribution
of estimatedScope values, task dependency chain depths, file overlap conflicts,
and balance of independent vs dependent tasks.

Granularity metrics:
- Scope distribution: Count and percentage of tiny/small/medium/large tasks
- Dependency depth: Maximum and average dependency chain lengths
- File conflicts: Tasks with overlapping expectedFiles within same pack
- Independence ratio: Proportion of tasks without dependencies

Granularity patterns:
- Well-balanced: Mix of scopes, shallow dependencies, minimal conflicts
- Over-granular: Too many tiny tasks, excessive dependencies
- Under-granular: Too many large tasks, potential parallelization missed
- Conflicted: High file overlap indicating poor task isolation
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


# Scope value mappings
SCOPE_TINY = "tiny"
SCOPE_SMALL = "small"
SCOPE_MEDIUM = "medium"
SCOPE_LARGE = "large"

# Granularity thresholds
OPTIMAL_AVG_DEPENDENCY_DEPTH = 2.0
HIGH_DEPENDENCY_DEPTH = 3.0
HIGH_FILE_CONFLICT_RATE = 0.3


def analyze_pack_task_granularity(records: object) -> dict[str, Any]:
    """Analyze task granularity and composition within execution packs.

    Evaluates task sizing, dependency structures, file conflicts, and
    independence ratios to identify optimal pack composition patterns.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - tasks: List of task dicts with:
                - task_id: Task identifier
                - estimated_scope: Scope value (tiny/small/medium/large)
                - depends_on: List of task IDs this task depends on
                - expected_files: List of files expected to be modified
                - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - total_tasks: Total number of tasks across all packs
            - scope_distribution: Dict mapping scope to count
            - scope_percentages: Dict mapping scope to percentage
            - avg_tasks_per_pack: Average number of tasks per pack
            - max_dependency_depth: Maximum dependency chain depth
            - avg_dependency_depth: Average dependency depth across packs
            - file_conflict_rate: Percentage of task pairs with overlapping files
            - independence_ratio: Percentage of tasks with no dependencies
            - granularity_pattern: Classification of overall granularity
            - examples: Examples of packs with different granularity patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tasks = 0
    scope_counts: Counter[str] = Counter()
    dependency_depths: list[int] = []
    total_conflicts = 0
    total_task_pairs = 0
    independent_task_count = 0
    examples: list[dict[str, Any]] = []

    for pack in records:
        if not isinstance(pack, Mapping):
            continue

        pack_id = _string(pack.get("pack_id"))
        tasks = pack.get("tasks")
        if not isinstance(tasks, list):
            continue

        total_packs += 1
        pack_task_count = 0

        # Build task map for dependency analysis
        task_map: dict[str, dict[str, Any]] = {}
        for task in tasks:
            if not isinstance(task, Mapping):
                continue
            task_id = _string(task.get("task_id"))
            if not task_id:
                continue
            task_map[task_id] = {
                "scope": _string(task.get("estimated_scope")),
                "depends_on": _normalize_list(task.get("depends_on")),
                "expected_files": _normalize_files(task.get("expected_files")),
                "title": _string(task.get("task_title")),
            }
            pack_task_count += 1

        total_tasks += pack_task_count

        # Analyze scope distribution
        for task_data in task_map.values():
            scope = task_data["scope"]
            if scope:
                scope_counts[scope] += 1

        # Analyze dependency depths
        max_depth = _calculate_max_dependency_depth(task_map)
        if max_depth >= 0:
            dependency_depths.append(max_depth)

        # Count independent tasks
        for task_data in task_map.values():
            if not task_data["depends_on"]:
                independent_task_count += 1

        # Analyze file conflicts
        conflicts, pairs = _count_file_conflicts(task_map)
        total_conflicts += conflicts
        total_task_pairs += pairs

        # Collect example if interesting
        _maybe_add_example(
            examples,
            pack_id,
            pack_task_count,
            max_depth,
            conflicts,
            pairs,
        )

    # Calculate metrics
    avg_tasks_per_pack = _average(total_tasks, total_packs)
    max_dependency_depth = max(dependency_depths) if dependency_depths else 0
    avg_dependency_depth = _average(sum(dependency_depths), len(dependency_depths))
    file_conflict_rate = _percentage(total_conflicts, total_task_pairs)
    independence_ratio = _percentage(independent_task_count, total_tasks)

    # Calculate scope percentages
    scope_percentages = {
        scope: _percentage(count, total_tasks)
        for scope, count in scope_counts.items()
    }

    # Classify granularity pattern
    granularity_pattern = _classify_granularity_pattern(
        scope_counts,
        total_tasks,
        avg_dependency_depth,
        file_conflict_rate,
        independence_ratio,
    )

    return {
        "total_packs": total_packs,
        "total_tasks": total_tasks,
        "scope_distribution": dict(scope_counts),
        "scope_percentages": scope_percentages,
        "avg_tasks_per_pack": avg_tasks_per_pack,
        "max_dependency_depth": max_dependency_depth,
        "avg_dependency_depth": avg_dependency_depth,
        "file_conflict_rate": file_conflict_rate,
        "independence_ratio": independence_ratio,
        "granularity_pattern": granularity_pattern,
        "examples": examples[:5],  # Limit to 5 examples
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _normalize_list(value: object) -> list[str]:
    """Normalize a list of strings."""
    if isinstance(value, str):
        return [value] if value.strip() else []
    elif isinstance(value, (list, tuple)):
        return [_string(item) for item in value if isinstance(item, str) and item.strip()]
    return []


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


def _calculate_max_dependency_depth(task_map: dict[str, dict[str, Any]]) -> int:
    """Calculate maximum dependency chain depth using memoization.

    Returns:
        Maximum depth, or 0 if no tasks or no dependencies
    """
    if not task_map:
        return 0

    memo: dict[str, int] = {}

    def get_depth(task_id: str, visited: set[str]) -> int:
        """Get depth of task, detecting cycles."""
        if task_id in memo:
            return memo[task_id]

        if task_id not in task_map:
            return 0

        if task_id in visited:
            # Cycle detected, return 0 to avoid infinite recursion
            return 0

        task_data = task_map[task_id]
        depends_on = task_data["depends_on"]

        if not depends_on:
            memo[task_id] = 0
            return 0

        visited.add(task_id)
        max_dep_depth = 0
        for dep_id in depends_on:
            dep_depth = get_depth(dep_id, visited)
            max_dep_depth = max(max_dep_depth, dep_depth)
        visited.remove(task_id)

        depth = max_dep_depth + 1
        memo[task_id] = depth
        return depth

    max_depth = 0
    for task_id in task_map:
        depth = get_depth(task_id, set())
        max_depth = max(max_depth, depth)

    return max_depth


def _count_file_conflicts(task_map: dict[str, dict[str, Any]]) -> tuple[int, int]:
    """Count pairs of tasks with overlapping expected files.

    Returns:
        Tuple of (conflict_count, total_pairs)
    """
    task_list = list(task_map.values())
    if len(task_list) < 2:
        return 0, 0

    conflicts = 0
    total_pairs = 0

    for i in range(len(task_list)):
        for j in range(i + 1, len(task_list)):
            task_a = task_list[i]
            task_b = task_list[j]

            files_a = set(task_a["expected_files"])
            files_b = set(task_b["expected_files"])

            # Skip if either has no files
            if not files_a or not files_b:
                continue

            total_pairs += 1

            # Check for overlap
            if files_a & files_b:
                conflicts += 1

    return conflicts, total_pairs


def _maybe_add_example(
    examples: list[dict[str, Any]],
    pack_id: str,
    task_count: int,
    max_depth: int,
    conflicts: int,
    pairs: int,
) -> None:
    """Add pack example if interesting and we have fewer than 5."""
    if len(examples) >= 5:
        return

    # Only add if pack has tasks
    if task_count == 0:
        return

    # Interesting if has high dependency depth or file conflicts
    conflict_rate = _percentage(conflicts, pairs)
    if max_depth >= HIGH_DEPENDENCY_DEPTH or conflict_rate >= HIGH_FILE_CONFLICT_RATE * 100:
        examples.append({
            "pack_id": pack_id,
            "task_count": task_count,
            "max_dependency_depth": max_depth,
            "file_conflict_rate": conflict_rate,
        })


def _average(total: float | int, count: int) -> float:
    """Calculate average, returning 0.0 if count is 0."""
    if count <= 0:
        return 0.0
    return round(total / count, 2)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _classify_granularity_pattern(
    scope_counts: Counter[str],
    total_tasks: int,
    avg_dependency_depth: float,
    file_conflict_rate: float,
    independence_ratio: float,
) -> str:
    """Classify the overall granularity pattern.

    Patterns:
    - well_balanced: Good mix of scopes, reasonable dependencies, low conflicts
    - over_granular: Too many tiny tasks, high dependency depth
    - under_granular: Too many large tasks, high independence
    - conflicted: High file overlap rate
    - empty: No tasks
    """
    if total_tasks == 0:
        return "empty"

    # Calculate tiny task ratio
    tiny_count = scope_counts.get(SCOPE_TINY, 0)
    tiny_ratio = _percentage(tiny_count, total_tasks)

    # Calculate large task ratio
    large_count = scope_counts.get(SCOPE_LARGE, 0)
    large_ratio = _percentage(large_count, total_tasks)

    # High file conflicts is a problem regardless
    if file_conflict_rate >= HIGH_FILE_CONFLICT_RATE * 100:
        return "conflicted"

    # Over-granular: too many tiny tasks with high dependencies
    if tiny_ratio > 50.0 and avg_dependency_depth >= HIGH_DEPENDENCY_DEPTH:
        return "over_granular"

    # Under-granular: too many large tasks with high independence
    if large_ratio > 50.0 and independence_ratio > 80.0:
        return "under_granular"

    # Well-balanced: good mix, reasonable dependencies
    if (
        avg_dependency_depth <= OPTIMAL_AVG_DEPENDENCY_DEPTH
        and file_conflict_rate < HIGH_FILE_CONFLICT_RATE * 100
        and 30.0 <= independence_ratio <= 80.0
    ):
        return "well_balanced"

    # Default to mixed if doesn't fit other patterns
    return "mixed"
