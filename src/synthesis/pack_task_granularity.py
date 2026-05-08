"""Pack task granularity analyzer for execution pack sizing.

Analyzes task granularity and appropriate sizing in execution packs. Evaluates
distribution of estimated scope, relationship between scope and actual changes,
dependency chain depths, and file overlap within packs.

Granularity metrics:
- Scope distribution: Small, medium, large task distribution
- Scope-to-files ratio: Relationship between estimated scope and actual changes
- Dependency depth: Maximum and average depth of task dependency chains
- File ownership conflicts: Tasks with overlapping expectedFiles
- Independence balance: Ratio of independent vs dependent tasks
- Optimal pack composition: Well-balanced vs over/under-granular
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_task_granularity(records: object) -> dict[str, Any]:
    """Analyze task granularity and sizing in execution packs.

    Evaluates scope distribution, dependency depths, file overlap,
    and balance of independent vs dependent tasks.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - tasks: List of task dictionaries, each with:
                - task_id: Task identifier
                - estimated_scope: Size estimate (small/medium/large)
                - expected_files: List of files expected to be modified
                - changed_files: List of files actually changed
                - dependencies: List of task IDs this task depends on

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - scope_distribution: Count of small/medium/large tasks
            - avg_tasks_per_pack: Average number of tasks in a pack
            - max_dependency_depth: Maximum dependency chain depth
            - avg_dependency_depth: Average dependency chain depth
            - file_conflicts: Tasks with overlapping expected files
            - independence_ratio: Percentage of independent tasks
            - granularity_pattern: Classification (well_balanced, over_granular, under_granular)
            - examples: Sample packs demonstrating patterns

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_tasks = 0
    scope_counts = {"small": 0, "medium": 0, "large": 0}
    dependency_depths: list[int] = []
    all_file_conflicts: list[dict[str, Any]] = []
    total_independent_tasks = 0
    total_dependent_tasks = 0
    granularity_patterns: list[str] = []

    for pack in records:
        if not isinstance(pack, Mapping):
            continue

        pack_id = _string(pack.get("pack_id"))
        tasks = pack.get("tasks", [])
        if not isinstance(tasks, list):
            continue

        total_packs += 1
        total_tasks += len(tasks)

        # Analyze each task
        for task in tasks:
            if not isinstance(task, dict):
                continue

            # Track scope
            estimated_scope = _string(task.get("estimated_scope")).lower()
            if estimated_scope in scope_counts:
                scope_counts[estimated_scope] += 1

            # Track dependencies
            dependencies = task.get("dependencies", [])
            if isinstance(dependencies, list) and len(dependencies) > 0:
                total_dependent_tasks += 1
            else:
                total_independent_tasks += 1

        # Calculate dependency depth for this pack
        max_depth = _calculate_max_dependency_depth(tasks)
        if max_depth > 0:
            dependency_depths.append(max_depth)

        # Detect file conflicts
        conflicts = _detect_file_conflicts(pack_id, tasks)
        all_file_conflicts.extend(conflicts)

        # Classify pack granularity
        pattern = _classify_pack_granularity(tasks)
        granularity_patterns.append(pattern)

    # Calculate metrics
    avg_tasks_per_pack = round(
        total_tasks / total_packs if total_packs > 0 else 0.0,
        2
    )

    max_dependency_depth = max(dependency_depths) if dependency_depths else 0
    avg_dependency_depth = round(
        sum(dependency_depths) / len(dependency_depths) if dependency_depths else 0.0,
        2
    )

    total_tasks_with_deps = total_independent_tasks + total_dependent_tasks
    independence_ratio = round(
        (total_independent_tasks / total_tasks_with_deps * 100.0) if total_tasks_with_deps > 0 else 0.0,
        2
    )

    # Determine overall granularity pattern
    overall_pattern = _determine_overall_pattern(granularity_patterns)

    return {
        "total_packs": total_packs,
        "scope_distribution": scope_counts,
        "avg_tasks_per_pack": avg_tasks_per_pack,
        "max_dependency_depth": max_dependency_depth,
        "avg_dependency_depth": avg_dependency_depth,
        "file_conflicts": all_file_conflicts[:10],  # Limit to 10
        "independence_ratio": independence_ratio,
        "granularity_pattern": overall_pattern,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _calculate_max_dependency_depth(tasks: list) -> int:
    """Calculate maximum dependency chain depth in a pack.

    Uses recursive depth-first search to find the longest dependency chain.
    """
    # Build dependency graph
    task_deps: dict[str, list[str]] = {}
    for task in tasks:
        if not isinstance(task, dict):
            continue
        task_id = _string(task.get("task_id"))
        dependencies = task.get("dependencies", [])
        if isinstance(dependencies, list):
            task_deps[task_id] = [_string(dep) for dep in dependencies if isinstance(dep, str)]

    if not task_deps:
        return 0

    # Calculate depth for each task
    def get_depth(task_id: str, visited: set[str]) -> int:
        if task_id in visited:  # Cycle detection
            return 0
        if task_id not in task_deps:
            return 0
        if not task_deps[task_id]:  # No dependencies
            return 0

        visited.add(task_id)
        max_child_depth = 0
        for dep in task_deps[task_id]:
            depth = get_depth(dep, visited.copy())
            max_child_depth = max(max_child_depth, depth)

        return max_child_depth + 1

    max_depth = 0
    for task_id in task_deps:
        depth = get_depth(task_id, set())
        max_depth = max(max_depth, depth)

    return max_depth


def _detect_file_conflicts(pack_id: str, tasks: list) -> list[dict[str, Any]]:
    """Detect tasks with overlapping expected files.

    Returns list of conflicts where multiple tasks expect to modify the same file.
    """
    conflicts: list[dict[str, Any]] = []
    file_to_tasks: dict[str, list[str]] = {}

    for task in tasks:
        if not isinstance(task, dict):
            continue

        task_id = _string(task.get("task_id"))
        expected_files = task.get("expected_files", [])

        if not isinstance(expected_files, list):
            continue

        for file in expected_files:
            if not isinstance(file, str):
                continue
            file = file.strip()
            if not file:
                continue

            if file not in file_to_tasks:
                file_to_tasks[file] = []
            file_to_tasks[file].append(task_id)

    # Find files with multiple tasks
    for file, task_ids in file_to_tasks.items():
        if len(task_ids) > 1:
            conflicts.append({
                "pack_id": pack_id,
                "file": file,
                "task_count": len(task_ids),
                "tasks": task_ids[:5],  # Limit to 5
            })

    return conflicts


def _classify_pack_granularity(tasks: list) -> str:
    """Classify pack granularity.

    Returns:
    - "well_balanced": Good mix of task sizes and dependencies
    - "over_granular": Too many small tasks (>80% small)
    - "under_granular": Too many large tasks (>60% large)
    - "mono_task": Only one task in pack
    """
    if len(tasks) == 0:
        return "empty"
    if len(tasks) == 1:
        return "mono_task"

    small_count = 0
    large_count = 0

    for task in tasks:
        if not isinstance(task, dict):
            continue
        scope = _string(task.get("estimated_scope")).lower()
        if scope == "small":
            small_count += 1
        elif scope == "large":
            large_count += 1

    total = len(tasks)
    small_ratio = small_count / total if total > 0 else 0.0
    large_ratio = large_count / total if total > 0 else 0.0

    if small_ratio > 0.8:
        return "over_granular"
    elif large_ratio > 0.6:
        return "under_granular"
    else:
        return "well_balanced"


def _determine_overall_pattern(patterns: list[str]) -> str:
    """Determine overall granularity pattern from all packs.

    Returns the most common pattern.
    """
    if not patterns:
        return "empty"

    # Count patterns
    pattern_counts: dict[str, int] = {}
    for pattern in patterns:
        pattern_counts[pattern] = pattern_counts.get(pattern, 0) + 1

    # Return most common
    return max(pattern_counts, key=pattern_counts.get) if pattern_counts else "empty"  # type: ignore
