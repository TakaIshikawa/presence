"""Pack hotspot avoidance analyzer.

Analyzes how well execution packs avoid frequently modified hotspot files.
Tracks tasks touching hotspot files, hotspot collision rate across pack, and
distribution of edits across codebase. Calculates avoidance score as ratio of
non-hotspot to total expectedFiles.

Hotspot avoidance metrics:
- Tasks touching hotspots: Count of tasks editing frequently modified files
- Hotspot collision rate: Percentage of pack tasks using hotspot files
- Non-hotspot file ratio: Distribution of edits across codebase
- Avoidance score: Ratio of non-hotspot to total expectedFiles
- Edit concentration: How spread out edits are across files

Quality indicators:
- High avoidance score (>70%): Most edits avoid hotspot files
- Low collision rate (<30%): Few tasks touching same hotspots
- Well-distributed edits: Changes spread across many files
- Low hotspot dependency: Pack not concentrated on few files
- Reduced merge conflicts: Independent file access patterns
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_hotspot_avoidance(records: object) -> dict[str, Any]:
    """Analyze how well packs avoid frequently modified hotspot files.

    Evaluates hotspot file usage and edit distribution across pack tasks.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - expected_files: List of files declared in expectedFiles
            - hotspot_files_count: Number of hotspot files in expectedFiles
            - is_touching_hotspot: Boolean indicating task edits hotspot
            - total_files_count: Total files in expectedFiles
            - unique_files_edited: Files unique to this task

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - tasks_touching_hotspots: Count of tasks editing hotspot files
            - hotspot_collision_rate: Percentage of tasks using hotspots
            - total_expected_files: Sum of all expectedFiles
            - total_hotspot_files: Sum of hotspot files across tasks
            - total_non_hotspot_files: Sum of non-hotspot files
            - avoidance_score: Non-hotspot / total expectedFiles (%)
            - avg_hotspots_per_task: Average hotspot files per task
            - tasks_with_no_hotspots: Count of tasks avoiding hotspots
            - tasks_with_all_hotspots: Count of tasks using only hotspots
            - well_distributed_edits: Tasks with many unique files

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
    tasks_touching_hotspots = 0
    total_expected_files = 0
    total_hotspot_files = 0
    tasks_with_no_hotspots = 0
    tasks_with_all_hotspots = 0
    well_distributed_tasks = 0
    hotspot_counts: list[int | float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        expected_files = record.get("expected_files")
        hotspot_count = _extract_number(record.get("hotspot_files_count"))
        is_touching = record.get("is_touching_hotspot")
        total_files = _extract_number(record.get("total_files_count"))
        unique_files = record.get("unique_files_edited")

        # Count expected files
        if isinstance(expected_files, list):
            file_count = len(expected_files)
            total_expected_files += file_count

            # Track total files if not provided separately
            if total_files is None:
                total_files = file_count

        # Track hotspot files
        if hotspot_count is not None:
            hotspot_count_int = int(hotspot_count)
            total_hotspot_files += hotspot_count_int
            hotspot_counts.append(hotspot_count)

            # Check if task has no hotspots
            if hotspot_count_int == 0:
                tasks_with_no_hotspots += 1

            # Check if task has all hotspots
            if total_files is not None and total_files > 0:
                if hotspot_count_int >= total_files:
                    tasks_with_all_hotspots += 1

        # Track tasks touching hotspots
        if is_touching is True:
            tasks_touching_hotspots += 1

        # Track well-distributed edits (many unique files)
        if isinstance(unique_files, list) and len(unique_files) >= 3:
            well_distributed_tasks += 1

    # Calculate non-hotspot files
    total_non_hotspot = total_expected_files - total_hotspot_files

    # Calculate aggregate metrics
    collision_rate = _percentage(tasks_touching_hotspots, total_tasks)
    avoidance_score = _percentage(total_non_hotspot, total_expected_files)
    avg_hotspots = _average(hotspot_counts)

    return {
        "total_tasks": total_tasks,
        "tasks_touching_hotspots": tasks_touching_hotspots,
        "hotspot_collision_rate": collision_rate,
        "total_expected_files": total_expected_files,
        "total_hotspot_files": total_hotspot_files,
        "total_non_hotspot_files": total_non_hotspot,
        "avoidance_score": avoidance_score,
        "avg_hotspots_per_task": avg_hotspots,
        "tasks_with_no_hotspots": tasks_with_no_hotspots,
        "tasks_with_all_hotspots": tasks_with_all_hotspots,
        "well_distributed_edits": well_distributed_tasks,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tasks": 0,
        "tasks_touching_hotspots": 0,
        "hotspot_collision_rate": 0.0,
        "total_expected_files": 0,
        "total_hotspot_files": 0,
        "total_non_hotspot_files": 0,
        "avoidance_score": 0.0,
        "avg_hotspots_per_task": 0.0,
        "tasks_with_no_hotspots": 0,
        "tasks_with_all_hotspots": 0,
        "well_distributed_edits": 0,
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
