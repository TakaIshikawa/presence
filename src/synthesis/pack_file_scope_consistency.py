"""Pack file scope consistency analyzer.

Analyzes consistency of file scopes across tasks within execution packs. Tracks
tasks with overlapping expectedFiles, ratio of shared hotspot files to total files,
and scope divergence (files edited outside expectedFiles). Measures how well actual
edits align with declared expectedFiles.

File scope consistency metrics:
- Overlapping files: Tasks with shared expectedFiles
- Hotspot file ratio: Shared files to total files ratio
- Scope divergence score: Files edited outside expectedFiles
- Consistency score: Alignment between expectedFiles and actual edits
- Collision rate: Percentage of tasks touching same files

Quality indicators:
- High consistency score (>90%): Edits align with declared scope
- Low hotspot collision (<30%): Well-distributed file access
- Low scope divergence (<10%): Minimal unexpected file edits
- Moderate overlap (20-40%): Some shared context, not excessive
- Clear file boundaries: Each task has distinct file scope
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_file_scope_consistency(records: object) -> dict[str, Any]:
    """Analyze file scope consistency across tasks in execution packs.

    Evaluates consistency of file scopes and identifies hotspot collisions and
    scope divergence across pack tasks.

    Args:
        records: List of task dictionaries with keys:
            - task_id: Task identifier
            - expected_files: List of files declared in expectedFiles
            - actual_files_edited: List of files actually edited
            - shared_files_count: Number of files shared with other tasks
            - is_hotspot_file: Boolean for each expected file
            - scope_divergence_count: Files edited but not in expectedFiles

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - tasks_with_overlapping_files: Tasks sharing expectedFiles
            - total_expected_files: Total files across all expectedFiles
            - total_shared_files: Files appearing in multiple task scopes
            - total_actual_edits: Total files actually edited
            - total_scope_divergence: Files edited outside expectedFiles
            - consistency_score: % of actual edits within expectedFiles
            - hotspot_collision_rate: % of tasks touching hotspot files
            - avg_overlap_per_task: Average shared files per task
            - scope_divergence_rate: % of edits outside declared scope
            - tasks_with_no_overlap: Tasks with unique file scopes
            - tasks_with_complete_overlap: Tasks sharing all files

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
    tasks_with_overlap = 0
    total_expected_files = 0
    total_shared_files = 0
    total_actual_edits = 0
    total_scope_divergence = 0
    tasks_touching_hotspots = 0
    shared_files_per_task: list[int | float] = []
    tasks_with_no_overlap = 0
    tasks_with_complete_overlap = 0

    # Track actual edits within expected scope
    edits_within_scope = 0
    edits_outside_scope = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_tasks += 1

        expected_files = record.get("expected_files")
        actual_files = record.get("actual_files_edited")
        shared_count = _extract_number(record.get("shared_files_count"))
        divergence_count = _extract_number(record.get("scope_divergence_count"))
        is_hotspot = record.get("is_hotspot_file")

        # Track expected files
        if isinstance(expected_files, list):
            total_expected_files += len(expected_files)

        # Track actual edits
        if isinstance(actual_files, list):
            actual_edit_count = len(actual_files)
            total_actual_edits += actual_edit_count

            # Calculate edits within scope
            if isinstance(expected_files, list):
                expected_set = set(expected_files)
                actual_set = set(actual_files)
                within_scope = len(actual_set & expected_set)
                outside_scope = len(actual_set - expected_set)
                edits_within_scope += within_scope
                edits_outside_scope += outside_scope

        # Track shared files
        if shared_count is not None:
            shared_count_int = int(shared_count)
            total_shared_files += shared_count_int
            shared_files_per_task.append(shared_count)

            if shared_count_int > 0:
                tasks_with_overlap += 1
            else:
                tasks_with_no_overlap += 1

            # Check for complete overlap
            if isinstance(expected_files, list) and len(expected_files) > 0:
                if shared_count_int >= len(expected_files):
                    tasks_with_complete_overlap += 1

        # Track scope divergence
        if divergence_count is not None:
            total_scope_divergence += int(divergence_count)

        # Track hotspot files
        if is_hotspot is True:
            tasks_touching_hotspots += 1

    # Calculate aggregate metrics
    consistency_score = _percentage(edits_within_scope, total_actual_edits)
    hotspot_collision = _percentage(tasks_touching_hotspots, total_tasks)
    avg_overlap = _average(shared_files_per_task)
    scope_divergence_rate = _percentage(edits_outside_scope, total_actual_edits)

    return {
        "total_tasks": total_tasks,
        "tasks_with_overlapping_files": tasks_with_overlap,
        "total_expected_files": total_expected_files,
        "total_shared_files": total_shared_files,
        "total_actual_edits": total_actual_edits,
        "total_scope_divergence": total_scope_divergence,
        "consistency_score": consistency_score,
        "hotspot_collision_rate": hotspot_collision,
        "avg_overlap_per_task": avg_overlap,
        "scope_divergence_rate": scope_divergence_rate,
        "tasks_with_no_overlap": tasks_with_no_overlap,
        "tasks_with_complete_overlap": tasks_with_complete_overlap,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_tasks": 0,
        "tasks_with_overlapping_files": 0,
        "total_expected_files": 0,
        "total_shared_files": 0,
        "total_actual_edits": 0,
        "total_scope_divergence": 0,
        "consistency_score": 0.0,
        "hotspot_collision_rate": 0.0,
        "avg_overlap_per_task": 0.0,
        "scope_divergence_rate": 0.0,
        "tasks_with_no_overlap": 0,
        "tasks_with_complete_overlap": 0,
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
