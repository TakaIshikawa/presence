"""Pack batch coherence analyzer for semantic task grouping quality.

Analyzes semantic coherence of tasks batched together in execution packs to measure
how well tasks are grouped thematically, categorically, and by dependency structure.
Evaluates whether batching decisions result in cohesive work units.

Batch coherence metrics:
- Task title similarity: Pairwise SequenceMatcher scores
- Shared category rate: Tasks with same category
- ProjectMapSlice overlap: Shared slice usage
- Dependency chain length: Max depth of dependsOn graphs
- Root task ratio: Tasks with no dependencies / total

Quality indicators:
- High title similarity (>0.6): Related task descriptions
- High category alignment (>80%): Thematically grouped
- High slice overlap (>70%): Working in same codebase areas
- Short dependency chains (<3): Minimal coupling complexity
- Moderate root ratio (40-70%): Balance of independent and dependent work
"""

from __future__ import annotations

from difflib import SequenceMatcher
from typing import Any, Mapping


def analyze_pack_batch_coherence(records: object) -> dict[str, Any]:
    """Analyze semantic coherence of tasks batched in execution packs.

    Evaluates task grouping quality through title similarity, category alignment,
    slice overlap, and dependency structure analysis.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - task_titles: List of task title strings
            - task_categories: List of task category strings
            - task_slices: List of lists (each task's projectMapSlices)
            - dependency_chain_length: Max depth of dependsOn graph
            - root_task_count: Tasks with no dependencies
            - total_task_count: Total tasks in pack
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_task_title_similarity: Average pairwise title similarity
            - avg_shared_category_rate: Average % same category
            - avg_slice_overlap_rate: Average % shared slices
            - avg_dependency_chain_length: Average max dependency depth
            - avg_root_task_ratio: Average % root tasks
            - high_coherence_packs: Count with >0.7 title similarity
            - low_coherence_packs: Count with <0.4 title similarity

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    title_similarities: list[float] = []
    category_rates: list[float] = []
    slice_overlap_rates: list[float] = []
    chain_lengths: list[int | float] = []
    root_ratios: list[float] = []

    high_coherence_packs = 0  # >0.7 title similarity
    low_coherence_packs = 0   # <0.4 title similarity

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        task_titles = record.get("task_titles")
        task_categories = record.get("task_categories")
        task_slices = record.get("task_slices")
        chain_length = _extract_int(record.get("dependency_chain_length"))
        root_count = _extract_int(record.get("root_task_count"))
        total_tasks = _extract_int(record.get("total_task_count"))

        # Calculate title similarity
        if isinstance(task_titles, list) and len(task_titles) > 1:
            similarity = _calculate_pairwise_similarity(task_titles)
            title_similarities.append(similarity)

            # Classify coherence
            if similarity > 0.7:
                high_coherence_packs += 1
            elif similarity < 0.4:
                low_coherence_packs += 1

        # Calculate category alignment
        if isinstance(task_categories, list) and len(task_categories) > 1:
            category_rate = _calculate_category_alignment(task_categories)
            category_rates.append(category_rate)

        # Calculate slice overlap
        if isinstance(task_slices, list) and len(task_slices) > 1:
            overlap_rate = _calculate_slice_overlap(task_slices)
            slice_overlap_rates.append(overlap_rate)

        # Track dependency chain length
        if chain_length is not None:
            chain_lengths.append(chain_length)

        # Calculate root task ratio
        if root_count is not None and total_tasks is not None and total_tasks > 0:
            root_ratios.append(_percentage(root_count, total_tasks))

    # Calculate aggregate metrics
    avg_similarity = _average(title_similarities)
    avg_category = _average(category_rates)
    avg_overlap = _average(slice_overlap_rates)
    avg_chain = _average(chain_lengths)
    avg_root = _average(root_ratios)

    return {
        "total_packs": total_packs,
        "avg_task_title_similarity": avg_similarity,
        "avg_shared_category_rate": avg_category,
        "avg_slice_overlap_rate": avg_overlap,
        "avg_dependency_chain_length": avg_chain,
        "avg_root_task_ratio": avg_root,
        "high_coherence_packs": high_coherence_packs,
        "low_coherence_packs": low_coherence_packs,
    }


def _calculate_pairwise_similarity(titles: list[object]) -> float:
    """Calculate average pairwise title similarity using SequenceMatcher."""
    valid_titles = [str(t) for t in titles if isinstance(t, str) and t.strip()]

    if len(valid_titles) < 2:
        return 0.0

    similarities: list[float] = []
    for i in range(len(valid_titles)):
        for j in range(i + 1, len(valid_titles)):
            matcher = SequenceMatcher(None, valid_titles[i], valid_titles[j])
            similarities.append(matcher.ratio())

    if not similarities:
        return 0.0

    return round(sum(similarities) / len(similarities), 3)


def _calculate_category_alignment(categories: list[object]) -> float:
    """Calculate percentage of tasks sharing the same category."""
    valid_categories = [
        str(c).strip() for c in categories
        if isinstance(c, str) and c.strip()
    ]

    if len(valid_categories) < 2:
        return 0.0

    # Count most common category
    category_counts: dict[str, int] = {}
    for cat in valid_categories:
        category_counts[cat] = category_counts.get(cat, 0) + 1

    max_count = max(category_counts.values()) if category_counts else 0
    return _percentage(max_count, len(valid_categories))


def _calculate_slice_overlap(task_slices: list[object]) -> float:
    """Calculate percentage of shared projectMapSlices across tasks."""
    valid_slices: list[set[str]] = []

    for slices in task_slices:
        if isinstance(slices, list):
            slice_set = {str(s).strip() for s in slices if isinstance(s, str) and s.strip()}
            if slice_set:
                valid_slices.append(slice_set)

    if len(valid_slices) < 2:
        return 0.0

    # Calculate intersection across all tasks
    common_slices = valid_slices[0].copy()
    for slice_set in valid_slices[1:]:
        common_slices &= slice_set

    # Calculate average slice count
    avg_slices = sum(len(s) for s in valid_slices) / len(valid_slices)

    if avg_slices == 0:
        return 0.0

    return _percentage(len(common_slices), avg_slices)


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
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
