"""Pack task scope granularity analyzer.

Analyzes task scope granularity and sizing patterns in execution packs. Evaluates
scope distribution (small/medium/large), calculates scope variance within packs,
and reports statistics on scope alignment with actual file changes.

Scope granularity metrics:
- Scope distribution: Breakdown of small/medium/large tasks
- Scope variance: Consistency of task sizing within packs
- Scope estimation accuracy: How well estimated scope matches actual changes
- Pack homogeneity: Whether tasks in a pack have similar scope
- Granularity consistency: Alignment of scope sizing across packs

Quality indicators:
- Balanced scope distribution: Mix of task sizes appropriate for work
- Low scope variance: Consistent task sizing within packs
- High estimation accuracy (>80%): Scope estimates match actual changes
- Moderate homogeneity: Pack tasks have similar but not identical scope
- Consistent granularity: Scope sizing patterns align across packs
"""

from __future__ import annotations

from typing import Any, Mapping


# Scope size categories
SCOPE_SMALL = "small"
SCOPE_MEDIUM = "medium"
SCOPE_LARGE = "large"


def analyze_pack_task_scope_analyzer(records: object) -> dict[str, Any]:
    """Analyze task scope granularity and sizing patterns in execution packs.

    Evaluates scope distribution, variance within packs, and estimation accuracy
    against actual file changes.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_tasks: Total number of tasks in pack
            - small_scope_tasks: Number of tasks with small scope
            - medium_scope_tasks: Number of tasks with medium scope
            - large_scope_tasks: Number of tasks with large scope
            - expected_files_count: Total expected files to modify
            - actual_files_changed: Actual files changed during execution
            - scope_variance: Variance in scope sizes within pack (0.0-1.0)
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_tasks_per_pack: Average number of tasks per pack
            - small_scope_ratio: Percentage of tasks with small scope
            - medium_scope_ratio: Percentage of tasks with medium scope
            - large_scope_ratio: Percentage of tasks with large scope
            - avg_scope_variance: Average scope variance within packs
            - high_homogeneity_packs: Count of packs with low variance (<0.3)
            - low_homogeneity_packs: Count of packs with high variance (>0.7)
            - avg_estimation_accuracy: Average scope estimation accuracy
            - high_accuracy_packs: Count of packs with >80% accuracy
            - low_accuracy_packs: Count of packs with <50% accuracy
            - balanced_distribution_packs: Packs with good scope mix

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    tasks_per_pack: list[int | float] = []

    small_scope_counts: list[int] = []
    medium_scope_counts: list[int] = []
    large_scope_counts: list[int] = []
    total_task_counts: list[int] = []

    scope_variances: list[float] = []
    estimation_accuracies: list[float] = []

    high_homogeneity_packs = 0  # variance < 0.3
    low_homogeneity_packs = 0   # variance > 0.7

    high_accuracy_packs = 0  # accuracy > 80%
    low_accuracy_packs = 0   # accuracy < 50%

    balanced_distribution_packs = 0

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_tasks = _extract_int(record.get("total_tasks"))
        small_scope = _extract_int(record.get("small_scope_tasks"))
        medium_scope = _extract_int(record.get("medium_scope_tasks"))
        large_scope = _extract_int(record.get("large_scope_tasks"))
        expected_files = _extract_int(record.get("expected_files_count"))
        actual_files = _extract_int(record.get("actual_files_changed"))
        scope_variance = _extract_float(record.get("scope_variance"))

        total_packs += 1

        # Track tasks per pack
        if total_tasks is not None:
            tasks_per_pack.append(total_tasks)
            total_task_counts.append(total_tasks)

            # Track scope distribution
            if small_scope is not None:
                small_scope_counts.append(small_scope)
            if medium_scope is not None:
                medium_scope_counts.append(medium_scope)
            if large_scope is not None:
                large_scope_counts.append(large_scope)

            # Check for balanced distribution
            if small_scope is not None and medium_scope is not None and large_scope is not None:
                if _is_balanced_distribution(small_scope, medium_scope, large_scope, total_tasks):
                    balanced_distribution_packs += 1

        # Track scope variance (homogeneity)
        if scope_variance is not None:
            scope_variances.append(scope_variance)

            if scope_variance < 0.3:
                high_homogeneity_packs += 1
            elif scope_variance > 0.7:
                low_homogeneity_packs += 1

        # Track estimation accuracy
        if expected_files is not None and actual_files is not None:
            accuracy = _calculate_estimation_accuracy(expected_files, actual_files)
            estimation_accuracies.append(accuracy)

            if accuracy > 80.0:
                high_accuracy_packs += 1
            elif accuracy < 50.0:
                low_accuracy_packs += 1

    # Calculate aggregate metrics
    avg_tasks_per_pack = _average(tasks_per_pack)

    total_small = sum(small_scope_counts)
    total_medium = sum(medium_scope_counts)
    total_large = sum(large_scope_counts)
    total_all_tasks = sum(total_task_counts)

    small_scope_ratio = _percentage(total_small, total_all_tasks)
    medium_scope_ratio = _percentage(total_medium, total_all_tasks)
    large_scope_ratio = _percentage(total_large, total_all_tasks)

    avg_scope_variance = _average(scope_variances)
    avg_estimation_accuracy = _average(estimation_accuracies)

    return {
        "total_packs": total_packs,
        "avg_tasks_per_pack": avg_tasks_per_pack,
        "small_scope_ratio": small_scope_ratio,
        "medium_scope_ratio": medium_scope_ratio,
        "large_scope_ratio": large_scope_ratio,
        "avg_scope_variance": avg_scope_variance,
        "high_homogeneity_packs": high_homogeneity_packs,
        "low_homogeneity_packs": low_homogeneity_packs,
        "avg_estimation_accuracy": avg_estimation_accuracy,
        "high_accuracy_packs": high_accuracy_packs,
        "low_accuracy_packs": low_accuracy_packs,
        "balanced_distribution_packs": balanced_distribution_packs,
    }


def _is_balanced_distribution(
    small: int, medium: int, large: int, total: int
) -> bool:
    """Check if scope distribution is balanced (no category dominates)."""
    if total == 0:
        return False

    # Balanced means no single category exceeds 70% of tasks
    max_ratio = max(small, medium, large) / total
    return max_ratio <= 0.7


def _calculate_estimation_accuracy(expected: int, actual: int) -> float:
    """Calculate estimation accuracy as percentage match.

    Accuracy = 100% - |expected - actual| / max(expected, actual) * 100
    """
    if expected == 0 and actual == 0:
        return 100.0

    max_value = max(expected, actual)
    if max_value == 0:
        return 0.0

    difference = abs(expected - actual)
    accuracy = (1 - (difference / max_value)) * 100
    return round(max(0.0, accuracy), 2)


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value) if isinstance(value, float) else value
    return None


def _extract_float(value: object) -> float | None:
    """Extract float from value if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
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
