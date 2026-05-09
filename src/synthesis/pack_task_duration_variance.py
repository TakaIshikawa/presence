"""Pack task duration variance analyzer for task predictability.

Analyzes the variance and predictability of task durations within execution packs
to identify scheduling inefficiencies, outlier tasks, and overall pack predictability.
Higher variance indicates less predictable task durations, which can complicate
parallel execution planning and resource allocation.

Duration metrics:
- Duration variance: Coefficient of variation (stddev/mean) of task durations
- Predictability score: Inverse of variance, capped at 1.0 (1.0 = perfectly predictable)
- Outlier task rate: Proportion of tasks exceeding 2x mean duration
- Mean duration: Average task duration across all tasks in the pack

Predictability patterns:
- High predictability: Low variance, few outliers, consistent durations
- Low predictability: High variance, many outliers, inconsistent durations
- Uniform: Zero variance, all tasks take same duration
"""

from __future__ import annotations

import math
from typing import Any


# Predictability thresholds
LOW_PREDICTABILITY_THRESHOLD = 0.5
OUTLIER_MULTIPLIER = 2.0


def analyze_pack_task_duration_variance(records: object) -> dict[str, Any]:
    """Analyze task duration variance and predictability within execution packs.

    Evaluates the consistency and predictability of task durations to identify
    scheduling inefficiencies and outlier tasks that may require special handling.

    Args:
        records: List of task duration values in seconds (numeric values).
                Can be a list of dicts with 'duration' key or a list of numbers.

    Returns:
        Dict with:
            - total_tasks: Total number of tasks analyzed
            - mean_duration: Mean task duration in seconds
            - duration_variance: Coefficient of variation (stddev/mean)
            - predictability_score: 1 - min(duration_variance, 1.0)
            - outlier_task_rate: Percentage of tasks >2x mean duration
            - outlier_count: Number of outlier tasks
            - warnings: List of warning messages for low predictability

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of task durations")

    # Extract durations from records
    durations: list[float] = []
    for item in records:
        duration = _extract_duration(item)
        if duration is not None and duration >= 0:
            durations.append(duration)

    total_tasks = len(durations)

    # Handle empty input
    if total_tasks == 0:
        return {
            "total_tasks": 0,
            "mean_duration": 0.0,
            "duration_variance": 0.0,
            "predictability_score": 1.0,
            "outlier_task_rate": 0.0,
            "outlier_count": 0,
            "warnings": [],
        }

    # Calculate mean duration
    mean_duration = _average(durations)

    # Handle single task case
    if total_tasks == 1:
        return {
            "total_tasks": 1,
            "mean_duration": mean_duration,
            "duration_variance": 0.0,
            "predictability_score": 1.0,
            "outlier_task_rate": 0.0,
            "outlier_count": 0,
            "warnings": [],
        }

    # Calculate standard deviation
    stddev = _standard_deviation(durations, mean_duration)

    # Calculate duration variance (coefficient of variation)
    if mean_duration > 0:
        duration_variance = round(stddev / mean_duration, 2)
    else:
        duration_variance = 0.0

    # Calculate predictability score
    predictability_score = round(1.0 - min(duration_variance, 1.0), 2)

    # Count outlier tasks (>2x mean duration)
    outlier_threshold = mean_duration * OUTLIER_MULTIPLIER
    outlier_count = sum(1 for d in durations if d > outlier_threshold)
    outlier_task_rate = _percentage(outlier_count, total_tasks)

    # Generate warnings
    warnings: list[str] = []
    if predictability_score < LOW_PREDICTABILITY_THRESHOLD:
        warnings.append(
            f"Low predictability score ({predictability_score:.2f}): "
            f"Task durations are highly variable (variance={duration_variance:.2f})"
        )

    return {
        "total_tasks": total_tasks,
        "mean_duration": mean_duration,
        "duration_variance": duration_variance,
        "predictability_score": predictability_score,
        "outlier_task_rate": outlier_task_rate,
        "outlier_count": outlier_count,
        "warnings": warnings,
    }


def _extract_duration(item: object) -> float | None:
    """Extract duration value from various input formats.

    Args:
        item: Can be a number, a dict with 'duration' key, or other type

    Returns:
        Duration as float, or None if cannot extract
    """
    # Handle numeric types directly
    if isinstance(item, (int, float)):
        return float(item)

    # Handle dict with duration key
    if isinstance(item, dict):
        duration = item.get("duration")
        if isinstance(duration, (int, float)):
            return float(duration)

    return None


def _average(values: list[float]) -> float:
    """Calculate average of values.

    Args:
        values: List of numeric values

    Returns:
        Average rounded to 2 decimal places
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _standard_deviation(values: list[float], mean: float) -> float:
    """Calculate standard deviation of values.

    Args:
        values: List of numeric values
        mean: Pre-calculated mean of values

    Returns:
        Standard deviation rounded to 2 decimal places
    """
    if len(values) <= 1:
        return 0.0

    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return round(math.sqrt(variance), 2)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage rounded to 2 decimal places
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
