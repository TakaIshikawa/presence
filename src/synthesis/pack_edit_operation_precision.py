"""Pack edit operation precision analyzer for edit effectiveness.

Analyzes Edit tool usage patterns across execution packs. Measures edit
success rate (successful vs failed edits), average lines changed per edit,
edit targeting precision (small focused edits vs large replacements), and
correlation between edit size and failure rate.

Edit precision metrics:
- Edit success rate: Percentage of successful edits vs failures
- Average lines changed: Mean number of lines modified per edit
- Edit targeting precision: Ratio of focused edits vs broad replacements
- Edit size distribution: Small/medium/large edit classification
- Size-failure correlation: Relationship between edit size and failure

Optimization indicators:
- High success rate: Accurate edit operations with few failures
- Small average lines: Focused, precise edits
- High precision ratio: Targeted edits over broad replacements
- Low size-failure correlation: Large edits don't fail more often
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_edit_operation_precision(records: object) -> dict[str, Any]:
    """Analyze Edit tool operation precision and effectiveness.

    Tracks edit success rate, measures edit size, calculates targeting
    precision, and identifies correlation between edit size and failure.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_edits: Total number of edit operations
            - successful_edits: Number of successful edits
            - failed_edits: Number of failed edits
            - total_lines_changed: Total lines modified across all edits
            - small_edits: Count of edits < 10 lines
            - medium_edits: Count of edits 10-50 lines
            - large_edits: Count of edits > 50 lines
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_edit_success_rate: Average success rate across packs
            - avg_lines_per_edit: Average lines changed per edit
            - avg_precision_ratio: Average focused edits percentage
            - small_edit_percentage: Percentage of small edits
            - medium_edit_percentage: Percentage of medium edits
            - large_edit_percentage: Percentage of large edits
            - high_precision_packs: Count of packs with >80% small edits
            - low_precision_packs: Count of packs with >50% large edits
            - size_failure_correlation: Correlation between size and failure

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    success_rates: list[float] = []
    lines_per_edit_values: list[float] = []
    precision_ratios: list[float] = []

    small_edit_counts: list[int] = []
    medium_edit_counts: list[int] = []
    large_edit_counts: list[int] = []

    high_precision_packs = 0  # > 80% small edits
    low_precision_packs = 0   # > 50% large edits

    # For correlation analysis
    avg_edit_sizes: list[float] = []
    failure_rates: list[float] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_edits = _extract_int(record.get("total_edits"))
        successful_edits = _extract_int(record.get("successful_edits"))
        failed_edits = _extract_int(record.get("failed_edits"))
        total_lines_changed = _extract_int(record.get("total_lines_changed"))
        small_edits = _extract_int(record.get("small_edits"))
        medium_edits = _extract_int(record.get("medium_edits"))
        large_edits = _extract_int(record.get("large_edits"))

        total_packs += 1

        # Calculate success rate
        if total_edits is not None and total_edits > 0:
            if successful_edits is not None:
                success_rate = _percentage(successful_edits, total_edits)
                success_rates.append(success_rate)

            # Calculate failure rate for correlation
            if failed_edits is not None:
                failure_rate = _percentage(failed_edits, total_edits)
                failure_rates.append(failure_rate)

            # Calculate lines per edit
            if total_lines_changed is not None:
                lines_per_edit = total_lines_changed / total_edits
                lines_per_edit_values.append(lines_per_edit)
                avg_edit_sizes.append(lines_per_edit)

            # Calculate precision ratio (small edits / total edits)
            if small_edits is not None:
                precision = _percentage(small_edits, total_edits)
                precision_ratios.append(precision)

                if precision > 80.0:
                    high_precision_packs += 1

            # Check for low precision (large edits dominate)
            if large_edits is not None:
                large_pct = _percentage(large_edits, total_edits)
                if large_pct > 50.0:
                    low_precision_packs += 1

        # Track edit size distribution
        if small_edits is not None:
            small_edit_counts.append(small_edits)
        if medium_edits is not None:
            medium_edit_counts.append(medium_edits)
        if large_edits is not None:
            large_edit_counts.append(large_edits)

    # Calculate metrics
    avg_success_rate = _average(success_rates)
    avg_lines_per_edit = _average(lines_per_edit_values)
    avg_precision_ratio = _average(precision_ratios)

    # Calculate edit size distribution
    total_small = sum(small_edit_counts)
    total_medium = sum(medium_edit_counts)
    total_large = sum(large_edit_counts)
    total_edits_all = total_small + total_medium + total_large

    small_edit_pct = _percentage(total_small, total_edits_all)
    medium_edit_pct = _percentage(total_medium, total_edits_all)
    large_edit_pct = _percentage(total_large, total_edits_all)

    # Calculate size-failure correlation
    correlation = _calculate_correlation(avg_edit_sizes, failure_rates)

    return {
        "total_packs": total_packs,
        "avg_edit_success_rate": avg_success_rate,
        "avg_lines_per_edit": avg_lines_per_edit,
        "avg_precision_ratio": avg_precision_ratio,
        "small_edit_percentage": small_edit_pct,
        "medium_edit_percentage": medium_edit_pct,
        "large_edit_percentage": large_edit_pct,
        "high_precision_packs": high_precision_packs,
        "low_precision_packs": low_precision_packs,
        "size_failure_correlation": correlation,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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


def _calculate_correlation(x_values: list[float], y_values: list[float]) -> float:
    """Calculate Pearson correlation coefficient.

    Returns correlation between -1.0 (negative) and 1.0 (positive).
    Returns 0.0 if insufficient data or no variance.
    """
    if not x_values or not y_values or len(x_values) != len(y_values):
        return 0.0

    n = len(x_values)
    if n < 2:
        return 0.0

    # Calculate means
    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    # Calculate covariance and standard deviations
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    std_x = (sum((x - mean_x) ** 2 for x in x_values)) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in y_values)) ** 0.5

    # Avoid division by zero
    if std_x == 0 or std_y == 0:
        return 0.0

    correlation = covariance / (std_x * std_y)
    return round(correlation, 3)
