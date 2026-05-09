"""Pack final verification coverage analyzer for verification completeness.

Analyzes final verification step completeness across execution packs. Measures
verification coverage (files verified vs files changed), verification depth
(unit tests vs integration vs build), verification timing (immediate post-edit
vs end-of-pack), and correlation between verification coverage and pack success.

Verification coverage metrics:
- Verification coverage: Percentage of changed files verified
- Verification depth: Type of verification (unit/integration/build)
- Verification timing: When verification occurs relative to edits
- Coverage-success correlation: Relationship between coverage and success rate
- Complete verification: All changed files verified

Optimization indicators:
- High coverage: Most/all changed files verified
- Appropriate depth: Right level of verification for changes
- End-of-pack timing: Strategic verification after all edits
- Positive coverage-success correlation: More coverage = higher success
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_final_verification_coverage(records: object) -> dict[str, Any]:
    """Analyze final verification step completeness in execution packs.

    Tracks verification coverage, measures depth and timing, and identifies
    correlation between verification coverage and pack success rate.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - files_changed: Number of files changed in pack
            - files_verified: Number of files verified
            - verification_depth: Type (unit/integration/build)
            - verification_timing: When verified (immediate/end)
            - pack_success: Boolean indicating pack success
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_verification_coverage: Average coverage percentage
            - complete_verification_packs: Count of 100% coverage packs
            - partial_verification_packs: Count of 1-99% coverage packs
            - no_verification_packs: Count of 0% coverage packs
            - unit_test_verification_count: Unit test verification count
            - integration_test_verification_count: Integration test count
            - build_verification_count: Build verification count
            - end_of_pack_timing_count: End-of-pack verification count
            - immediate_timing_count: Immediate verification count
            - coverage_success_correlation: Coverage vs success correlation

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    coverage_values: list[float] = []

    complete_verification_packs = 0  # 100% coverage
    partial_verification_packs = 0   # 1-99% coverage
    no_verification_packs = 0        # 0% coverage

    # Verification depth counts
    unit_test_count = 0
    integration_test_count = 0
    build_count = 0

    # Verification timing counts
    end_of_pack_count = 0
    immediate_count = 0

    # For correlation analysis
    coverage_percentages: list[float] = []
    success_indicators: list[float] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        files_changed = _extract_int(record.get("files_changed"))
        files_verified = _extract_int(record.get("files_verified"))
        verification_depth = _string(record.get("verification_depth")).lower()
        verification_timing = _string(record.get("verification_timing")).lower()
        pack_success = record.get("pack_success")

        total_packs += 1

        # Calculate coverage
        if files_changed is not None and files_changed > 0:
            if files_verified is not None:
                coverage = _percentage(files_verified, files_changed)
                coverage_values.append(coverage)

                # Classify coverage
                if coverage >= 100.0:
                    complete_verification_packs += 1
                elif coverage > 0.0:
                    partial_verification_packs += 1
                else:
                    no_verification_packs += 1
            else:
                no_verification_packs += 1
        elif files_changed == 0:
            # No files changed, so no verification needed
            complete_verification_packs += 1

        # Track verification depth
        if "unit" in verification_depth:
            unit_test_count += 1
        if "integration" in verification_depth:
            integration_test_count += 1
        if "build" in verification_depth:
            build_count += 1

        # Track verification timing
        if "end" in verification_timing or "final" in verification_timing:
            end_of_pack_count += 1
        elif "immediate" in verification_timing or "post" in verification_timing:
            immediate_count += 1

        # Track for correlation
        if files_changed is not None and files_verified is not None:
            if files_changed > 0:
                cov_pct = (files_verified / files_changed) * 100.0
                coverage_percentages.append(cov_pct)

                # Success indicator: 1.0 for success, 0.0 for failure
                if isinstance(pack_success, bool):
                    success_indicators.append(1.0 if pack_success else 0.0)

    # Calculate metrics
    avg_coverage = _average(coverage_values)

    # Calculate coverage-success correlation
    correlation = _calculate_correlation(coverage_percentages, success_indicators)

    return {
        "total_packs": total_packs,
        "avg_verification_coverage": avg_coverage,
        "complete_verification_packs": complete_verification_packs,
        "partial_verification_packs": partial_verification_packs,
        "no_verification_packs": no_verification_packs,
        "unit_test_verification_count": unit_test_count,
        "integration_test_verification_count": integration_test_count,
        "build_verification_count": build_count,
        "end_of_pack_timing_count": end_of_pack_count,
        "immediate_timing_count": immediate_count,
        "coverage_success_correlation": correlation,
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
