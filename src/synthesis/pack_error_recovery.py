"""Pack error recovery and retry pattern analyzer.

Analyzes error handling and recovery patterns across execution packs. Tracks error
types, recovery success rates, retry strategies, resolution time, and error clustering
to identify effective vs inefficient error handling discipline.

Error recovery metrics:
- Total errors encountered: All error occurrences across pack
- Error types: Build errors, test failures, runtime errors, tool failures
- Recovery success rate: Percentage of errors successfully resolved
- Retry attempts per error: Average attempts to resolve each error
- Time to resolution: Average time from error to fix
- Error clustering: Same error repeated across multiple tasks
- Abandoned errors: Errors left unresolved
- Recovery efficiency score: 0-100 score (quick resolution, few retries = higher)

Quality indicators:
- High recovery rate (>85%): Most errors successfully fixed
- Low retry attempts (<2.5): Efficient error resolution
- Short resolution time (<120s): Quick fixes
- Low error clustering (<20%): No repeated failures
- Low abandonment rate (<10%): Few unresolved errors
- High efficiency score (>80): Optimal recovery discipline
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_error_recovery(records: object) -> dict[str, Any]:
    """Analyze error handling and recovery patterns in execution packs.

    Tracks error types, recovery effectiveness, and resolution efficiency.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_errors: Total number of errors encountered
            - build_errors: Number of build/compilation errors
            - test_errors: Number of test failure errors
            - runtime_errors: Number of runtime/execution errors
            - tool_errors: Number of tool call failures
            - other_errors: Number of other error types
            - recovered_errors: Number of successfully resolved errors
            - abandoned_errors: Number of unresolved errors
            - total_retry_attempts: Sum of all retry attempts
            - unique_error_signatures: Number of distinct error types
            - repeated_error_count: Errors occurring multiple times
            - total_resolution_time_seconds: Time spent resolving errors
            - avg_retries_per_error: Average retry attempts per error
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_errors: Average errors per pack
            - avg_build_error_ratio: Average % build errors
            - avg_test_error_ratio: Average % test errors
            - avg_runtime_error_ratio: Average % runtime errors
            - avg_tool_error_ratio: Average % tool errors
            - avg_recovery_rate: Average % errors recovered
            - avg_abandonment_rate: Average % errors abandoned
            - avg_retries_per_error: Average retry attempts
            - avg_error_clustering_rate: Average % repeated errors
            - avg_resolution_time: Average seconds to resolve
            - recovery_efficiency_score: Score 0-100 (higher = better)
            - high_recovery_packs: Count with >85% recovery rate
            - low_recovery_packs: Count with <50% recovery rate
            - packs_with_abandoned_errors: Count with abandoned errors
            - efficient_recovery_packs: Count with score >80

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    error_counts: list[int | float] = []
    build_ratios: list[float] = []
    test_ratios: list[float] = []
    runtime_ratios: list[float] = []
    tool_ratios: list[float] = []
    recovery_rates: list[float] = []
    abandonment_rates: list[float] = []
    retries_per_error: list[float] = []
    clustering_rates: list[float] = []
    resolution_times: list[float] = []
    efficiency_scores: list[float] = []

    high_recovery_packs = 0  # >85% recovery
    low_recovery_packs = 0   # <50% recovery
    packs_with_abandoned = 0
    efficient_recovery_packs = 0  # >80 efficiency score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_errors = _extract_number(record.get("total_errors"))
        build_errors = _extract_number(record.get("build_errors"))
        test_errors = _extract_number(record.get("test_errors"))
        runtime_errors = _extract_number(record.get("runtime_errors"))
        tool_errors = _extract_number(record.get("tool_errors"))
        recovered = _extract_number(record.get("recovered_errors"))
        abandoned = _extract_number(record.get("abandoned_errors"))
        retry_attempts = _extract_number(record.get("total_retry_attempts"))
        unique_errors = _extract_number(record.get("unique_error_signatures"))
        repeated_errors = _extract_number(record.get("repeated_error_count"))
        resolution_time = _extract_number(record.get("total_resolution_time_seconds"))
        avg_retries = _extract_number(record.get("avg_retries_per_error"))

        # Track error counts
        if total_errors is not None and total_errors > 0:
            error_counts.append(total_errors)

            # Calculate error type ratios
            if build_errors is not None:
                build_ratios.append(_percentage(build_errors, total_errors))
            if test_errors is not None:
                test_ratios.append(_percentage(test_errors, total_errors))
            if runtime_errors is not None:
                runtime_ratios.append(_percentage(runtime_errors, total_errors))
            if tool_errors is not None:
                tool_ratios.append(_percentage(tool_errors, total_errors))

            # Calculate recovery rate
            recovery_rate = 0.0
            if recovered is not None:
                recovery_rate = _percentage(recovered, total_errors)
                recovery_rates.append(recovery_rate)

                if recovery_rate > 85.0:
                    high_recovery_packs += 1
                elif recovery_rate < 50.0:
                    low_recovery_packs += 1

            # Calculate abandonment rate
            if abandoned is not None:
                abandonment_rates.append(_percentage(abandoned, total_errors))
                if abandoned > 0:
                    packs_with_abandoned += 1

            # Calculate retries per error
            if avg_retries is not None:
                retries_per_error.append(avg_retries)
            elif retry_attempts is not None:
                retries_per_error.append(retry_attempts / total_errors)

            # Calculate error clustering rate
            if repeated_errors is not None and unique_errors is not None and unique_errors > 0:
                clustering_rate = _percentage(repeated_errors, total_errors)
                clustering_rates.append(clustering_rate)

            # Calculate average resolution time
            if resolution_time is not None:
                avg_resolution = resolution_time / total_errors
                resolution_times.append(avg_resolution)

        # Calculate efficiency score
        efficiency_score = _calculate_efficiency_score(
            recovery_rate=recovery_rates[-1] if recovery_rates and len(recovery_rates) > len(efficiency_scores) else None,
            retries=retries_per_error[-1] if retries_per_error and len(retries_per_error) > len(efficiency_scores) else None,
            resolution_time=resolution_times[-1] if resolution_times and len(resolution_times) > len(efficiency_scores) else None,
            clustering=clustering_rates[-1] if clustering_rates and len(clustering_rates) > len(efficiency_scores) else None,
            abandonment=abandonment_rates[-1] if abandonment_rates and len(abandonment_rates) > len(efficiency_scores) else None,
        )
        efficiency_scores.append(efficiency_score)

        if efficiency_score > 80.0:
            efficient_recovery_packs += 1

    # Calculate aggregate metrics
    avg_errors = _average(error_counts)
    avg_build = _average(build_ratios)
    avg_test = _average(test_ratios)
    avg_runtime = _average(runtime_ratios)
    avg_tool = _average(tool_ratios)
    avg_recovery = _average(recovery_rates)
    avg_abandonment = _average(abandonment_rates)
    avg_retries = _average(retries_per_error)
    avg_clustering = _average(clustering_rates)
    avg_resolution = _average(resolution_times)
    avg_efficiency = _average(efficiency_scores)

    return {
        "total_packs": total_packs,
        "avg_total_errors": avg_errors,
        "avg_build_error_ratio": avg_build,
        "avg_test_error_ratio": avg_test,
        "avg_runtime_error_ratio": avg_runtime,
        "avg_tool_error_ratio": avg_tool,
        "avg_recovery_rate": avg_recovery,
        "avg_abandonment_rate": avg_abandonment,
        "avg_retries_per_error": avg_retries,
        "avg_error_clustering_rate": avg_clustering,
        "avg_resolution_time": avg_resolution,
        "recovery_efficiency_score": avg_efficiency,
        "high_recovery_packs": high_recovery_packs,
        "low_recovery_packs": low_recovery_packs,
        "packs_with_abandoned_errors": packs_with_abandoned,
        "efficient_recovery_packs": efficient_recovery_packs,
    }


def _calculate_efficiency_score(
    recovery_rate: float | None,
    retries: float | None,
    resolution_time: float | None,
    clustering: float | None,
    abandonment: float | None,
) -> float:
    """Calculate error recovery efficiency score (0-100).

    Higher scores indicate better efficiency:
    - High recovery rate (>85%)
    - Low retry attempts (<2.5)
    - Short resolution time (<120s)
    - Low error clustering (<20%)
    - Low abandonment rate (<10%)

    Scoring breakdown:
    - Recovery rate: 35 points (85% threshold)
    - Retry efficiency: 25 points (2.5 retries threshold)
    - Resolution speed: 20 points (120s threshold)
    - Error clustering: 10 points (20% threshold)
    - Abandonment rate: 10 points (10% threshold)
    """
    score = 0.0

    # Recovery rate component (35 points)
    if recovery_rate is not None:
        if recovery_rate >= 85:  # >=85% = excellent
            score += 35.0
        elif recovery_rate >= 70:  # >=70% = good
            score += 25.0
        elif recovery_rate >= 55:  # >=55% = acceptable
            score += 15.0
        # <55% = 0 points

    # Retry efficiency component (25 points)
    if retries is not None:
        if retries < 2.5:  # <2.5 = excellent
            score += 25.0
        elif retries < 4.0:  # <4.0 = good
            score += 18.0
        elif retries < 5.5:  # <5.5 = acceptable
            score += 10.0
        # >=5.5 = 0 points

    # Resolution speed component (20 points)
    if resolution_time is not None:
        if resolution_time < 120:  # <2min = excellent
            score += 20.0
        elif resolution_time < 240:  # <4min = good
            score += 15.0
        elif resolution_time < 360:  # <6min = acceptable
            score += 10.0
        # >=6min = 0 points

    # Error clustering component (10 points)
    if clustering is not None:
        if clustering < 20:  # <20% = excellent
            score += 10.0
        elif clustering < 40:  # <40% = good
            score += 7.0
        elif clustering < 60:  # <60% = acceptable
            score += 4.0
        # >=60% = 0 points

    # Abandonment rate component (10 points)
    if abandonment is not None:
        if abandonment < 10:  # <10% = excellent
            score += 10.0
        elif abandonment < 25:  # <25% = good
            score += 7.0
        elif abandonment < 40:  # <40% = acceptable
            score += 4.0
        # >=40% = 0 points

    return round(score, 2)


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
