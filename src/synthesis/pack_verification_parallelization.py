"""Pack verification parallelization efficiency analyzer.

Analyzes verification parallelization patterns in execution packs. Evaluates
whether verification commands run in parallel vs sequentially, calculates
parallelization ratios, and measures verification time distribution for
resource utilization insights.

Parallelization metrics:
- Parallelization ratio: Percentage of verification commands run in parallel
- Sequential verification ratio: Percentage run sequentially
- Concurrent efficiency: Time savings from parallel execution
- Batch verification patterns: Grouping of verification commands
- Resource utilization: CPU/thread usage during verification

Quality indicators:
- High parallelization (>70%): Most verifications run concurrently
- Efficient batching: Related verifications grouped appropriately
- Balanced resource usage: Good CPU/thread utilization without overload
- Optimal concurrency: Parallel execution where beneficial, sequential where necessary
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_verification_parallelization(records: object) -> dict[str, Any]:
    """Analyze verification parallelization patterns in execution packs.

    Evaluates parallel vs sequential verification strategies, calculates
    parallelization ratios, and measures verification time distribution.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_verifications: Total verification commands in pack
            - parallel_verifications: Number of verifications run in parallel
            - sequential_verifications: Number run sequentially
            - verification_batches: Number of distinct verification batches
            - avg_batch_size: Average number of verifications per batch
            - total_verification_time_seconds: Total time spent on verification
            - parallel_time_seconds: Time spent in parallel execution
            - max_concurrent_verifications: Peak concurrent verification count
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_verifications: Average verifications per pack
            - avg_parallelization_ratio: Average percentage of parallel verifications
            - avg_sequential_ratio: Average percentage of sequential verifications
            - high_parallelization_packs: Count of packs with >70% parallelization
            - low_parallelization_packs: Count of packs with <30% parallelization
            - avg_batch_size: Average verification batch size
            - avg_verification_time: Average total verification time
            - avg_parallel_time_ratio: Percentage of time spent in parallel
            - avg_concurrent_efficiency: Average concurrent verification efficiency
            - total_time_saved_estimate: Estimated time saved by parallelization

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    total_verifications_counts: list[int | float] = []
    parallelization_ratios: list[float] = []
    sequential_ratios: list[float] = []
    batch_sizes: list[float] = []
    verification_times: list[float] = []
    parallel_time_ratios: list[float] = []
    concurrent_efficiencies: list[float] = []

    high_parallelization_packs = 0  # > 70% parallel
    low_parallelization_packs = 0   # < 30% parallel

    total_time_saved = 0.0

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_verifications = _extract_number(record.get("total_verifications"))
        parallel_verifications = _extract_number(record.get("parallel_verifications"))
        sequential_verifications = _extract_number(record.get("sequential_verifications"))
        verification_batches = _extract_number(record.get("verification_batches"))
        avg_batch_size = _extract_number(record.get("avg_batch_size"))
        total_verification_time = _extract_number(record.get("total_verification_time_seconds"))
        parallel_time = _extract_number(record.get("parallel_time_seconds"))
        max_concurrent = _extract_number(record.get("max_concurrent_verifications"))

        total_packs += 1

        # Track total verifications
        if total_verifications is not None:
            total_verifications_counts.append(total_verifications)

            # Calculate parallelization ratios
            if total_verifications > 0:
                if parallel_verifications is not None:
                    parallel_ratio = _percentage(parallel_verifications, total_verifications)
                    parallelization_ratios.append(parallel_ratio)

                    if parallel_ratio > 70.0:
                        high_parallelization_packs += 1
                    elif parallel_ratio < 30.0:
                        low_parallelization_packs += 1

                if sequential_verifications is not None:
                    sequential_ratio = _percentage(sequential_verifications, total_verifications)
                    sequential_ratios.append(sequential_ratio)

        # Track batch sizes
        if avg_batch_size is not None:
            batch_sizes.append(avg_batch_size)

        # Track verification times
        if total_verification_time is not None:
            verification_times.append(total_verification_time)

            if parallel_time is not None:
                parallel_time_ratio = _percentage(parallel_time, total_verification_time)
                parallel_time_ratios.append(parallel_time_ratio)

                # Estimate time saved by parallelization
                # If 4 tasks run in parallel taking 10s total, sequential would take ~40s
                # Time saved = (sequential_estimate - parallel_time)
                if max_concurrent is not None and max_concurrent > 1:
                    sequential_estimate = parallel_time * max_concurrent
                    time_saved = sequential_estimate - parallel_time
                    total_time_saved += time_saved

        # Calculate concurrent efficiency
        if (parallel_verifications is not None and max_concurrent is not None and
                parallel_verifications > 0 and max_concurrent > 0):
            # Efficiency = actual parallel work / theoretical max parallel work
            # Higher is better, 1.0 means perfect utilization
            efficiency = min(parallel_verifications / max_concurrent, 1.0)
            concurrent_efficiencies.append(efficiency * 100.0)

    # Calculate aggregate metrics
    avg_total_verifications = _average(total_verifications_counts)
    avg_parallelization = _average(parallelization_ratios)
    avg_sequential = _average(sequential_ratios)
    avg_batch_size_result = _average(batch_sizes)
    avg_verification_time = _average(verification_times)
    avg_parallel_time_ratio = _average(parallel_time_ratios)
    avg_concurrent_efficiency = _average(concurrent_efficiencies)

    return {
        "total_packs": total_packs,
        "avg_total_verifications": avg_total_verifications,
        "avg_parallelization_ratio": avg_parallelization,
        "avg_sequential_ratio": avg_sequential,
        "high_parallelization_packs": high_parallelization_packs,
        "low_parallelization_packs": low_parallelization_packs,
        "avg_batch_size": avg_batch_size_result,
        "avg_verification_time": avg_verification_time,
        "avg_parallel_time_ratio": avg_parallel_time_ratio,
        "avg_concurrent_efficiency": avg_concurrent_efficiency,
        "total_time_saved_estimate": round(total_time_saved, 2),
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


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
