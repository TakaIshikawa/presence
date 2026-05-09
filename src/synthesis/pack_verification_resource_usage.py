"""Pack verification resource usage analyzer.

Analyzes verification resource consumption and efficiency patterns in execution packs.
Tracks resource usage per verification (time, memory, CPU), calculates efficiency ratios,
and reports statistics on resource utilization distribution.

Resource usage metrics:
- Verification cost per task: Time/resources spent per verification
- Resource efficiency ratio: Output value vs resource consumed
- Utilization distribution: How resources are distributed across verifications
- Optimization opportunities: Inefficient verification patterns
- Pack-level resource efficiency: Overall resource effectiveness

Quality indicators:
- Low cost per task (<30s): Quick verification execution
- High efficiency ratio (>0.8): Good value from resources used
- Balanced utilization: Even resource distribution across tasks
- Few optimization opportunities: Minimal inefficient patterns
- High pack efficiency: Effective overall resource usage
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_verification_resource_usage(records: object) -> dict[str, Any]:
    """Analyze verification resource consumption and efficiency in execution packs.

    Tracks resource usage per verification and calculates efficiency metrics.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_verifications: Total verification commands in pack
            - total_verification_time_seconds: Total time spent on verification
            - total_verification_memory_mb: Total memory used (MB)
            - total_verification_cpu_seconds: Total CPU time used
            - successful_verifications: Number of successful verifications
            - failed_verifications: Number of failed verifications
            - avg_verification_time_seconds: Average time per verification
            - inefficient_verifications: Count of inefficient verifications
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_verifications_per_pack: Average verifications per pack
            - avg_verification_cost_seconds: Average time per verification
            - avg_verification_memory_mb: Average memory per pack
            - avg_verification_cpu_seconds: Average CPU time per pack
            - avg_efficiency_ratio: Average resource efficiency
            - low_cost_packs: Count of packs with <30s avg verification time
            - high_cost_packs: Count of packs with >120s avg verification time
            - high_efficiency_packs: Count with efficiency >0.8
            - low_efficiency_packs: Count with efficiency <0.5
            - total_optimization_opportunities: Total inefficient verifications
            - avg_success_rate: Average verification success rate

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    verifications_per_pack: list[int | float] = []
    verification_costs: list[float] = []
    memory_usages: list[float] = []
    cpu_usages: list[float] = []
    efficiency_ratios: list[float] = []
    success_rates: list[float] = []

    low_cost_packs = 0  # < 30s avg
    high_cost_packs = 0  # > 120s avg
    high_efficiency_packs = 0  # > 0.8 efficiency
    low_efficiency_packs = 0   # < 0.5 efficiency

    total_optimization_opportunities = 0

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_verifications = _extract_number(record.get("total_verifications"))
        total_time = _extract_number(record.get("total_verification_time_seconds"))
        total_memory = _extract_number(record.get("total_verification_memory_mb"))
        total_cpu = _extract_number(record.get("total_verification_cpu_seconds"))
        successful = _extract_number(record.get("successful_verifications"))
        failed = _extract_number(record.get("failed_verifications"))
        avg_time = _extract_number(record.get("avg_verification_time_seconds"))
        inefficient_count = _extract_number(record.get("inefficient_verifications"))

        total_packs += 1

        # Track verifications per pack
        if total_verifications is not None:
            verifications_per_pack.append(total_verifications)

            # Calculate average verification cost
            if avg_time is not None:
                verification_costs.append(avg_time)

                if avg_time < 30:
                    low_cost_packs += 1
                elif avg_time > 120:
                    high_cost_packs += 1

        # Track memory usage
        if total_memory is not None:
            memory_usages.append(total_memory)

        # Track CPU usage
        if total_cpu is not None:
            cpu_usages.append(total_cpu)

        # Calculate efficiency ratio (successful / total)
        if successful is not None and total_verifications is not None and total_verifications > 0:
            efficiency = successful / total_verifications
            efficiency_ratios.append(efficiency)

            if efficiency > 0.8:
                high_efficiency_packs += 1
            elif efficiency < 0.5:
                low_efficiency_packs += 1

        # Calculate success rate
        if successful is not None and failed is not None:
            total_attempts = successful + failed
            if total_attempts > 0:
                success_rate = (successful / total_attempts) * 100
                success_rates.append(success_rate)

        # Track optimization opportunities
        if inefficient_count is not None:
            total_optimization_opportunities += int(inefficient_count)

    # Calculate aggregate metrics
    avg_verifications_per_pack = _average(verifications_per_pack)
    avg_verification_cost = _average(verification_costs)
    avg_memory = _average(memory_usages)
    avg_cpu = _average(cpu_usages)
    avg_efficiency = _average(efficiency_ratios)
    avg_success_rate = _average(success_rates)

    return {
        "total_packs": total_packs,
        "avg_verifications_per_pack": avg_verifications_per_pack,
        "avg_verification_cost_seconds": avg_verification_cost,
        "avg_verification_memory_mb": avg_memory,
        "avg_verification_cpu_seconds": avg_cpu,
        "avg_efficiency_ratio": avg_efficiency,
        "low_cost_packs": low_cost_packs,
        "high_cost_packs": high_cost_packs,
        "high_efficiency_packs": high_efficiency_packs,
        "low_efficiency_packs": low_efficiency_packs,
        "total_optimization_opportunities": total_optimization_opportunities,
        "avg_success_rate": avg_success_rate,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
