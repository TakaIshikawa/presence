"""Pack verification timing distribution analyzer.

Analyzes when verification occurs during task execution in execution packs.
Measures timing patterns, frequency, and correlation between verification delay
and fix cycles.

Verification timing metrics:
- Timing pattern classification: immediate post-edit, batch end, error-triggered
- Verification frequency per task: How often verification is performed
- Time between edit and verification: Delay in verification after code changes
- Verification delay average: Mean time from edit to verification
- Fix cycle correlation: Relationship between delay and iteration count

Timing pattern categories:
- Immediate: Verification within 1 minute of edit
- Batched: Verification at end of multiple edits
- Error-triggered: Verification only after errors occur
- Delayed: Verification significantly after edits (>5 minutes)

Quality indicators:
- High immediate verification rate (>60%): Quick feedback loops
- Low verification delay (<2 minutes avg): Timely error detection
- Negative fix cycle correlation: Faster verification = fewer iterations
- Consistent verification frequency: Regular verification across tasks
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping


def analyze_pack_verification_timing_distribution(records: object) -> dict[str, Any]:
    """Analyze verification timing patterns in execution packs.

    Characterizes when verification occurs during task execution and measures
    correlation between verification timing and fix iteration count.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - tasks: List of task dicts with:
                - task_id: Task identifier
                - events: List of event dicts with:
                    - event_type: "edit", "verification", "error"
                    - timestamp: Event timestamp in seconds
                    - details: Additional event information
            - total_verification_count: Total verifications in pack
            - total_edit_count: Total edits in pack

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_verification_per_task: Average verifications per task
            - avg_verification_delay: Average time from edit to verification (seconds)
            - immediate_verification_rate: % of verifications within 1 minute of edit
            - batched_verification_rate: % of verifications after multiple edits
            - error_triggered_rate: % of verifications following errors
            - delayed_verification_rate: % of verifications >5 minutes after edit
            - timing_strategy_distribution: Breakdown by timing pattern
            - avg_fix_iterations: Average fix cycles per task
            - verification_delay_correlation: Correlation with fix iteration count
            - high_immediate_packs: Count with >60% immediate verification
            - delayed_packs: Count with >30% delayed verification

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    if not records:
        return _empty_result()

    total_packs = 0
    verification_per_task_list: list[float] = []
    verification_delays: list[float] = []

    immediate_count = 0
    batched_count = 0
    error_triggered_count = 0
    delayed_count = 0
    total_verifications = 0

    fix_iterations_list: list[int] = []
    delay_iteration_pairs: list[tuple[float, int]] = []

    high_immediate_packs = 0
    delayed_packs = 0

    timing_strategies: defaultdict[str, int] = defaultdict(int)

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        tasks = record.get("tasks")
        if not isinstance(tasks, list):
            continue

        pack_verifications = 0
        pack_immediate = 0
        pack_batched = 0
        pack_error_triggered = 0
        pack_delayed = 0

        for task in tasks:
            if not isinstance(task, Mapping):
                continue

            events = task.get("events")
            if not isinstance(events, list):
                continue

            # Parse events to identify verification timing patterns
            task_edits: list[float] = []
            task_verifications: list[tuple[float, str]] = []
            task_errors: list[float] = []
            fix_iterations = 0

            for event in events:
                if not isinstance(event, Mapping):
                    continue

                event_type = _string(event.get("event_type"))
                timestamp = _float(event.get("timestamp"))

                if event_type == "edit":
                    task_edits.append(timestamp)
                elif event_type == "verification":
                    strategy = _string(event.get("details", {}).get("strategy", "unknown"))
                    task_verifications.append((timestamp, strategy))
                    pack_verifications += 1
                elif event_type == "error":
                    task_errors.append(timestamp)
                    fix_iterations += 1

            # Analyze verification timing for each verification event
            for ver_timestamp, strategy in task_verifications:
                # Find most recent edit before this verification
                recent_edits = [e for e in task_edits if e < ver_timestamp]

                if recent_edits:
                    last_edit = max(recent_edits)
                    delay = ver_timestamp - last_edit
                    verification_delays.append(delay)

                    # Classify timing pattern
                    if delay <= 60:  # Within 1 minute
                        immediate_count += 1
                        pack_immediate += 1
                        timing_strategies["immediate"] += 1
                    elif delay <= 300:  # 1-5 minutes
                        # Check if there were multiple edits (batched)
                        edits_in_window = [e for e in task_edits if last_edit - 300 <= e <= ver_timestamp]
                        if len(edits_in_window) > 1:
                            batched_count += 1
                            pack_batched += 1
                            timing_strategies["batched"] += 1
                        else:
                            delayed_count += 1
                            pack_delayed += 1
                            timing_strategies["delayed"] += 1
                    else:  # >5 minutes
                        delayed_count += 1
                        pack_delayed += 1
                        timing_strategies["delayed"] += 1

                # Check if verification was error-triggered
                recent_errors = [e for e in task_errors if ver_timestamp - e <= 60]
                if recent_errors:
                    error_triggered_count += 1
                    pack_error_triggered += 1
                    timing_strategies["error_triggered"] += 1

            if task_verifications:
                verification_per_task_list.append(len(task_verifications))

            if fix_iterations > 0:
                fix_iterations_list.append(fix_iterations)

            # Correlate delay with fix iterations
            if verification_delays and fix_iterations > 0:
                avg_delay = sum(verification_delays[-len(task_verifications):]) / len(task_verifications)
                delay_iteration_pairs.append((avg_delay, fix_iterations))

        total_verifications += pack_verifications

        # Classify pack timing strategy
        if pack_verifications > 0:
            immediate_rate = (pack_immediate / pack_verifications) * 100
            delayed_rate = (pack_delayed / pack_verifications) * 100

            if immediate_rate > 60:
                high_immediate_packs += 1
            if delayed_rate > 30:
                delayed_packs += 1

    # Calculate aggregate metrics
    avg_verification_per_task = _average([float(v) for v in verification_per_task_list])
    avg_verification_delay = _average(verification_delays)

    immediate_rate = _percentage(immediate_count, total_verifications)
    batched_rate = _percentage(batched_count, total_verifications)
    error_triggered_rate = _percentage(error_triggered_count, total_verifications)
    delayed_rate = _percentage(delayed_count, total_verifications)

    avg_fix_iterations = _average([float(i) for i in fix_iterations_list])

    # Calculate correlation between delay and fix iterations
    correlation = _calculate_correlation_from_pairs(delay_iteration_pairs)

    # Format timing strategy distribution
    total_strategy_count = sum(timing_strategies.values())
    strategy_distribution = [
        {
            "strategy": strategy,
            "count": count,
            "percentage": _percentage(count, total_strategy_count)
        }
        for strategy, count in sorted(
            timing_strategies.items(), key=lambda x: x[1], reverse=True
        )
    ]

    return {
        "total_packs": total_packs,
        "avg_verification_per_task": avg_verification_per_task,
        "avg_verification_delay": avg_verification_delay,
        "immediate_verification_rate": immediate_rate,
        "batched_verification_rate": batched_rate,
        "error_triggered_rate": error_triggered_rate,
        "delayed_verification_rate": delayed_rate,
        "timing_strategy_distribution": strategy_distribution,
        "avg_fix_iterations": avg_fix_iterations,
        "verification_delay_correlation": correlation,
        "high_immediate_packs": high_immediate_packs,
        "delayed_packs": delayed_packs,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_packs": 0,
        "avg_verification_per_task": 0.0,
        "avg_verification_delay": 0.0,
        "immediate_verification_rate": 0.0,
        "batched_verification_rate": 0.0,
        "error_triggered_rate": 0.0,
        "delayed_verification_rate": 0.0,
        "timing_strategy_distribution": [],
        "avg_fix_iterations": 0.0,
        "verification_delay_correlation": 0.0,
        "high_immediate_packs": 0,
        "delayed_packs": 0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace.

    Args:
        value: Value to convert

    Returns:
        String value
    """
    return value.strip() if isinstance(value, str) else ""


def _float(value: object) -> float:
    """Convert value to float.

    Args:
        value: Value to convert

    Returns:
        Float value, or 0.0 if invalid
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage value (0.0-100.0)
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_correlation_from_pairs(pairs: list[tuple[float, int]]) -> float:
    """Calculate correlation coefficient from delay-iteration pairs.

    Negative correlation indicates faster verification leads to fewer iterations.
    Positive correlation indicates delays lead to more iterations.

    Args:
        pairs: List of (delay, iteration_count) tuples

    Returns:
        Correlation coefficient (-1.0 to 1.0), or 0.0 if cannot calculate
    """
    if len(pairs) < 2:
        return 0.0

    # Simple Pearson correlation
    delays = [p[0] for p in pairs]
    iterations = [p[1] for p in pairs]

    n = len(pairs)
    sum_delays = sum(delays)
    sum_iterations = sum(iterations)
    sum_delays_sq = sum(d * d for d in delays)
    sum_iterations_sq = sum(i * i for i in iterations)
    sum_products = sum(d * i for d, i in pairs)

    numerator = n * sum_products - sum_delays * sum_iterations
    denominator_delays = n * sum_delays_sq - sum_delays * sum_delays
    denominator_iterations = n * sum_iterations_sq - sum_iterations * sum_iterations

    if denominator_delays <= 0 or denominator_iterations <= 0:
        return 0.0

    denominator = (denominator_delays * denominator_iterations) ** 0.5

    if denominator == 0:
        return 0.0

    correlation = numerator / denominator
    return round(max(-1.0, min(1.0, correlation)), 2)
