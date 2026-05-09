"""Pack error recovery pattern analyzer.

Analyzes how well execution packs handle and recover from errors. Tracks error
occurrences (build failures, test failures, type errors, runtime errors), recovery
attempts, detection speed, and success rates. Measures resilience based on error
handling effectiveness and graceful degradation.

Error recovery metrics:
- Error detection speed: Turns until error acknowledged
- Recovery attempts: Count of recovery strategies used
- Recovery success rate: Ratio of resolved to total errors
- Average recovery time: Mean turns spent resolving errors
- Graceful degradation: Handling of partial failures

Quality indicators:
- High recovery success ratio (>80%): Most errors successfully resolved
- Fast detection speed (<2 turns): Errors quickly acknowledged
- Low average recovery time (<3 turns): Quick resolution
- High resilience score (>0.8): Strong overall error handling
- Effective recovery strategies: Use of verification tools and targeted reads
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_error_recovery(records: object) -> dict[str, Any]:
    """Analyze error recovery patterns in execution packs.

    Evaluates how effectively packs detect, handle, and recover from errors.

    Args:
        records: List of error event dictionaries with keys:
            - error_id: Error identifier
            - error_type: Type of error (build, test, type, runtime)
            - detected_at_turn: Turn number when error detected
            - acknowledged_at_turn: Turn when error acknowledged
            - resolved_at_turn: Turn when error resolved (None if unresolved)
            - recovery_attempts: Number of recovery attempts
            - recovery_strategy: Strategy used (targeted_read, verify, ask, etc.)
            - was_successful: Boolean indicating successful resolution
            - turns_in_error_state: Total turns spent on this error
            - is_cascading_error: Boolean indicating error caused by another
            - partial_failure_handled: Boolean for graceful degradation

    Returns:
        Dict with:
            - total_errors: Total number of errors encountered
            - errors_resolved: Count of successfully resolved errors
            - errors_unresolved: Count of unresolved errors
            - recovery_success_ratio: Percentage of errors resolved
            - avg_detection_speed: Average turns until error acknowledged
            - avg_recovery_turns: Average turns to resolve errors
            - total_recovery_attempts: Sum of all recovery attempts
            - avg_attempts_per_error: Average recovery attempts per error
            - cascading_errors: Count of errors caused by other errors
            - graceful_degradation_score: Percentage handling partial failures
            - resilience_score: Overall error handling score (0.0-1.0)
            - fast_detections: Errors detected within 2 turns
            - slow_detections: Errors taking >3 turns to detect
            - recovery_strategies_used: Count of different strategies
            - errors_by_type: Dict of error counts by type

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of error event dictionaries")

    if not records:
        return _empty_result()

    total_errors = 0
    errors_resolved = 0
    errors_unresolved = 0
    detection_speeds: list[int | float] = []
    recovery_turns: list[int | float] = []
    total_attempts = 0
    attempts_per_error: list[int | float] = []
    cascading_errors = 0
    partial_failures_handled = 0
    partial_failures_total = 0
    fast_detections = 0
    slow_detections = 0
    strategies_used: set[str] = set()
    errors_by_type: dict[str, int] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_errors += 1

        error_type = record.get("error_type")
        detected_at = _extract_number(record.get("detected_at_turn"))
        acknowledged_at = _extract_number(record.get("acknowledged_at_turn"))
        resolved_at = _extract_number(record.get("resolved_at_turn"))
        attempts = _extract_number(record.get("recovery_attempts"))
        strategy = record.get("recovery_strategy")
        was_successful = record.get("was_successful")
        turns_in_error = _extract_number(record.get("turns_in_error_state"))
        is_cascading = record.get("is_cascading_error")
        partial_handled = record.get("partial_failure_handled")

        # Track error types
        if isinstance(error_type, str):
            errors_by_type[error_type] = errors_by_type.get(error_type, 0) + 1

        # Track resolution status
        if was_successful is True:
            errors_resolved += 1
        elif was_successful is False:
            errors_unresolved += 1

        # Track detection speed (turns from detection to acknowledgment)
        if detected_at is not None and acknowledged_at is not None:
            speed = int(acknowledged_at) - int(detected_at)
            detection_speeds.append(speed)

            # Categorize detection speed
            if speed <= 2:
                fast_detections += 1
            elif speed > 3:
                slow_detections += 1

        # Track recovery time (turns from acknowledgment to resolution)
        if acknowledged_at is not None and resolved_at is not None:
            recovery_time = int(resolved_at) - int(acknowledged_at)
            recovery_turns.append(recovery_time)
        elif turns_in_error is not None:
            # Use total error time if specific resolution not tracked
            recovery_turns.append(int(turns_in_error))

        # Track recovery attempts
        if attempts is not None:
            attempt_count = int(attempts)
            total_attempts += attempt_count
            attempts_per_error.append(attempt_count)

        # Track recovery strategies
        if isinstance(strategy, str) and strategy:
            strategies_used.add(strategy)

        # Track cascading errors
        if is_cascading is True:
            cascading_errors += 1

        # Track graceful degradation (partial failure handling)
        if partial_handled is not None:
            partial_failures_total += 1
            if partial_handled is True:
                partial_failures_handled += 1

    # Calculate aggregate metrics
    recovery_success = _percentage(errors_resolved, total_errors)
    avg_detection = _average(detection_speeds)
    avg_recovery = _average(recovery_turns)
    avg_attempts = _average(attempts_per_error)
    degradation_score = _percentage(partial_failures_handled, partial_failures_total)

    # Calculate resilience score (0.0-1.0)
    # Components: success ratio, fast detection, low recovery time, graceful degradation
    resilience = _calculate_resilience_score(
        recovery_success_ratio=recovery_success,
        avg_detection_speed=avg_detection,
        avg_recovery_turns=avg_recovery,
        degradation_score=degradation_score,
        total_errors=total_errors,
    )

    return {
        "total_errors": total_errors,
        "errors_resolved": errors_resolved,
        "errors_unresolved": errors_unresolved,
        "recovery_success_ratio": recovery_success,
        "avg_detection_speed": avg_detection,
        "avg_recovery_turns": avg_recovery,
        "total_recovery_attempts": total_attempts,
        "avg_attempts_per_error": avg_attempts,
        "cascading_errors": cascading_errors,
        "graceful_degradation_score": degradation_score,
        "resilience_score": resilience,
        "fast_detections": fast_detections,
        "slow_detections": slow_detections,
        "recovery_strategies_used": len(strategies_used),
        "errors_by_type": errors_by_type,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_errors": 0,
        "errors_resolved": 0,
        "errors_unresolved": 0,
        "recovery_success_ratio": 0.0,
        "avg_detection_speed": 0.0,
        "avg_recovery_turns": 0.0,
        "total_recovery_attempts": 0,
        "avg_attempts_per_error": 0.0,
        "cascading_errors": 0,
        "graceful_degradation_score": 0.0,
        "resilience_score": 1.0,  # Perfect resilience when no errors
        "fast_detections": 0,
        "slow_detections": 0,
        "recovery_strategies_used": 0,
        "errors_by_type": {},
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


def _calculate_resilience_score(
    recovery_success_ratio: float,
    avg_detection_speed: float,
    avg_recovery_turns: float,
    degradation_score: float,
    total_errors: int,
) -> float:
    """Calculate overall resilience score (0.0-1.0).

    Components weighted by importance:
    - 40% recovery success ratio (most important)
    - 20% detection speed (fast detection is critical)
    - 20% recovery time (efficient resolution)
    - 20% graceful degradation (handling partial failures)

    Args:
        recovery_success_ratio: Percentage of errors resolved (0-100)
        avg_detection_speed: Average turns to detect (lower is better)
        avg_recovery_turns: Average turns to recover (lower is better)
        degradation_score: Percentage of partial failures handled (0-100)
        total_errors: Total number of errors for context

    Returns:
        Resilience score normalized to 0.0-1.0 range
    """
    if total_errors == 0:
        # No errors encountered = perfect resilience
        return 1.0

    # Normalize success ratio (0-100 -> 0-1)
    success_component = recovery_success_ratio / 100.0

    # Normalize detection speed (fast detection = high score)
    # Target: <=1 turn is excellent (1.0), <=2 turns is very good (0.95), 2-3 turns is good (0.8), >5 turns is poor (0.2)
    if avg_detection_speed <= 0:
        detection_component = 1.0
    elif avg_detection_speed <= 1:
        detection_component = 1.0
    elif avg_detection_speed <= 2:
        detection_component = 0.95
    elif avg_detection_speed <= 3:
        detection_component = 0.8
    elif avg_detection_speed <= 5:
        detection_component = 0.5
    else:
        detection_component = max(0.2, 1.0 - (avg_detection_speed - 1) * 0.1)

    # Normalize recovery time (fast recovery = high score)
    # Target: <3 turns is excellent (1.0), 3-5 turns is good (0.8), >8 turns is poor (0.2)
    if avg_recovery_turns <= 0:
        recovery_component = 1.0
    elif avg_recovery_turns <= 3:
        recovery_component = 1.0
    elif avg_recovery_turns <= 5:
        recovery_component = 0.8
    elif avg_recovery_turns <= 8:
        recovery_component = 0.5
    else:
        recovery_component = max(0.2, 1.0 - (avg_recovery_turns - 3) * 0.1)

    # Normalize degradation score (0-100 -> 0-1)
    degradation_component = degradation_score / 100.0

    # Weighted combination
    resilience = (
        0.4 * success_component
        + 0.2 * detection_component
        + 0.2 * recovery_component
        + 0.2 * degradation_component
    )

    return round(resilience, 2)
