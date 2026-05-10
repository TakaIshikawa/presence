"""Pack error recovery strategy and retry discipline analyzer.

Analyzes execution pack transcripts for error handling patterns, measuring
error recovery strategies, retry discipline, targeted fixes, verification tool
usage, and error resolution effectiveness.

Error recovery dimensions:
1. Error occurrence and resolution:
   - Error rate across pack
   - Resolution rate (errors fixed vs abandoned)
   - Average turns from error to resolution

2. Recovery strategies:
   - Targeted fixes (Read with offset/limit around error line)
   - Blind retries (immediate retry without diagnostic reads)
   - Verification escalation (/verify usage after errors)
   - Manual retry patterns

3. Error sources:
   - Test failures
   - Build errors
   - Type errors
   - Runtime errors
   - Tool validation failures

4. Anti-patterns:
   - Blind retries without reading error context
   - Full-file re-reads after single-line errors
   - Repeated failures without strategy change
   - Unacknowledged errors (continuing without addressing)

Quality indicators:
- High targeted recovery rate (>60%): Reading error context before fix
- Low blind retry rate (<20%): Diagnostic reads before retry
- High resolution rate (>85%): Most errors get fixed
- Appropriate verification usage (10-30%): Using /verify for complex errors
- Fast resolution (<5 turns average): Efficient error fixing
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_error_recovery_strategy(records: object) -> dict[str, Any]:
    """Analyze error recovery strategies across pack transcripts.

    Args:
        records: List of session dictionaries with keys:
            - total_errors: Total tool call failures and errors
            - errors_resolved: Errors successfully fixed
            - errors_abandoned: Errors left unresolved
            - targeted_recovery_count: Errors followed by Read(offset/limit)
            - blind_retry_count: Immediate retries without diagnostic reads
            - verification_escalation_count: Errors triggering /verify
            - full_file_reread_after_error: Full reads after single-line errors
            - avg_turns_to_resolution: Average turns from error to fix
            - test_errors: Test failure count
            - build_errors: Build/compile error count
            - type_errors: Type checking error count
            - runtime_errors: Runtime error count
            - tool_validation_errors: Tool parameter validation errors
            - repeated_failures_count: Same error occurring multiple times
            - unacknowledged_errors: Errors not addressed in next turn

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - sessions_with_errors: Sessions encountering errors
            - total_errors: Total errors across pack
            - error_resolution_rate: % errors successfully fixed
            - targeted_recovery_rate: % errors with targeted diagnostic reads
            - blind_retry_rate: % errors with blind retries
            - verification_usage_rate: % errors escalated to /verify
            - full_reread_after_error_rate: % single-line errors with full reads
            - avg_turns_to_resolution: Average turns from error to fix
            - unacknowledged_error_rate: % errors ignored
            - test_error_count: Test failures
            - build_error_count: Build/compile errors
            - type_error_count: Type checking errors
            - runtime_error_count: Runtime errors
            - tool_validation_error_count: Tool validation errors
            - repeated_failure_rate: % errors repeating
            - recovery_effectiveness_score: Overall recovery score 0-1
            - high_effectiveness_sessions: Count with score >0.8
            - low_effectiveness_sessions: Count with score <0.5

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    sessions_with_errors = 0
    total_errors = 0
    errors_resolved = 0
    errors_abandoned = 0
    targeted_recovery = 0
    blind_retry = 0
    verification_escalation = 0
    full_reread_after_error = 0
    unacknowledged_errors = 0
    test_errors = 0
    build_errors = 0
    type_errors = 0
    runtime_errors = 0
    tool_validation_errors = 0
    repeated_failures = 0

    turns_to_resolution: list[int | float] = []
    session_effectiveness_scores: list[int | float] = []

    high_effectiveness_sessions = 0  # >0.8 score
    low_effectiveness_sessions = 0   # <0.5 score

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        session_errors = _int(record.get("total_errors", 0))
        resolved = _int(record.get("errors_resolved", 0))
        abandoned = _int(record.get("errors_abandoned", 0))
        targeted = _int(record.get("targeted_recovery_count", 0))
        blind = _int(record.get("blind_retry_count", 0))
        verify = _int(record.get("verification_escalation_count", 0))
        full_reread = _int(record.get("full_file_reread_after_error", 0))
        avg_turns = _float(record.get("avg_turns_to_resolution", 0.0))
        test_err = _int(record.get("test_errors", 0))
        build_err = _int(record.get("build_errors", 0))
        type_err = _int(record.get("type_errors", 0))
        runtime_err = _int(record.get("runtime_errors", 0))
        tool_err = _int(record.get("tool_validation_errors", 0))
        repeated = _int(record.get("repeated_failures_count", 0))
        unack = _int(record.get("unacknowledged_errors", 0))

        if session_errors > 0:
            sessions_with_errors += 1

        total_errors += session_errors
        errors_resolved += resolved
        errors_abandoned += abandoned
        targeted_recovery += targeted
        blind_retry += blind
        verification_escalation += verify
        full_reread_after_error += full_reread
        unacknowledged_errors += unack
        test_errors += test_err
        build_errors += build_err
        type_errors += type_err
        runtime_errors += runtime_err
        tool_validation_errors += tool_err
        repeated_failures += repeated

        if avg_turns > 0:
            turns_to_resolution.append(avg_turns)

        # Calculate session-level effectiveness score
        session_score = _calculate_session_effectiveness_score(
            total_errors=session_errors,
            errors_resolved=resolved,
            targeted_recovery=targeted,
            blind_retry=blind,
            verification_escalation=verify,
            avg_turns_to_resolution=avg_turns,
            unacknowledged_errors=unack,
        )
        session_effectiveness_scores.append(session_score)

        if session_score > 0.8:
            high_effectiveness_sessions += 1
        elif session_score < 0.5:
            low_effectiveness_sessions += 1

    # Calculate pack-level rates
    error_resolution_rate = _percentage(errors_resolved, total_errors)
    targeted_recovery_rate = _percentage(targeted_recovery, total_errors)
    blind_retry_rate = _percentage(blind_retry, total_errors)
    verification_usage_rate = _percentage(verification_escalation, total_errors)
    full_reread_rate = _percentage(full_reread_after_error, total_errors)
    unacknowledged_error_rate = _percentage(unacknowledged_errors, total_errors)
    repeated_failure_rate = _percentage(repeated_failures, total_errors)

    # Calculate averages
    avg_turns_to_resolution = _average(turns_to_resolution)

    # Calculate overall effectiveness score
    recovery_effectiveness_score = _calculate_pack_effectiveness_score(
        error_resolution_rate=error_resolution_rate,
        targeted_recovery_rate=targeted_recovery_rate,
        blind_retry_rate=blind_retry_rate,
        verification_usage_rate=verification_usage_rate,
        avg_turns_to_resolution=avg_turns_to_resolution,
        unacknowledged_error_rate=unacknowledged_error_rate,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_with_errors": sessions_with_errors,
        "total_errors": total_errors,
        "error_resolution_rate": error_resolution_rate,
        "targeted_recovery_rate": targeted_recovery_rate,
        "blind_retry_rate": blind_retry_rate,
        "verification_usage_rate": verification_usage_rate,
        "full_reread_after_error_rate": full_reread_rate,
        "avg_turns_to_resolution": avg_turns_to_resolution,
        "unacknowledged_error_rate": unacknowledged_error_rate,
        "test_error_count": test_errors,
        "build_error_count": build_errors,
        "type_error_count": type_errors,
        "runtime_error_count": runtime_errors,
        "tool_validation_error_count": tool_validation_errors,
        "repeated_failure_rate": repeated_failure_rate,
        "recovery_effectiveness_score": recovery_effectiveness_score,
        "high_effectiveness_sessions": high_effectiveness_sessions,
        "low_effectiveness_sessions": low_effectiveness_sessions,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "sessions_with_errors": 0,
        "total_errors": 0,
        "error_resolution_rate": 0.0,
        "targeted_recovery_rate": 0.0,
        "blind_retry_rate": 0.0,
        "verification_usage_rate": 0.0,
        "full_reread_after_error_rate": 0.0,
        "avg_turns_to_resolution": 0.0,
        "unacknowledged_error_rate": 0.0,
        "test_error_count": 0,
        "build_error_count": 0,
        "type_error_count": 0,
        "runtime_error_count": 0,
        "tool_validation_error_count": 0,
        "repeated_failure_rate": 0.0,
        "recovery_effectiveness_score": 0.0,
        "high_effectiveness_sessions": 0,
        "low_effectiveness_sessions": 0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_session_effectiveness_score(
    total_errors: int,
    errors_resolved: int,
    targeted_recovery: int,
    blind_retry: int,
    verification_escalation: int,  # noqa: ARG001
    avg_turns_to_resolution: float,
    unacknowledged_errors: int,
) -> float:
    """Calculate session-level error recovery effectiveness score (0-1).

    Scoring components:
    - Error resolution rate (0-0.35)
    - Targeted recovery rate (0-0.30)
    - Low blind retry rate (0-0.15)
    - Fast resolution (<5 turns) (0-0.10)
    - Low unacknowledged rate (0-0.10)

    Returns:
        Session effectiveness score from 0.0 to 1.0
    """
    if total_errors == 0:
        return 1.0  # No errors = perfect score

    score = 0.0

    # Resolution rate component (0-0.35)
    resolution_rate = _percentage(errors_resolved, total_errors)
    if resolution_rate >= 85:
        score += 0.35
    elif resolution_rate >= 70:
        score += 0.25
    elif resolution_rate >= 50:
        score += 0.15

    # Targeted recovery component (0-0.30)
    targeted_rate = _percentage(targeted_recovery, total_errors)
    if targeted_rate >= 60:
        score += 0.30
    elif targeted_rate >= 40:
        score += 0.20
    elif targeted_rate >= 20:
        score += 0.10

    # Blind retry penalty (0-0.15)
    blind_rate = _percentage(blind_retry, total_errors)
    if blind_rate <= 15:
        score += 0.15
    elif blind_rate <= 30:
        score += 0.10
    elif blind_rate <= 50:
        score += 0.05

    # Fast resolution component (0-0.10)
    if avg_turns_to_resolution > 0:
        if avg_turns_to_resolution <= 3:
            score += 0.10
        elif avg_turns_to_resolution <= 5:
            score += 0.07
        elif avg_turns_to_resolution <= 8:
            score += 0.04

    # Low unacknowledged rate (0-0.10)
    unack_rate = _percentage(unacknowledged_errors, total_errors)
    if unack_rate <= 5:
        score += 0.10
    elif unack_rate <= 10:
        score += 0.07
    elif unack_rate <= 20:
        score += 0.04

    return round(score, 3)


def _calculate_pack_effectiveness_score(
    error_resolution_rate: float,
    targeted_recovery_rate: float,
    blind_retry_rate: float,
    verification_usage_rate: float,
    avg_turns_to_resolution: float,
    unacknowledged_error_rate: float,
) -> float:
    """Calculate overall pack error recovery effectiveness score (0-1).

    Scoring components:
    - Error resolution rate (0-0.30): % errors fixed
    - Targeted recovery rate (0-0.25): % using diagnostic reads
    - Low blind retry rate (0-0.15): No blind retries
    - Appropriate verification usage (0-0.10): 10-30% usage
    - Fast resolution (0-0.10): <5 turns average
    - Low unacknowledged rate (0-0.10): <5% ignored

    Returns:
        Pack effectiveness score from 0.0 to 1.0
    """
    score = 0.0

    # Resolution rate component (0-0.30)
    if error_resolution_rate >= 85:
        score += 0.30
    elif error_resolution_rate >= 70:
        score += 0.22
    elif error_resolution_rate >= 50:
        score += 0.15

    # Targeted recovery component (0-0.25)
    if targeted_recovery_rate >= 60:
        score += 0.25
    elif targeted_recovery_rate >= 40:
        score += 0.18
    elif targeted_recovery_rate >= 20:
        score += 0.10

    # Blind retry penalty (0-0.15)
    if blind_retry_rate <= 15:
        score += 0.15
    elif blind_retry_rate <= 30:
        score += 0.10
    elif blind_retry_rate <= 50:
        score += 0.05

    # Verification usage component (0-0.10)
    # Optimal range: 10-30% (using /verify for complex errors, not all)
    if 10 <= verification_usage_rate <= 30:
        score += 0.10
    elif 5 <= verification_usage_rate < 10 or 30 < verification_usage_rate <= 40:
        score += 0.07
    elif verification_usage_rate < 5 or verification_usage_rate > 40:
        score += 0.03

    # Fast resolution component (0-0.10)
    if avg_turns_to_resolution > 0:
        if avg_turns_to_resolution <= 3:
            score += 0.10
        elif avg_turns_to_resolution <= 5:
            score += 0.07
        elif avg_turns_to_resolution <= 8:
            score += 0.04

    # Low unacknowledged rate (0-0.10)
    if unacknowledged_error_rate <= 5:
        score += 0.10
    elif unacknowledged_error_rate <= 10:
        score += 0.07
    elif unacknowledged_error_rate <= 20:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
