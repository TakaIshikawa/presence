"""Session error message comprehension and fix accuracy analyzer.

Dimensions: error comprehension, fix accuracy, root cause identification,
error context gathering, error propagation.
"""

from collections.abc import Mapping
from typing import Any


def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def analyze_session_error_comprehension(records: object) -> dict[str, Any]:
    """Analyze session error message comprehension and fix accuracy."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    sessions: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        sessions.append(record)

    total_sessions = len(sessions)
    total_errors_encountered = 0
    errors_with_context_read = 0
    first_fix_success_count = 0
    total_fix_attempts = 0
    targeted_reads_for_errors = 0
    full_rereads_for_errors = 0
    error_suppression_count = 0
    cascading_errors = 0
    session_scores: list[float] = []

    for session in sessions:
        s_total_errors = _int(session.get("total_errors_encountered"))
        s_context_read = _int(session.get("errors_with_context_read"))
        s_first_fix = _int(session.get("first_fix_success_count"))
        s_fix_attempts = _int(session.get("total_fix_attempts"))
        s_targeted = _int(session.get("targeted_reads_for_errors"))
        s_full = _int(session.get("full_rereads_for_errors"))
        s_suppressions = _int(session.get("error_suppressions"))
        s_cascading = _int(session.get("cascading_errors"))

        total_errors_encountered += s_total_errors
        errors_with_context_read += s_context_read
        first_fix_success_count += s_first_fix
        total_fix_attempts += s_fix_attempts
        targeted_reads_for_errors += s_targeted
        full_rereads_for_errors += s_full
        error_suppression_count += s_suppressions
        cascading_errors += s_cascading

        # Session score components
        score = 0.0

        # Context read rate (0-0.30)
        if s_total_errors > 0:
            ctx_rate = s_context_read / s_total_errors
            if ctx_rate > 0.8:
                score += 0.30
            else:
                score += 0.30 * (ctx_rate / 0.8)
        else:
            score += 0.30

        # First fix success rate (0-0.30)
        if s_fix_attempts > 0:
            fix_rate = s_first_fix / s_fix_attempts
            if fix_rate > 0.7:
                score += 0.30
            else:
                score += 0.30 * (fix_rate / 0.7)
        else:
            score += 0.30

        # Low error suppression (0-0.20)
        if s_suppressions == 0:
            score += 0.20
        else:
            score += 0.0

        # Targeted reads (0-0.20)
        total_reads = s_targeted + s_full
        if total_reads > 0:
            targeted_rate = s_targeted / total_reads
            if targeted_rate > 0.6:
                score += 0.20
            else:
                score += 0.20 * (targeted_rate / 0.6)
        else:
            score += 0.20

        session_scores.append(round(score, 4))

    # Aggregate rates
    context_read_rate = _percentage(errors_with_context_read, total_errors_encountered)
    first_fix_success_rate = _percentage(first_fix_success_count, total_fix_attempts)
    avg_fix_attempts = round(total_fix_attempts / total_errors_encountered, 2) if total_errors_encountered > 0 else 0.0
    total_error_reads = targeted_reads_for_errors + full_rereads_for_errors
    targeted_read_rate = _percentage(targeted_reads_for_errors, total_error_reads)
    full_reread_rate = _percentage(full_rereads_for_errors, total_error_reads)
    cascading_error_rate = _percentage(cascading_errors, total_errors_encountered)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    # Overall error comprehension score (0-1)
    overall_score = 0.0
    # Context read rate component (0-0.30)
    agg_ctx_rate = (errors_with_context_read / total_errors_encountered) if total_errors_encountered > 0 else 1.0
    if agg_ctx_rate > 0.8:
        overall_score += 0.30
    else:
        overall_score += 0.30 * (agg_ctx_rate / 0.8)

    # First fix success rate component (0-0.30)
    agg_fix_rate = (first_fix_success_count / total_fix_attempts) if total_fix_attempts > 0 else 1.0
    if agg_fix_rate > 0.7:
        overall_score += 0.30
    else:
        overall_score += 0.30 * (agg_fix_rate / 0.7)

    # Low error suppression component (0-0.20)
    if error_suppression_count == 0:
        overall_score += 0.20
    else:
        overall_score += 0.0

    # Targeted reads component (0-0.20)
    if total_error_reads > 0:
        agg_targeted_rate = targeted_reads_for_errors / total_error_reads
        if agg_targeted_rate > 0.6:
            overall_score += 0.20
        else:
            overall_score += 0.20 * (agg_targeted_rate / 0.6)
    else:
        overall_score += 0.20

    overall_score = round(overall_score, 4)

    return {
        "total_sessions": total_sessions,
        "total_errors_encountered": total_errors_encountered,
        "errors_with_context_read": errors_with_context_read,
        "context_read_rate": context_read_rate,
        "first_fix_success_rate": first_fix_success_rate,
        "avg_fix_attempts_per_error": avg_fix_attempts,
        "targeted_read_for_errors_rate": targeted_read_rate,
        "full_reread_for_errors_rate": full_reread_rate,
        "error_suppression_count": error_suppression_count,
        "cascading_error_rate": cascading_error_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "error_comprehension_score": overall_score,
    }
