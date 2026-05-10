"""Session tool call error recovery speed analyzer.

Measures how quickly an agent recovers from tool call errors,
including time-to-recovery and actions taken before successful recovery.

Dimensions: error count, recovery rate, actions to recover,
unrecovered errors.
"""

from __future__ import annotations

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


def analyze_session_tool_call_error_recovery_speed(records: object) -> dict[str, Any]:
    """Analyze tool call error recovery speed across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_errors = 0
    agg_recovered = 0
    agg_unrecovered = 0
    agg_immediate_recovery = 0
    all_actions_to_recover: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        errors = _int(record.get("total_tool_errors"))
        recovered = _int(record.get("errors_recovered"))
        unrecovered = _int(record.get("errors_unrecovered"))
        immediate = _int(record.get("immediate_recovery"))

        agg_errors += errors
        agg_recovered += recovered
        agg_unrecovered += unrecovered
        agg_immediate_recovery += immediate

        actions_values = record.get("actions_to_recover_values")
        if isinstance(actions_values, list):
            all_actions_to_recover.extend(actions_values)

        if errors == 0:
            session_scores.append(1.0)
            continue

        # Recovery rate (0-0.40): higher is better
        recovery_ratio = recovered / errors if errors > 0 else 0.0
        recovery_score = min(recovery_ratio / 0.90, 1.0) * 0.40

        # Immediate recovery rate (0-0.25): higher is better
        immediate_ratio = immediate / errors if errors > 0 else 0.0
        immediate_score = min(immediate_ratio / 0.50, 1.0) * 0.25

        # Low actions to recover (0-0.20): fewer actions is faster
        if isinstance(actions_values, list) and actions_values:
            avg_actions = sum(actions_values) / len(actions_values)
            # 1-2 actions is ideal, >5 is slow
            speed_score = max(0.0, (1.0 - (avg_actions - 1.0) / 4.0)) * 0.20
        else:
            speed_score = 0.10

        # Low unrecovered rate (0-0.15): lower is better
        unrecovered_ratio = unrecovered / errors if errors > 0 else 0.0
        unrecovered_score = (1.0 - min(unrecovered_ratio / 0.30, 1.0)) * 0.15

        session_score = round(
            recovery_score + immediate_score + speed_score + unrecovered_score, 4
        )
        session_scores.append(session_score)

    # Aggregate metrics
    recovery_rate = _percentage(agg_recovered, agg_errors)
    immediate_recovery_rate = _percentage(agg_immediate_recovery, agg_errors)
    unrecovered_rate = _percentage(agg_unrecovered, agg_errors)
    avg_actions_to_recover = _average(all_actions_to_recover)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    error_recovery_speed_score = (
        round(_average(session_scores), 4) if session_scores else 0.0
    )

    return {
        "total_sessions": total_sessions,
        "total_tool_errors": agg_errors,
        "errors_recovered": agg_recovered,
        "recovery_rate": recovery_rate,
        "errors_unrecovered": agg_unrecovered,
        "unrecovered_rate": unrecovered_rate,
        "immediate_recovery": agg_immediate_recovery,
        "immediate_recovery_rate": immediate_recovery_rate,
        "avg_actions_to_recover": avg_actions_to_recover,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "error_recovery_speed_score": error_recovery_speed_score,
    }
