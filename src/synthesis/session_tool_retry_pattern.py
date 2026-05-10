"""Session tool retry pattern and backoff strategy analyzer.

Dimensions: retry frequency, retry variation, backoff patterns,
tool switch after failure, give-up appropriateness.
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


def analyze_session_tool_retry_pattern(records: object) -> dict[str, Any]:
    """Analyze tool retry patterns and backoff strategies across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_tool_failures = 0
    agg_retries = 0
    agg_exact_retries = 0
    agg_varied_retries = 0
    agg_tool_switches = 0
    agg_excessive_retries = 0
    agg_appropriate_giveups = 0
    agg_total_giveups = 0
    all_retries_before_success: list[int] = []
    all_retries_before_giveup: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        tool_failures = _int(record.get("total_tool_failures"))
        retries = _int(record.get("total_retries"))
        exact = _int(record.get("exact_retries"))
        varied = _int(record.get("varied_retries"))
        switches = _int(record.get("tool_switches_after_failure"))
        excessive = _int(record.get("excessive_retries"))
        appropriate = _int(record.get("appropriate_giveups"))
        total_giveups = _int(record.get("total_giveups"))

        agg_tool_failures += tool_failures
        agg_retries += retries
        agg_exact_retries += exact
        agg_varied_retries += varied
        agg_tool_switches += switches
        agg_excessive_retries += excessive
        agg_appropriate_giveups += appropriate
        agg_total_giveups += total_giveups

        rbs = record.get("retries_before_success_values")
        if isinstance(rbs, list):
            all_retries_before_success.extend(rbs)

        rbg = record.get("retries_before_giveup_values")
        if isinstance(rbg, list):
            all_retries_before_giveup.extend(rbg)

        # Session score components
        # Varied retries over exact (0-0.30)
        total_retry_types = exact + varied
        if total_retry_types > 0:
            varied_ratio = varied / total_retry_types
            varied_score = min(varied_ratio / 0.60, 1.0) * 0.30
        else:
            varied_score = 0.30  # no retries means no bad pattern

        # Tool switch after failure (0-0.25)
        if tool_failures > 0:
            switch_ratio = switches / tool_failures
            switch_score = min(switch_ratio / 0.40, 1.0) * 0.25
        else:
            switch_score = 0.25  # no failures means no bad pattern

        # Low excessive retries (0-0.25)
        if retries > 0:
            excessive_ratio = excessive / retries
            excessive_score = (1.0 - min(excessive_ratio / 0.10, 1.0)) * 0.25
        else:
            excessive_score = 0.25  # no retries means no excessive

        # Appropriate giveup (0-0.20)
        if total_giveups > 0:
            giveup_ratio = appropriate / total_giveups
            giveup_score = min(giveup_ratio / 0.70, 1.0) * 0.20
        else:
            giveup_score = 0.20  # no giveups means no bad pattern

        session_score = round(varied_score + switch_score + excessive_score + giveup_score, 4)
        session_scores.append(session_score)

    # Aggregate rates
    retry_rate = _percentage(agg_retries, agg_tool_failures)
    exact_retry_rate = _percentage(agg_exact_retries, agg_retries)
    varied_retry_rate = _percentage(agg_varied_retries, agg_retries)
    tool_switch_after_failure_rate = _percentage(agg_tool_switches, agg_tool_failures)
    excessive_retry_rate = _percentage(agg_excessive_retries, agg_retries)
    appropriate_giveup_rate = _percentage(agg_appropriate_giveups, agg_total_giveups)

    avg_retries_before_success = _average(all_retries_before_success)
    avg_retries_before_giveup = _average(all_retries_before_giveup)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    tool_retry_pattern_score = round(_average(session_scores), 4) if session_scores else 0.0

    return {
        "total_sessions": total_sessions,
        "total_tool_failures": agg_tool_failures,
        "retry_rate": retry_rate,
        "exact_retry_rate": exact_retry_rate,
        "varied_retry_rate": varied_retry_rate,
        "tool_switch_after_failure_rate": tool_switch_after_failure_rate,
        "avg_retries_before_success": avg_retries_before_success,
        "avg_retries_before_giveup": avg_retries_before_giveup,
        "excessive_retry_rate": excessive_retry_rate,
        "appropriate_giveup_rate": appropriate_giveup_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "tool_retry_pattern_score": tool_retry_pattern_score,
    }
