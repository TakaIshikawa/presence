"""Session Glob/Grep result utilization analyzer.

Measures how effectively an agent uses the results from Glob and Grep
tool calls — whether returned file paths are actually read or acted upon.

Dimensions: result utilization rate, zero-result searches, redundant searches,
searches with no followup.
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


def analyze_session_glob_grep_result_utilization(records: object) -> dict[str, Any]:
    """Analyze Glob/Grep result utilization across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_search_calls = 0
    agg_results_returned = 0
    agg_results_read = 0
    agg_zero_result_searches = 0
    agg_redundant_searches = 0
    agg_no_followup = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        search_calls = _int(record.get("total_search_calls"))
        results_returned = _int(record.get("total_results_returned"))
        results_read = _int(record.get("results_subsequently_read"))
        zero_result = _int(record.get("zero_result_searches"))
        redundant = _int(record.get("redundant_searches"))
        no_followup = _int(record.get("searches_with_no_followup"))

        agg_search_calls += search_calls
        agg_results_returned += results_returned
        agg_results_read += results_read
        agg_zero_result_searches += zero_result
        agg_redundant_searches += redundant
        agg_no_followup += no_followup

        if search_calls == 0:
            session_scores.append(1.0)
            continue

        # Result utilization rate (0-0.40): higher is better
        util_ratio = results_read / results_returned if results_returned > 0 else 0.0
        util_score = min(util_ratio / 0.60, 1.0) * 0.40

        # Low no-followup rate (0-0.30): lower is better
        no_followup_ratio = no_followup / search_calls if search_calls > 0 else 0.0
        no_followup_score = (1.0 - min(no_followup_ratio / 0.50, 1.0)) * 0.30

        # Low redundant search rate (0-0.15): lower is better
        redundant_ratio = redundant / search_calls if search_calls > 0 else 0.0
        redundant_score = (1.0 - min(redundant_ratio / 0.30, 1.0)) * 0.15

        # Low zero-result rate (0-0.15): lower is better
        zero_ratio = zero_result / search_calls if search_calls > 0 else 0.0
        zero_score = (1.0 - min(zero_ratio / 0.40, 1.0)) * 0.15

        session_score = round(util_score + no_followup_score + redundant_score + zero_score, 4)
        session_scores.append(session_score)

    # Aggregate metrics
    result_utilization_rate = _percentage(agg_results_read, agg_results_returned)
    zero_result_rate = _percentage(agg_zero_result_searches, agg_search_calls)
    redundant_search_rate = _percentage(agg_redundant_searches, agg_search_calls)
    avg_results_per_search = round(agg_results_returned / agg_search_calls, 2) if agg_search_calls > 0 else 0.0
    no_followup_rate = _percentage(agg_no_followup, agg_search_calls)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    glob_grep_result_utilization_score = (
        round(_average(session_scores), 4) if session_scores else 0.0
    )

    return {
        "total_sessions": total_sessions,
        "total_search_calls": agg_search_calls,
        "total_results_returned": agg_results_returned,
        "results_subsequently_read": agg_results_read,
        "result_utilization_rate": result_utilization_rate,
        "zero_result_searches": agg_zero_result_searches,
        "zero_result_rate": zero_result_rate,
        "redundant_searches": agg_redundant_searches,
        "redundant_search_rate": redundant_search_rate,
        "avg_results_per_search": avg_results_per_search,
        "searches_with_no_followup": agg_no_followup,
        "no_followup_rate": no_followup_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "glob_grep_result_utilization_score": glob_grep_result_utilization_score,
    }
