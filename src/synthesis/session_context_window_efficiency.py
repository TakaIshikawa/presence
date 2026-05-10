"""Session context window efficiency and summarization impact analyzer.

Dimensions: context window utilization, redundant reads, tool output volume,
summarization triggers, information density.
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


def analyze_session_context_window_efficiency(records: object) -> dict[str, Any]:
    """Analyze session context window efficiency and summarization impact."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    sessions: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, Mapping):
            continue
        sessions.append(dict(record))

    total_sessions = len(sessions)

    all_tokens: list[int | float] = []
    all_tokens_per_tool_call: list[float] = []
    all_redundant_rates: list[float] = []
    all_large_output_calls = 0
    all_large_output_rates: list[float] = []
    all_summarization_triggered = 0
    all_density_scores: list[float] = []
    all_tokens_per_file_change: list[float] = []
    all_session_scores: list[float] = []
    high_quality_sessions = 0
    low_quality_sessions = 0

    for session in sessions:
        total_tokens = _int(session.get("total_tokens_used"))
        total_tool_calls = _int(session.get("total_tool_calls"))
        redundant_reads = _int(session.get("redundant_file_reads"))
        total_reads = _int(session.get("total_file_reads"))
        large_output = _int(session.get("large_output_tool_calls"))
        total_output_calls = _int(session.get("total_tool_output_calls"))
        summarization = bool(session.get("summarization_triggered"))
        total_changes = _int(session.get("total_file_changes"))
        density = _float(session.get("information_density_score"))

        all_tokens.append(total_tokens)

        if total_tool_calls > 0:
            all_tokens_per_tool_call.append(total_tokens / total_tool_calls)

        redundant_rate = _percentage(redundant_reads, total_reads) if total_reads > 0 else 0.0
        all_redundant_rates.append(redundant_rate)

        all_large_output_calls += large_output
        large_output_rate = _percentage(large_output, total_output_calls) if total_output_calls > 0 else 0.0
        all_large_output_rates.append(large_output_rate)

        if summarization:
            all_summarization_triggered += 1

        all_density_scores.append(density)

        if total_changes > 0:
            all_tokens_per_file_change.append(total_tokens / total_changes)

        # Session score components
        # Low redundant reads (0-0.30): <10% redundant = full
        if redundant_rate < 10.0:
            redundant_score = 0.30
        else:
            redundant_score = max(0.0, 0.30 * (1.0 - (redundant_rate - 10.0) / 90.0))

        # Low large outputs (0-0.25): <15% large output calls = full
        if large_output_rate < 15.0:
            large_output_score = 0.25
        else:
            large_output_score = max(0.0, 0.25 * (1.0 - (large_output_rate - 15.0) / 85.0))

        # No summarization triggered (0-0.25): not triggered = full
        summarization_score = 0.25 if not summarization else 0.0

        # High info density (0-0.20): >0.6 density score = full
        if density > 0.6:
            density_component = 0.20
        else:
            density_component = 0.20 * (density / 0.6) if density > 0 else 0.0

        session_score = redundant_score + large_output_score + summarization_score + density_component
        all_session_scores.append(session_score)

        if session_score > 0.7:
            high_quality_sessions += 1
        if session_score < 0.4:
            low_quality_sessions += 1

    return {
        "total_sessions": total_sessions,
        "avg_total_tokens": _average(all_tokens),
        "avg_tokens_per_tool_call": _average(all_tokens_per_tool_call),
        "redundant_read_rate": _average(all_redundant_rates),
        "large_output_tool_calls": all_large_output_calls,
        "large_output_rate": _average(all_large_output_rates),
        "summarization_triggered_sessions": all_summarization_triggered,
        "summarization_trigger_rate": _percentage(all_summarization_triggered, total_sessions),
        "avg_information_density": _average(all_density_scores),
        "tokens_per_file_change": _average(all_tokens_per_file_change),
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "context_window_efficiency_score": round(_average(all_session_scores), 4),
    }
