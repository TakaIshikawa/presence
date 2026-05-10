"""Session Edit batch efficiency and multi-edit grouping analyzer.

Measures how efficiently an agent batches Edit tool calls when making
multiple changes to the same file.

Dimensions: edits per file, consecutive same-file edits, replace_all usage.
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


def analyze_session_edit_batch_efficiency(records: object) -> dict[str, Any]:
    """Analyze Edit tool batching efficiency across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_edit_calls = 0
    agg_files_edited = 0
    agg_consecutive_same_file = 0
    agg_single_edit_files = 0
    agg_multi_edit_files = 0
    agg_max_consecutive = 0
    agg_replace_all_usage = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        edit_calls = _int(record.get("total_edit_calls"))
        files_edited = _int(record.get("total_files_edited"))
        consecutive_same_file = _int(record.get("consecutive_same_file_edits"))
        single_edit_files = _int(record.get("single_edit_files"))
        multi_edit_files = _int(record.get("multi_edit_files"))
        max_consecutive = _int(record.get("max_consecutive_same_file"))
        replace_all_usage = _int(record.get("replace_all_usage_count"))

        agg_edit_calls += edit_calls
        agg_files_edited += files_edited
        agg_consecutive_same_file += consecutive_same_file
        agg_single_edit_files += single_edit_files
        agg_multi_edit_files += multi_edit_files
        agg_max_consecutive = max(agg_max_consecutive, max_consecutive)
        agg_replace_all_usage += replace_all_usage

        if edit_calls == 0:
            session_scores.append(1.0)
            continue

        # Scoring: efficiency means fewer consecutive same-file edits
        # Low consecutive same-file rate (0-0.40): lower is better
        consec_ratio = consecutive_same_file / edit_calls if edit_calls > 0 else 0.0
        consec_score = (1.0 - min(consec_ratio / 0.70, 1.0)) * 0.40

        # High single-edit file ratio (0-0.30): more single-edit files is efficient
        if files_edited > 0:
            single_ratio = single_edit_files / files_edited
            single_score = min(single_ratio / 0.80, 1.0) * 0.30
        else:
            single_score = 0.30

        # replace_all usage (0-0.15): using replace_all is efficient for bulk changes
        if edit_calls > 0:
            replace_ratio = replace_all_usage / edit_calls
            replace_score = min(replace_ratio / 0.20, 1.0) * 0.15
        else:
            replace_score = 0.075

        # Low max consecutive streak (0-0.15): shorter streaks are better
        if max_consecutive <= 2:
            streak_score = 0.15
        elif max_consecutive <= 5:
            streak_score = 0.10
        else:
            streak_score = max(0.0, (1.0 - (max_consecutive - 5) / 10.0)) * 0.15

        session_score = round(consec_score + single_score + replace_score + streak_score, 4)
        session_scores.append(session_score)

    # Aggregate metrics
    edits_per_file_avg = _average(
        [agg_edit_calls // agg_files_edited] if agg_files_edited > 0 else []
    ) if agg_files_edited > 0 else 0.0
    consecutive_same_file_rate = _percentage(agg_consecutive_same_file, agg_edit_calls)
    replace_all_rate = _percentage(agg_replace_all_usage, agg_edit_calls)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    edit_batch_efficiency_score = (
        round(_average(session_scores), 4) if session_scores else 0.0
    )

    return {
        "total_sessions": total_sessions,
        "total_edit_calls": agg_edit_calls,
        "total_files_edited": agg_files_edited,
        "edits_per_file_avg": round(agg_edit_calls / agg_files_edited, 2) if agg_files_edited > 0 else 0.0,
        "consecutive_same_file_edits": agg_consecutive_same_file,
        "consecutive_same_file_rate": consecutive_same_file_rate,
        "single_edit_files": agg_single_edit_files,
        "multi_edit_files": agg_multi_edit_files,
        "max_consecutive_same_file": agg_max_consecutive,
        "replace_all_usage_count": agg_replace_all_usage,
        "replace_all_rate": replace_all_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "edit_batch_efficiency_score": edit_batch_efficiency_score,
    }
