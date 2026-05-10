"""Session Write vs Edit tool decision appropriateness analyzer.

Measures whether an agent makes appropriate choices between Write
(full file creation) and Edit (targeted modification) tools.

Dimensions: write to existing rate, edit to new rate, appropriate decisions.
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


def analyze_session_write_vs_edit_decision(records: object) -> dict[str, Any]:
    """Analyze Write vs Edit decision appropriateness across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_write_calls = 0
    agg_edit_calls = 0
    agg_write_to_existing = 0
    agg_edit_to_new = 0
    agg_appropriate_write = 0
    agg_appropriate_edit = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        write_calls = _int(record.get("total_write_calls"))
        edit_calls = _int(record.get("total_edit_calls"))
        write_to_existing = _int(record.get("write_to_existing_file"))
        edit_to_new = _int(record.get("edit_to_new_file"))
        appropriate_write = _int(record.get("appropriate_write"))
        appropriate_edit = _int(record.get("appropriate_edit"))

        agg_write_calls += write_calls
        agg_edit_calls += edit_calls
        agg_write_to_existing += write_to_existing
        agg_edit_to_new += edit_to_new
        agg_appropriate_write += appropriate_write
        agg_appropriate_edit += appropriate_edit

        total_calls = write_calls + edit_calls
        if total_calls == 0:
            session_scores.append(1.0)
            continue

        # Appropriate decision rate (0-0.50): higher is better
        total_appropriate = appropriate_write + appropriate_edit
        appropriate_ratio = total_appropriate / total_calls if total_calls > 0 else 0.0
        appropriate_score = min(appropriate_ratio / 0.90, 1.0) * 0.50

        # Low write-to-existing rate (0-0.30): lower is better
        write_existing_ratio = write_to_existing / write_calls if write_calls > 0 else 0.0
        write_existing_score = (1.0 - min(write_existing_ratio / 0.30, 1.0)) * 0.30

        # Low edit-to-new rate (0-0.20): lower is better
        edit_new_ratio = edit_to_new / edit_calls if edit_calls > 0 else 0.0
        edit_new_score = (1.0 - min(edit_new_ratio / 0.20, 1.0)) * 0.20

        session_score = round(appropriate_score + write_existing_score + edit_new_score, 4)
        session_scores.append(session_score)

    # Aggregate metrics
    write_to_existing_rate = _percentage(agg_write_to_existing, agg_write_calls)
    edit_to_new_rate = _percentage(agg_edit_to_new, agg_edit_calls)
    appropriate_write_rate = _percentage(agg_appropriate_write, agg_write_calls)
    appropriate_edit_rate = _percentage(agg_appropriate_edit, agg_edit_calls)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    write_vs_edit_decision_score = (
        round(_average(session_scores), 4) if session_scores else 0.0
    )

    return {
        "total_sessions": total_sessions,
        "total_write_calls": agg_write_calls,
        "total_edit_calls": agg_edit_calls,
        "write_to_existing_file": agg_write_to_existing,
        "write_to_existing_rate": write_to_existing_rate,
        "edit_to_new_file": agg_edit_to_new,
        "edit_to_new_rate": edit_to_new_rate,
        "appropriate_write": agg_appropriate_write,
        "appropriate_write_rate": appropriate_write_rate,
        "appropriate_edit": agg_appropriate_edit,
        "appropriate_edit_rate": appropriate_edit_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "write_vs_edit_decision_score": write_vs_edit_decision_score,
    }
