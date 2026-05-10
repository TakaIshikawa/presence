"""Pack session handoff continuity analyzer.

Measures how well context and progress are maintained across
session handoffs within a pack.

Dimensions: handoff count, context preserved rate, repeated work,
progress continuity.
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


def analyze_pack_session_handoff_continuity(records: object) -> dict[str, Any]:
    """Analyze session handoff continuity across packs."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    agg_handoffs = 0
    agg_context_preserved = 0
    agg_repeated_work = 0
    agg_progress_continued = 0
    agg_sessions_per_pack: list[int] = []
    pack_scores: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        handoffs = _int(record.get("total_handoffs"))
        context_preserved = _int(record.get("context_preserved"))
        repeated_work = _int(record.get("repeated_work_instances"))
        progress_continued = _int(record.get("progress_continued"))
        sessions = _int(record.get("total_sessions"))

        agg_handoffs += handoffs
        agg_context_preserved += context_preserved
        agg_repeated_work += repeated_work
        agg_progress_continued += progress_continued
        if sessions > 0:
            agg_sessions_per_pack.append(sessions)

        if handoffs == 0:
            pack_scores.append(1.0)
            continue

        # Context preservation rate (0-0.40): higher is better
        context_ratio = context_preserved / handoffs if handoffs > 0 else 0.0
        context_score = min(context_ratio / 0.90, 1.0) * 0.40

        # Low repeated work rate (0-0.30): lower is better
        repeated_ratio = repeated_work / handoffs if handoffs > 0 else 0.0
        repeated_score = (1.0 - min(repeated_ratio / 0.40, 1.0)) * 0.30

        # Progress continuity rate (0-0.30): higher is better
        progress_ratio = progress_continued / handoffs if handoffs > 0 else 0.0
        progress_score = min(progress_ratio / 0.90, 1.0) * 0.30

        pack_score = round(context_score + repeated_score + progress_score, 4)
        pack_scores.append(pack_score)

    # Aggregate metrics
    context_preserved_rate = _percentage(agg_context_preserved, agg_handoffs)
    repeated_work_rate = _percentage(agg_repeated_work, agg_handoffs)
    progress_continued_rate = _percentage(agg_progress_continued, agg_handoffs)
    avg_sessions_per_pack = _average(agg_sessions_per_pack)

    high_quality_packs = sum(1 for s in pack_scores if s > 0.7)
    low_quality_packs = sum(1 for s in pack_scores if s < 0.4)

    session_handoff_continuity_score = (
        round(_average(pack_scores), 4) if pack_scores else 0.0
    )

    return {
        "total_packs": total_packs,
        "total_handoffs": agg_handoffs,
        "context_preserved": agg_context_preserved,
        "context_preserved_rate": context_preserved_rate,
        "repeated_work_instances": agg_repeated_work,
        "repeated_work_rate": repeated_work_rate,
        "progress_continued": agg_progress_continued,
        "progress_continued_rate": progress_continued_rate,
        "avg_sessions_per_pack": avg_sessions_per_pack,
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "session_handoff_continuity_score": session_handoff_continuity_score,
    }
