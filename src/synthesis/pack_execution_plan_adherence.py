"""Pack execution plan adherence and drift analyzer.

Measures how well an execution pack adheres to its original task plan
versus drifting into unplanned work.

Dimensions: planned file hit rate, unplanned file rate, scope creep,
underdelivery, exact match.
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


def analyze_pack_execution_plan_adherence(records: object) -> dict[str, Any]:
    """Analyze execution plan adherence across packs."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    agg_planned_files = 0
    agg_actual_files = 0
    agg_planned_hit = 0
    agg_unplanned = 0
    scope_creep_count = 0
    underdelivery_count = 0
    exact_match_count = 0
    pack_scores: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        planned_files = _int(record.get("total_planned_files"))
        actual_files = _int(record.get("total_actual_files_changed"))
        planned_hit = _int(record.get("planned_files_hit"))
        unplanned = _int(record.get("unplanned_files"))

        agg_planned_files += planned_files
        agg_actual_files += actual_files
        agg_planned_hit += planned_hit
        agg_unplanned += unplanned

        # Classify pack
        is_scope_creep = unplanned > planned_files and planned_files > 0
        is_underdelivery = planned_files > 0 and planned_hit < (planned_files * 0.5)
        is_exact_match = (
            planned_files > 0
            and planned_hit == planned_files
            and unplanned == 0
        )

        if is_scope_creep:
            scope_creep_count += 1
        if is_underdelivery:
            underdelivery_count += 1
        if is_exact_match:
            exact_match_count += 1

        # Scoring per pack
        if planned_files == 0:
            pack_scores.append(1.0)
            continue

        # Planned file hit rate (0-0.50): higher is better
        hit_ratio = planned_hit / planned_files if planned_files > 0 else 0.0
        hit_score = min(hit_ratio / 1.0, 1.0) * 0.50

        # Low unplanned file rate (0-0.30): lower is better
        if actual_files > 0:
            unplanned_ratio = unplanned / actual_files
            unplanned_score = (1.0 - min(unplanned_ratio / 0.50, 1.0)) * 0.30
        else:
            unplanned_score = 0.30

        # Exact match bonus (0-0.20)
        if is_exact_match:
            exact_score = 0.20
        elif hit_ratio >= 0.80 and unplanned <= 1:
            exact_score = 0.10
        else:
            exact_score = 0.0

        pack_score = round(hit_score + unplanned_score + exact_score, 4)
        pack_scores.append(pack_score)

    # Aggregate rates
    planned_file_hit_rate = _percentage(agg_planned_hit, agg_planned_files)
    unplanned_file_rate = _percentage(agg_unplanned, agg_actual_files)
    scope_creep_rate = _percentage(scope_creep_count, total_packs)
    underdelivery_rate = _percentage(underdelivery_count, total_packs)
    exact_match_rate = _percentage(exact_match_count, total_packs)

    high_quality_packs = sum(1 for s in pack_scores if s > 0.7)
    low_quality_packs = sum(1 for s in pack_scores if s < 0.4)

    execution_plan_adherence_score = (
        round(_average(pack_scores), 4) if pack_scores else 0.0
    )

    return {
        "total_packs": total_packs,
        "total_planned_files": agg_planned_files,
        "total_actual_files_changed": agg_actual_files,
        "planned_file_hit_rate": planned_file_hit_rate,
        "unplanned_file_rate": unplanned_file_rate,
        "scope_creep_count": scope_creep_count,
        "scope_creep_rate": scope_creep_rate,
        "underdelivery_count": underdelivery_count,
        "underdelivery_rate": underdelivery_rate,
        "exact_match_count": exact_match_count,
        "exact_match_rate": exact_match_rate,
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "execution_plan_adherence_score": execution_plan_adherence_score,
    }
