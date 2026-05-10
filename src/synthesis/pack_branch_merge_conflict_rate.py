"""Pack branch merge conflict rate and resolution analyzer.

Measures how frequently execution packs encounter merge conflicts
and how effectively they are resolved.

Dimensions: conflict rate, resolution rate, auto-resolution,
conflicts in expected files.
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


def analyze_pack_branch_merge_conflict_rate(records: object) -> dict[str, Any]:
    """Analyze merge conflict rates and resolution across packs."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    packs_with_conflicts = 0
    agg_conflicts = 0
    agg_resolved = 0
    agg_auto_resolved = 0
    agg_conflict_in_expected = 0
    agg_blocked = 0
    conflicts_per_pack_values: list[int] = []
    pack_scores: list[float] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        conflicts = _int(record.get("total_conflicts"))
        resolved = _int(record.get("total_resolved"))
        auto_resolved = _int(record.get("auto_resolved"))
        conflict_in_expected = _int(record.get("conflict_in_expected_files"))
        blocked = _int(record.get("blocked_by_conflict"))

        agg_conflicts += conflicts
        agg_resolved += resolved
        agg_auto_resolved += auto_resolved
        agg_conflict_in_expected += conflict_in_expected
        agg_blocked += blocked

        if conflicts > 0:
            packs_with_conflicts += 1
            conflicts_per_pack_values.append(conflicts)

        # Scoring per pack
        if conflicts == 0:
            pack_scores.append(1.0)
            continue

        # High resolution rate (0-0.40): higher is better
        resolution_ratio = resolved / conflicts if conflicts > 0 else 0.0
        resolution_score = min(resolution_ratio / 1.0, 1.0) * 0.40

        # High auto-resolution rate (0-0.25): higher is better
        auto_ratio = auto_resolved / conflicts if conflicts > 0 else 0.0
        auto_score = min(auto_ratio / 0.80, 1.0) * 0.25

        # Low conflict count (0-0.20): fewer conflicts is better
        conflict_penalty = min(conflicts / 5.0, 1.0)
        conflict_score = (1.0 - conflict_penalty) * 0.20

        # Not blocked (0-0.15): not being blocked is good
        blocked_score = 0.0 if blocked else 0.15

        pack_score = round(resolution_score + auto_score + conflict_score + blocked_score, 4)
        pack_scores.append(pack_score)

    # Aggregate metrics
    conflict_rate = _percentage(packs_with_conflicts, total_packs)
    resolution_rate = _percentage(agg_resolved, agg_conflicts)
    auto_resolution_rate = _percentage(agg_auto_resolved, agg_conflicts)
    avg_conflicts_per_pack = _average(conflicts_per_pack_values)
    conflict_in_expected_rate = _percentage(agg_conflict_in_expected, agg_conflicts)

    high_quality_packs = sum(1 for s in pack_scores if s > 0.7)
    low_quality_packs = sum(1 for s in pack_scores if s < 0.4)

    branch_merge_conflict_score = (
        round(_average(pack_scores), 4) if pack_scores else 0.0
    )

    return {
        "total_packs": total_packs,
        "packs_with_conflicts": packs_with_conflicts,
        "conflict_rate": conflict_rate,
        "total_conflicts": agg_conflicts,
        "total_resolved": agg_resolved,
        "resolution_rate": resolution_rate,
        "auto_resolved": agg_auto_resolved,
        "auto_resolution_rate": auto_resolution_rate,
        "avg_conflicts_per_pack": avg_conflicts_per_pack,
        "conflict_in_expected_files": agg_conflict_in_expected,
        "conflict_in_expected_rate": conflict_in_expected_rate,
        "packs_blocked_by_conflict": agg_blocked,
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "branch_merge_conflict_score": branch_merge_conflict_score,
    }
