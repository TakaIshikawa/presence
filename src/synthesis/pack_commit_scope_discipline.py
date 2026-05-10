"""Pack commit scope discipline and diff size analyzer.

Dimensions: commit message quality, single-concern commits, diff size management,
file count per commit, test inclusion.
"""

from __future__ import annotations

from typing import Any, Mapping


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


def _empty_result() -> dict[str, Any]:
    return {
        "total_packs": 0,
        "total_commits": 0,
        "single_concern_rate": 0.0,
        "multi_concern_rate": 0.0,
        "avg_diff_lines": 0.0,
        "large_diff_rate": 0.0,
        "test_inclusion_rate": 0.0,
        "avg_files_per_commit": 0.0,
        "descriptive_message_rate": 0.0,
        "high_quality_packs": 0,
        "low_quality_packs": 0,
        "commit_scope_discipline_score": 0.0,
    }


def analyze_pack_commit_scope_discipline(records: object) -> dict[str, Any]:
    """Analyze commit scope discipline across packs."""
    if records is None:
        return _empty_result()
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")
    if not records:
        return _empty_result()

    total_packs = 0
    total_commits = 0
    total_single_concern = 0
    total_multi_concern = 0
    all_avg_diff_lines: list[float] = []
    total_large_diff = 0
    total_commits_with_tests = 0
    total_testable_commits = 0
    all_avg_files: list[float] = []
    total_descriptive = 0
    pack_scores: list[float] = []
    high_quality_packs = 0
    low_quality_packs = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1
        commits = _int(record.get("total_commits"))
        single_concern = _int(record.get("single_concern_commits"))
        multi_concern = _int(record.get("multi_concern_commits"))
        avg_diff = _float(record.get("avg_diff_lines"))
        large_diff = _int(record.get("large_diff_commits"))
        with_tests = _int(record.get("commits_with_tests"))
        testable = _int(record.get("total_testable_commits"))
        avg_files = _float(record.get("avg_files_per_commit"))
        descriptive = _int(record.get("descriptive_message_commits"))

        total_commits += commits
        total_single_concern += single_concern
        total_multi_concern += multi_concern
        if avg_diff > 0:
            all_avg_diff_lines.append(avg_diff)
        total_large_diff += large_diff
        total_commits_with_tests += with_tests
        total_testable_commits += testable
        if avg_files > 0:
            all_avg_files.append(avg_files)
        total_descriptive += descriptive

        # Pack score calculation
        if commits <= 0:
            pack_scores.append(0.0)
            low_quality_packs += 1
            continue

        # Single concern rate (0-0.30): >80% single concern = full
        sc_rate = single_concern / commits if commits > 0 else 0.0
        sc_score = min(sc_rate / 0.80, 1.0) * 0.30

        # Low large diffs (0-0.25): <15% large = full
        ld_rate = large_diff / commits if commits > 0 else 0.0
        ld_score = max(0.0, 1.0 - ld_rate / 0.15) * 0.25

        # Test inclusion (0-0.25): >60% testable have tests = full
        ti_rate = with_tests / testable if testable > 0 else 0.0
        ti_score = min(ti_rate / 0.60, 1.0) * 0.25

        # Descriptive messages (0-0.20): >80% descriptive = full
        dm_rate = descriptive / commits if commits > 0 else 0.0
        dm_score = min(dm_rate / 0.80, 1.0) * 0.20

        pack_score = round(sc_score + ld_score + ti_score + dm_score, 4)
        pack_scores.append(pack_score)

        if pack_score > 0.7:
            high_quality_packs += 1
        elif pack_score < 0.4:
            low_quality_packs += 1

    # Overall aggregated score
    if total_commits > 0:
        overall_sc_rate = total_single_concern / total_commits
        sc_component = min(overall_sc_rate / 0.80, 1.0) * 0.30

        overall_ld_rate = total_large_diff / total_commits
        ld_component = max(0.0, 1.0 - overall_ld_rate / 0.15) * 0.25

        overall_ti_rate = (
            total_commits_with_tests / total_testable_commits
            if total_testable_commits > 0
            else 0.0
        )
        ti_component = min(overall_ti_rate / 0.60, 1.0) * 0.25

        overall_dm_rate = total_descriptive / total_commits
        dm_component = min(overall_dm_rate / 0.80, 1.0) * 0.20

        overall_score = round(sc_component + ld_component + ti_component + dm_component, 4)
    else:
        overall_score = 0.0

    return {
        "total_packs": total_packs,
        "total_commits": total_commits,
        "single_concern_rate": _percentage(total_single_concern, total_commits),
        "multi_concern_rate": _percentage(total_multi_concern, total_commits),
        "avg_diff_lines": _average(all_avg_diff_lines),
        "large_diff_rate": _percentage(total_large_diff, total_commits),
        "test_inclusion_rate": _percentage(total_commits_with_tests, total_testable_commits),
        "avg_files_per_commit": _average(all_avg_files),
        "descriptive_message_rate": _percentage(total_descriptive, total_commits),
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "commit_scope_discipline_score": overall_score,
    }
