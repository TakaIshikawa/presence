"""Tests for pack branch merge conflict rate analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_branch_merge_conflict_rate import (
    analyze_pack_branch_merge_conflict_rate,
)


class TestAnalyzePackBranchMergeConflictRate:
    """Tests for analyze_pack_branch_merge_conflict_rate."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_branch_merge_conflict_rate([])
        assert result["total_packs"] == 0
        assert result["total_conflicts"] == 0
        assert result["branch_merge_conflict_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_branch_merge_conflict_rate(None)
        assert result["total_packs"] == 0
        assert result["branch_merge_conflict_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_branch_merge_conflict_rate("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_branch_merge_conflict_rate(42)

    def test_no_conflicts_scores_high(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_conflicts": 0,
                "total_resolved": 0,
                "auto_resolved": 0,
                "conflict_in_expected_files": 0,
                "blocked_by_conflict": 0,
            }
        ]
        result = analyze_pack_branch_merge_conflict_rate(records)
        assert result["total_packs"] == 1
        assert result["packs_with_conflicts"] == 0
        assert result["high_quality_packs"] == 1
        assert result["branch_merge_conflict_score"] > 0.7

    def test_blocked_by_conflicts_scores_low(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_conflicts": 8,
                "total_resolved": 2,
                "auto_resolved": 0,
                "conflict_in_expected_files": 5,
                "blocked_by_conflict": 1,
            }
        ]
        result = analyze_pack_branch_merge_conflict_rate(records)
        assert result["total_packs"] == 1
        assert result["packs_with_conflicts"] == 1
        assert result["low_quality_packs"] == 1
        assert result["branch_merge_conflict_score"] < 0.4

    def test_multiple_packs_mixed(self) -> None:
        records = [
            {
                "pack_id": "clean",
                "total_conflicts": 0,
                "total_resolved": 0,
                "auto_resolved": 0,
                "conflict_in_expected_files": 0,
                "blocked_by_conflict": 0,
            },
            {
                "pack_id": "messy",
                "total_conflicts": 8,
                "total_resolved": 2,
                "auto_resolved": 0,
                "conflict_in_expected_files": 5,
                "blocked_by_conflict": 1,
            },
        ]
        result = analyze_pack_branch_merge_conflict_rate(records)
        assert result["total_packs"] == 2
        assert result["packs_with_conflicts"] == 1
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "pack_id": "valid",
                "total_conflicts": 0,
                "total_resolved": 0,
                "auto_resolved": 0,
                "conflict_in_expected_files": 0,
                "blocked_by_conflict": 0,
            },
        ]
        result = analyze_pack_branch_merge_conflict_rate(records)
        assert result["total_packs"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_branch_merge_conflict_rate([])
        expected_keys = {
            "total_packs",
            "packs_with_conflicts",
            "conflict_rate",
            "total_conflicts",
            "total_resolved",
            "resolution_rate",
            "auto_resolved",
            "auto_resolution_rate",
            "avg_conflicts_per_pack",
            "conflict_in_expected_files",
            "conflict_in_expected_rate",
            "packs_blocked_by_conflict",
            "high_quality_packs",
            "low_quality_packs",
            "branch_merge_conflict_score",
        }
        assert set(result.keys()) == expected_keys
