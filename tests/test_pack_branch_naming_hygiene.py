"""Tests for pack branch naming hygiene analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_branch_naming_hygiene import analyze_pack_branch_naming_hygiene


EXPECTED_KEYS = {
    "total_packs",
    "total_branches_created",
    "convention_compliant_rate",
    "task_aligned_rate",
    "pr_workflow_adherence_rate",
    "avg_commits_per_pr",
    "oversized_pr_rate",
    "undersized_pr_rate",
    "branch_reuse_rate",
    "high_quality_packs",
    "low_quality_packs",
    "branch_naming_hygiene_score",
}


class TestAnalyzePackBranchNamingHygiene:
    """Tests for analyze_pack_branch_naming_hygiene."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_branch_naming_hygiene([])
        assert result["total_packs"] == 0
        assert result["total_branches_created"] == 0
        assert result["convention_compliant_rate"] == 0.0
        assert result["branch_naming_hygiene_score"] == 0.0
        assert result["high_quality_packs"] == 0
        assert result["low_quality_packs"] == 0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_branch_naming_hygiene(None)
        assert result["total_packs"] == 0
        assert result["total_branches_created"] == 0
        assert result["branch_naming_hygiene_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_branch_naming_hygiene("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_branch_naming_hygiene(42)

    def test_single_pack_high_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-1",
                "total_branches_created": 10,
                "convention_compliant_branches": 9,  # 90% > 80%
                "task_aligned_branches": 8,  # 80% > 70%
                "pr_workflow_steps_followed": 9,
                "pr_workflow_steps_total": 10,  # 90% > 80%
                "total_commits": 30,
                "total_prs": 10,
                "oversized_prs": 0,  # 0% < 10%
                "undersized_prs": 1,  # 10% < 20%
                "branch_reuse_count": 2,
            }
        ]
        result = analyze_pack_branch_naming_hygiene(records)
        assert result["total_packs"] == 1
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 0
        # All thresholds met: 0.30 + 0.25 + 0.25 + 0.20 = 1.0
        assert result["branch_naming_hygiene_score"] == 1.0
        assert result["avg_commits_per_pr"] == 3.0

    def test_single_pack_low_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-2",
                "total_branches_created": 10,
                "convention_compliant_branches": 2,  # 20% << 80%
                "task_aligned_branches": 1,  # 10% << 70%
                "pr_workflow_steps_followed": 2,
                "pr_workflow_steps_total": 10,  # 20% << 80%
                "total_commits": 50,
                "total_prs": 4,
                "oversized_prs": 3,  # 75% >> 10%
                "undersized_prs": 0,
                "branch_reuse_count": 0,
            }
        ]
        result = analyze_pack_branch_naming_hygiene(records)
        assert result["total_packs"] == 1
        assert result["high_quality_packs"] == 0
        assert result["low_quality_packs"] == 1
        assert result["branch_naming_hygiene_score"] < 0.4
        assert result["oversized_pr_rate"] == 75.0

    def test_multiple_packs_mixed(self) -> None:
        records = [
            {
                "pack_id": "high",
                "total_branches_created": 10,
                "convention_compliant_branches": 9,
                "task_aligned_branches": 8,
                "pr_workflow_steps_followed": 9,
                "pr_workflow_steps_total": 10,
                "total_commits": 30,
                "total_prs": 10,
                "oversized_prs": 0,
                "undersized_prs": 1,
                "branch_reuse_count": 1,
            },
            {
                "pack_id": "low",
                "total_branches_created": 10,
                "convention_compliant_branches": 1,
                "task_aligned_branches": 1,
                "pr_workflow_steps_followed": 1,
                "pr_workflow_steps_total": 10,
                "total_commits": 50,
                "total_prs": 4,
                "oversized_prs": 3,
                "undersized_prs": 0,
                "branch_reuse_count": 0,
            },
        ]
        result = analyze_pack_branch_naming_hygiene(records)
        assert result["total_packs"] == 2
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["total_branches_created"] == 20
        # Aggregate: 10/20 = 50% compliant
        assert result["convention_compliant_rate"] == 50.0

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "pack_id": "valid",
                "total_branches_created": 5,
                "convention_compliant_branches": 5,
                "task_aligned_branches": 4,
                "pr_workflow_steps_followed": 5,
                "pr_workflow_steps_total": 5,
                "total_commits": 10,
                "total_prs": 5,
                "oversized_prs": 0,
                "undersized_prs": 0,
                "branch_reuse_count": 0,
            },
        ]
        result = analyze_pack_branch_naming_hygiene(records)
        assert result["total_packs"] == 1
        assert result["total_branches_created"] == 5

    def test_zero_branches_pack(self) -> None:
        records = [
            {
                "pack_id": "empty",
                "total_branches_created": 0,
                "convention_compliant_branches": 0,
                "task_aligned_branches": 0,
                "pr_workflow_steps_followed": 0,
                "pr_workflow_steps_total": 0,
                "total_commits": 0,
                "total_prs": 0,
                "oversized_prs": 0,
                "undersized_prs": 0,
                "branch_reuse_count": 0,
            }
        ]
        result = analyze_pack_branch_naming_hygiene(records)
        assert result["total_packs"] == 1
        assert result["total_branches_created"] == 0
        assert result["convention_compliant_rate"] == 0.0
        assert result["avg_commits_per_pr"] == 0.0

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_branch_naming_hygiene([])
        assert set(result.keys()) == EXPECTED_KEYS
