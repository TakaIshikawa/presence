"""Tests for pack execution plan adherence analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_execution_plan_adherence import analyze_pack_execution_plan_adherence


class TestAnalyzePackExecutionPlanAdherence:
    """Tests for analyze_pack_execution_plan_adherence."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_execution_plan_adherence([])
        assert result["total_packs"] == 0
        assert result["total_planned_files"] == 0
        assert result["execution_plan_adherence_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_execution_plan_adherence(None)
        assert result["total_packs"] == 0
        assert result["execution_plan_adherence_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_execution_plan_adherence("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_execution_plan_adherence(42)

    def test_exact_match_pack_scores_high(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_planned_files": 5,
                "total_actual_files_changed": 5,
                "planned_files_hit": 5,
                "unplanned_files": 0,
            }
        ]
        result = analyze_pack_execution_plan_adherence(records)
        assert result["total_packs"] == 1
        assert result["exact_match_count"] == 1
        assert result["high_quality_packs"] == 1
        assert result["execution_plan_adherence_score"] > 0.7

    def test_scope_creep_pack_scores_low(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_planned_files": 3,
                "total_actual_files_changed": 10,
                "planned_files_hit": 1,
                "unplanned_files": 9,
            }
        ]
        result = analyze_pack_execution_plan_adherence(records)
        assert result["total_packs"] == 1
        assert result["scope_creep_count"] == 1
        assert result["low_quality_packs"] == 1
        assert result["execution_plan_adherence_score"] < 0.4

    def test_multiple_packs_mixed(self) -> None:
        records = [
            {
                "pack_id": "exact",
                "total_planned_files": 4,
                "total_actual_files_changed": 4,
                "planned_files_hit": 4,
                "unplanned_files": 0,
            },
            {
                "pack_id": "creep",
                "total_planned_files": 3,
                "total_actual_files_changed": 10,
                "planned_files_hit": 1,
                "unplanned_files": 9,
            },
        ]
        result = analyze_pack_execution_plan_adherence(records)
        assert result["total_packs"] == 2
        assert result["exact_match_count"] == 1
        assert result["scope_creep_count"] == 1

    def test_zero_planned_files_gets_full_score(self) -> None:
        records = [
            {
                "pack_id": "no_plan",
                "total_planned_files": 0,
                "total_actual_files_changed": 3,
                "planned_files_hit": 0,
                "unplanned_files": 3,
            }
        ]
        result = analyze_pack_execution_plan_adherence(records)
        assert result["execution_plan_adherence_score"] == 1.0

    def test_skips_non_mapping_records(self) -> None:
        records = ["not a dict", None, {"total_planned_files": 2, "total_actual_files_changed": 2, "planned_files_hit": 2, "unplanned_files": 0}]
        result = analyze_pack_execution_plan_adherence(records)
        assert result["total_packs"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_execution_plan_adherence([])
        expected_keys = {
            "total_packs",
            "total_planned_files",
            "total_actual_files_changed",
            "planned_file_hit_rate",
            "unplanned_file_rate",
            "scope_creep_count",
            "scope_creep_rate",
            "underdelivery_count",
            "underdelivery_rate",
            "exact_match_count",
            "exact_match_rate",
            "high_quality_packs",
            "low_quality_packs",
            "execution_plan_adherence_score",
        }
        assert set(result.keys()) == expected_keys
