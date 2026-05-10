"""Tests for pack idle detection analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_idle_detection import analyze_pack_idle_detection


class TestAnalyzePackIdleDetection:
    """Tests for analyze_pack_idle_detection."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_idle_detection([])
        assert result["total_packs"] == 0
        assert result["total_sessions"] == 0
        assert result["idle_detection_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_idle_detection(None)
        assert result["total_packs"] == 0
        assert result["total_sessions"] == 0
        assert result["idle_detection_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_idle_detection("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_idle_detection(42)

    def test_single_pack_high_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-1",
                "total_sessions": 20,
                "idle_periods_detected": 5,
                "idle_periods_recovered": 5,
                "avg_idle_duration_seconds": 30.0,
                "stalled_sessions": 0,
                "recovered_stalled_sessions": 0,
                "proactive_interventions": 4,
                "total_session_duration_seconds": 10000,
                "wasted_idle_seconds": 100,
            }
        ]
        result = analyze_pack_idle_detection(records)
        assert result["total_packs"] == 1
        assert result["total_sessions"] == 20
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 0
        assert result["idle_detection_score"] > 0.7

    def test_single_pack_low_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-1",
                "total_sessions": 20,
                "idle_periods_detected": 10,
                "idle_periods_recovered": 1,
                "avg_idle_duration_seconds": 600.0,
                "stalled_sessions": 15,
                "recovered_stalled_sessions": 1,
                "proactive_interventions": 0,
                "total_session_duration_seconds": 10000,
                "wasted_idle_seconds": 3000,
            }
        ]
        result = analyze_pack_idle_detection(records)
        assert result["total_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["high_quality_packs"] == 0
        assert result["idle_detection_score"] < 0.4

    def test_multiple_packs_mixed(self) -> None:
        records = [
            {
                "pack_id": "pack-high",
                "total_sessions": 20,
                "idle_periods_detected": 5,
                "idle_periods_recovered": 5,
                "avg_idle_duration_seconds": 30.0,
                "stalled_sessions": 0,
                "recovered_stalled_sessions": 0,
                "proactive_interventions": 4,
                "total_session_duration_seconds": 10000,
                "wasted_idle_seconds": 100,
            },
            {
                "pack_id": "pack-low",
                "total_sessions": 20,
                "idle_periods_detected": 10,
                "idle_periods_recovered": 1,
                "avg_idle_duration_seconds": 600.0,
                "stalled_sessions": 15,
                "recovered_stalled_sessions": 1,
                "proactive_interventions": 0,
                "total_session_duration_seconds": 10000,
                "wasted_idle_seconds": 3000,
            },
        ]
        result = analyze_pack_idle_detection(records)
        assert result["total_packs"] == 2
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["total_sessions"] == 40

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "pack_id": "pack-1",
                "total_sessions": 10,
                "idle_periods_detected": 3,
                "idle_periods_recovered": 3,
                "avg_idle_duration_seconds": 20.0,
                "stalled_sessions": 0,
                "recovered_stalled_sessions": 0,
                "proactive_interventions": 2,
                "total_session_duration_seconds": 5000,
                "wasted_idle_seconds": 50,
            },
        ]
        result = analyze_pack_idle_detection(records)
        assert result["total_packs"] == 1
        assert result["total_sessions"] == 10

    def test_zero_sessions_pack(self) -> None:
        records = [
            {
                "pack_id": "pack-empty",
                "total_sessions": 0,
                "idle_periods_detected": 0,
                "idle_periods_recovered": 0,
                "avg_idle_duration_seconds": 0.0,
                "stalled_sessions": 0,
                "recovered_stalled_sessions": 0,
                "proactive_interventions": 0,
                "total_session_duration_seconds": 0,
                "wasted_idle_seconds": 0,
            }
        ]
        result = analyze_pack_idle_detection(records)
        assert result["total_packs"] == 1
        assert result["total_sessions"] == 0
        assert result["idle_detection_score"] == 0.0
        assert result["low_quality_packs"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_idle_detection([])
        expected_keys = {
            "total_packs",
            "total_sessions",
            "idle_periods_detected",
            "idle_recovery_rate",
            "avg_idle_duration_seconds",
            "stalled_session_rate",
            "stall_recovery_rate",
            "proactive_intervention_rate",
            "idle_waste_percentage",
            "high_quality_packs",
            "low_quality_packs",
            "idle_detection_score",
        }
        assert set(result.keys()) == expected_keys
