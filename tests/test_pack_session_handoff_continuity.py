"""Tests for pack session handoff continuity analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_session_handoff_continuity import (
    analyze_pack_session_handoff_continuity,
)


class TestAnalyzePackSessionHandoffContinuity:
    """Tests for analyze_pack_session_handoff_continuity."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_session_handoff_continuity([])
        assert result["total_packs"] == 0
        assert result["total_handoffs"] == 0
        assert result["session_handoff_continuity_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_session_handoff_continuity(None)
        assert result["total_packs"] == 0
        assert result["session_handoff_continuity_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_session_handoff_continuity("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_session_handoff_continuity(42)

    def test_good_continuity_scores_high(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_handoffs": 5,
                "context_preserved": 5,
                "repeated_work_instances": 0,
                "progress_continued": 5,
                "total_sessions": 6,
            }
        ]
        result = analyze_pack_session_handoff_continuity(records)
        assert result["total_packs"] == 1
        assert result["high_quality_packs"] == 1
        assert result["session_handoff_continuity_score"] > 0.7

    def test_poor_continuity_scores_low(self) -> None:
        records = [
            {
                "pack_id": "p1",
                "total_handoffs": 5,
                "context_preserved": 1,
                "repeated_work_instances": 4,
                "progress_continued": 1,
                "total_sessions": 6,
            }
        ]
        result = analyze_pack_session_handoff_continuity(records)
        assert result["total_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["session_handoff_continuity_score"] < 0.4

    def test_no_handoffs_gets_full_score(self) -> None:
        records = [
            {
                "pack_id": "single_session",
                "total_handoffs": 0,
                "context_preserved": 0,
                "repeated_work_instances": 0,
                "progress_continued": 0,
                "total_sessions": 1,
            }
        ]
        result = analyze_pack_session_handoff_continuity(records)
        assert result["session_handoff_continuity_score"] == 1.0
        assert result["high_quality_packs"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            None,
            {
                "pack_id": "valid",
                "total_handoffs": 2,
                "context_preserved": 2,
                "repeated_work_instances": 0,
                "progress_continued": 2,
                "total_sessions": 3,
            },
        ]
        result = analyze_pack_session_handoff_continuity(records)
        assert result["total_packs"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_session_handoff_continuity([])
        expected_keys = {
            "total_packs",
            "total_handoffs",
            "context_preserved",
            "context_preserved_rate",
            "repeated_work_instances",
            "repeated_work_rate",
            "progress_continued",
            "progress_continued_rate",
            "avg_sessions_per_pack",
            "high_quality_packs",
            "low_quality_packs",
            "session_handoff_continuity_score",
        }
        assert set(result.keys()) == expected_keys
