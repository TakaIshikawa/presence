"""Tests for pack commit scope discipline analyzer."""

from __future__ import annotations

import pytest

from synthesis.pack_commit_scope_discipline import analyze_pack_commit_scope_discipline


class TestAnalyzePackCommitScopeDiscipline:
    """Tests for analyze_pack_commit_scope_discipline."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_commit_scope_discipline([])
        assert result["total_packs"] == 0
        assert result["total_commits"] == 0
        assert result["commit_scope_discipline_score"] == 0.0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_pack_commit_scope_discipline(None)
        assert result["total_packs"] == 0
        assert result["total_commits"] == 0
        assert result["commit_scope_discipline_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_commit_scope_discipline("not a list")
        with pytest.raises(ValueError, match="records must be a list of pack dictionaries"):
            analyze_pack_commit_scope_discipline(42)

    def test_single_pack_high_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-1",
                "total_commits": 10,
                "single_concern_commits": 9,
                "multi_concern_commits": 1,
                "avg_diff_lines": 80.0,
                "large_diff_commits": 0,
                "commits_with_tests": 8,
                "total_testable_commits": 10,
                "avg_files_per_commit": 2.5,
                "descriptive_message_commits": 9,
            }
        ]
        result = analyze_pack_commit_scope_discipline(records)
        assert result["total_packs"] == 1
        assert result["total_commits"] == 10
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 0
        assert result["commit_scope_discipline_score"] > 0.7

    def test_single_pack_low_quality(self) -> None:
        records = [
            {
                "pack_id": "pack-1",
                "total_commits": 10,
                "single_concern_commits": 2,
                "multi_concern_commits": 8,
                "avg_diff_lines": 500.0,
                "large_diff_commits": 8,
                "commits_with_tests": 0,
                "total_testable_commits": 10,
                "avg_files_per_commit": 15.0,
                "descriptive_message_commits": 1,
            }
        ]
        result = analyze_pack_commit_scope_discipline(records)
        assert result["total_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["high_quality_packs"] == 0
        assert result["commit_scope_discipline_score"] < 0.4

    def test_multiple_packs_mixed(self) -> None:
        records = [
            {
                "pack_id": "pack-high",
                "total_commits": 10,
                "single_concern_commits": 9,
                "multi_concern_commits": 1,
                "avg_diff_lines": 80.0,
                "large_diff_commits": 0,
                "commits_with_tests": 8,
                "total_testable_commits": 10,
                "avg_files_per_commit": 2.0,
                "descriptive_message_commits": 9,
            },
            {
                "pack_id": "pack-low",
                "total_commits": 10,
                "single_concern_commits": 1,
                "multi_concern_commits": 9,
                "avg_diff_lines": 600.0,
                "large_diff_commits": 9,
                "commits_with_tests": 0,
                "total_testable_commits": 10,
                "avg_files_per_commit": 20.0,
                "descriptive_message_commits": 1,
            },
        ]
        result = analyze_pack_commit_scope_discipline(records)
        assert result["total_packs"] == 2
        assert result["high_quality_packs"] == 1
        assert result["low_quality_packs"] == 1
        assert result["total_commits"] == 20

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "pack_id": "pack-1",
                "total_commits": 5,
                "single_concern_commits": 4,
                "multi_concern_commits": 1,
                "avg_diff_lines": 50.0,
                "large_diff_commits": 0,
                "commits_with_tests": 3,
                "total_testable_commits": 5,
                "avg_files_per_commit": 2.0,
                "descriptive_message_commits": 4,
            },
        ]
        result = analyze_pack_commit_scope_discipline(records)
        assert result["total_packs"] == 1
        assert result["total_commits"] == 5

    def test_zero_commits_pack(self) -> None:
        records = [
            {
                "pack_id": "pack-empty",
                "total_commits": 0,
                "single_concern_commits": 0,
                "multi_concern_commits": 0,
                "avg_diff_lines": 0.0,
                "large_diff_commits": 0,
                "commits_with_tests": 0,
                "total_testable_commits": 0,
                "avg_files_per_commit": 0.0,
                "descriptive_message_commits": 0,
            }
        ]
        result = analyze_pack_commit_scope_discipline(records)
        assert result["total_packs"] == 1
        assert result["total_commits"] == 0
        assert result["commit_scope_discipline_score"] == 0.0
        assert result["low_quality_packs"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_pack_commit_scope_discipline([])
        expected_keys = {
            "total_packs",
            "total_commits",
            "single_concern_rate",
            "multi_concern_rate",
            "avg_diff_lines",
            "large_diff_rate",
            "test_inclusion_rate",
            "avg_files_per_commit",
            "descriptive_message_rate",
            "high_quality_packs",
            "low_quality_packs",
            "commit_scope_discipline_score",
        }
        assert set(result.keys()) == expected_keys
