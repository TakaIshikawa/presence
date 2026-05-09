"""Tests for pack commit discipline analyzer."""

import pytest

from synthesis.pack_commit_discipline import analyze_pack_commit_discipline


class TestAnalyzePackCommitDiscipline:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_commit_discipline([])

        assert result["total_packs"] == 0
        assert result["avg_total_commits"] == 0.0
        assert result["avg_commits_per_task"] == 0.0
        assert result["avg_conventional_format_rate"] == 0.0
        assert result["avg_clear_message_rate"] == 0.0
        assert result["avg_specific_staging_rate"] == 0.0
        assert result["avg_hook_pass_rate"] == 0.0
        assert result["packs_with_unauthorized_commands"] == 0
        assert result["commit_discipline_score"] == 0.0
        assert result["high_discipline_packs"] == 0
        assert result["low_discipline_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_commit_discipline(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_commit_discipline("not a list")

    def test_high_discipline_excellent_commits(self):
        """Verify high discipline with excellent commit messages and staging."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "high_discipline",
                "total_commits": 10,
                "total_tasks": 5,
                "conventional_commits": 9,
                "clear_message_commits": 10,
                "specific_staging_commits": 8,
                "bulk_staging_commits": 2,
                "hook_executions": 10,
                "hook_passes": 10,
                "unauthorized_force_push": 0,
                "unauthorized_reset_hard": 0,
                "coauthor_tagged_commits": 10,
            }
        ])

        assert result["avg_total_commits"] == 10.0
        # 10 / 5 = 2.0 commits per task
        assert result["avg_commits_per_task"] == 2.0
        # 9 / 10 = 90%
        assert result["avg_conventional_format_rate"] == 90.0
        # 10 / 10 = 100%
        assert result["avg_clear_message_rate"] == 100.0
        # 8 / (8 + 2) = 80%
        assert result["avg_specific_staging_rate"] == 80.0
        # 10 / 10 = 100%
        assert result["avg_hook_pass_rate"] == 100.0
        assert result["packs_with_unauthorized_commands"] == 0
        # 10 / 10 = 100%
        assert result["avg_coauthor_rate"] == 100.0
        assert result["commit_discipline_score"] > 80.0
        assert result["high_discipline_packs"] == 1

    def test_low_discipline_poor_commits(self):
        """Verify low discipline with poor messages and bulk staging."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "low_discipline",
                "total_commits": 15,
                "total_tasks": 5,
                "conventional_commits": 3,
                "clear_message_commits": 5,
                "specific_staging_commits": 2,
                "bulk_staging_commits": 13,
                "hook_executions": 5,
                "hook_passes": 2,
                "unauthorized_force_push": 1,
                "unauthorized_reset_hard": 0,
            }
        ])

        # 3 / 15 = 20%
        assert result["avg_conventional_format_rate"] == 20.0
        # 5 / 15 = 33.33%
        assert 33.0 <= result["avg_clear_message_rate"] <= 34.0
        # 2 / (2 + 13) = 13.33%
        assert 13.0 <= result["avg_specific_staging_rate"] <= 14.0
        # 2 / 5 = 40%
        assert result["avg_hook_pass_rate"] == 40.0
        assert result["packs_with_unauthorized_commands"] == 1
        assert result["commit_discipline_score"] < 50.0
        assert result["low_discipline_packs"] == 1

    def test_unauthorized_command_detection(self):
        """Verify unauthorized destructive command detection."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "force_push",
                "total_commits": 5,
                "unauthorized_force_push": 1,
            },
            {
                "pack_id": "reset_hard",
                "total_commits": 5,
                "unauthorized_reset_hard": 1,
            },
            {
                "pack_id": "safe",
                "total_commits": 5,
                "unauthorized_force_push": 0,
                "unauthorized_reset_hard": 0,
            },
        ])

        assert result["packs_with_unauthorized_commands"] == 2

    def test_commits_per_task_calculation(self):
        """Verify commits per task calculated correctly."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": 20,
                "total_tasks": 10,
            }
        ])

        # 20 / 10 = 2.0
        assert result["avg_commits_per_task"] == 2.0

    def test_message_quality_metrics(self):
        """Verify message quality metrics calculated correctly."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": 50,
                "conventional_commits": 40,
                "good_length_commits": 45,
                "clear_message_commits": 48,
            }
        ])

        # 40 / 50 = 80%
        assert result["avg_conventional_format_rate"] == 80.0
        # 45 / 50 = 90%
        assert result["avg_good_length_rate"] == 90.0
        # 48 / 50 = 96%
        assert result["avg_clear_message_rate"] == 96.0

    def test_staging_discipline_calculation(self):
        """Verify staging discipline calculated correctly."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": 20,
                "specific_staging_commits": 15,
                "bulk_staging_commits": 5,
            }
        ])

        # 15 / (15 + 5) = 75%
        assert result["avg_specific_staging_rate"] == 75.0

    def test_hook_compliance_calculation(self):
        """Verify hook pass rate calculated correctly."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": 10,
                "hook_executions": 10,
                "hook_passes": 9,
            }
        ])

        # 9 / 10 = 90%
        assert result["avg_hook_pass_rate"] == 90.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": 10,
                "conventional_commits": 9,
            },
            {
                "pack_id": "pack2",
                "total_commits": 20,
                "conventional_commits": 16,
            },
        ])

        assert result["total_packs"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_total_commits"] == 15.0
        # (90% + 80%) / 2 = 85%
        assert result["avg_conventional_format_rate"] == 85.0

    def test_discipline_score_excellent_all_metrics(self):
        """Verify discipline score with excellent metrics."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "excellent",
                "total_commits": 10,
                "conventional_commits": 9,  # 90% (25pts)
                "clear_message_commits": 10,  # 100% (25pts)
                "specific_staging_commits": 8,
                "bulk_staging_commits": 2,  # 80% (25pts)
                "hook_executions": 10,
                "hook_passes": 10,  # 100% (15pts)
                "unauthorized_force_push": 0,  # 10pts
            }
        ])

        # Should score: 25 + 25 + 25 + 15 + 10 = 100
        assert result["commit_discipline_score"] == 100.0
        assert result["high_discipline_packs"] == 1

    def test_discipline_score_poor_all_metrics(self):
        """Verify discipline score with poor metrics."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "poor",
                "total_commits": 10,
                "conventional_commits": 2,  # 20% (0pts)
                "clear_message_commits": 3,  # 30% (0pts)
                "specific_staging_commits": 1,
                "bulk_staging_commits": 9,  # 10% (0pts)
                "hook_executions": 5,
                "hook_passes": 2,  # 40% (0pts)
                "unauthorized_force_push": 1,  # 0pts
            }
        ])

        # Should score: 0 + 0 + 0 + 0 + 0 = 0
        assert result["commit_discipline_score"] == 0.0
        assert result["low_discipline_packs"] == 1

    def test_discipline_score_mixed_metrics(self):
        """Verify discipline score with mixed quality metrics."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "mixed",
                "total_commits": 20,
                "conventional_commits": 13,  # 65% (18pts)
                "clear_message_commits": 15,  # 75% (18pts)
                "specific_staging_commits": 11,
                "bulk_staging_commits": 9,  # 55% (18pts)
                "hook_executions": 10,
                "hook_passes": 9,  # 90% (10pts)
                "unauthorized_force_push": 0,  # 10pts
            }
        ])

        # Should score: 18 + 18 + 18 + 10 + 10 = 74
        assert result["commit_discipline_score"] == 74.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_commit_discipline([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_commits": 5,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "pack1",
                "total_commits": True,
                "conventional_commits": False,
            }
        ])

        assert result["avg_total_commits"] == 0.0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_commit_discipline([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_commits": 25,
                "total_tasks": 10,
                "conventional_commits": 22,
                "good_length_commits": 24,
                "clear_message_commits": 23,
                "specific_staging_commits": 20,
                "bulk_staging_commits": 5,
                "hook_executions": 25,
                "hook_passes": 25,
                "unauthorized_force_push": 0,
                "unauthorized_reset_hard": 0,
                "coauthor_tagged_commits": 25,
            }
        ])

        assert result["avg_total_commits"] == 25.0
        # 25 / 10 = 2.5
        assert result["avg_commits_per_task"] == 2.5
        # 22 / 25 = 88%
        assert result["avg_conventional_format_rate"] == 88.0
        # 20 / 25 = 80%
        assert result["avg_specific_staging_rate"] == 80.0
        # 25 / 25 = 100%
        assert result["avg_hook_pass_rate"] == 100.0
        # 25 / 25 = 100%
        assert result["avg_coauthor_rate"] == 100.0
        assert result["packs_with_unauthorized_commands"] == 0
        assert result["commit_discipline_score"] > 80.0
