"""Tests for session commit message quality analyzer."""

import pytest

from synthesis.session_commit_message_quality import (
    analyze_session_commit_message_quality,
)


class TestAnalyzeSessionCommitMessageQuality:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_session_commit_message_quality([])
        assert result["total_sessions"] == 0
        assert result["total_commits"] == 0
        assert result["commit_quality_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_session_commit_message_quality("not a list")

    def test_high_quality_commits(self):
        """Verify high quality commits yield high score."""
        records = [
            {
                "total_commits": 5,
                "conventional_format_commits": 5,
                "commits_with_claude_coauthor": 0,
                "commits_with_why_focus": 4,
                "concise_commits": 5,
                "accurate_add_verbs": 2,
                "inaccurate_add_verbs": 0,
                "commits_with_prior_status": 5,
                "commits_with_prior_diff": 5,
                "commits_using_add_all": 0,
                "commits_with_specific_staging": 5,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["conventional_format_rate"] == 100.0
        assert result["claude_coauthor_violation_rate"] == 0.0
        assert result["commit_quality_score"] > 0.85

    def test_claude_coauthor_violation_detection(self):
        """Verify Claude co-author violation tracking."""
        records = [
            {
                "total_commits": 10,
                "commits_with_claude_coauthor": 3,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["claude_coauthor_violation_rate"] == 30.0

    def test_conventional_format_compliance(self):
        """Verify conventional format tracking."""
        records = [
            {
                "total_commits": 10,
                "conventional_format_commits": 9,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["conventional_format_rate"] == 90.0

    def test_why_focus_rate(self):
        """Verify 'why' focus tracking."""
        records = [
            {
                "total_commits": 10,
                "commits_with_why_focus": 8,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["why_focus_rate"] == 80.0

    def test_verb_accuracy_calculation(self):
        """Verify change verb accuracy."""
        records = [
            {
                "accurate_add_verbs": 5,
                "inaccurate_add_verbs": 2,
                "accurate_update_verbs": 3,
                "accurate_fix_verbs": 4,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        # (5+3+4)/(5+2+3+4) = 12/14 = 85.71%
        assert result["verb_accuracy_rate"] == 85.71

    def test_pre_commit_workflow_tracking(self):
        """Verify pre-commit workflow tracking."""
        records = [
            {
                "total_commits": 10,
                "commits_with_prior_status": 9,
                "commits_with_prior_diff": 8,
                "commits_with_prior_log": 5,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["pre_commit_status_rate"] == 90.0
        assert result["pre_commit_diff_rate"] == 80.0
        assert result["pre_commit_log_rate"] == 50.0

    def test_staging_discipline_tracking(self):
        """Verify staging discipline tracking."""
        records = [
            {
                "commits_using_add_all": 2,
                "commits_with_specific_staging": 8,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["specific_staging_rate"] == 80.0
        assert result["git_add_all_rate"] == 20.0

    def test_high_quality_sessions_count(self):
        """Verify high quality sessions are counted."""
        records = [
            {
                "total_commits": 5,
                "conventional_format_commits": 5,
                "commits_with_claude_coauthor": 0,
                "commits_with_why_focus": 4,
                "accurate_add_verbs": 3,
                "inaccurate_add_verbs": 0,
                "commits_with_prior_status": 5,
                "commits_with_prior_diff": 5,
                "commits_using_add_all": 0,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["high_quality_sessions"] == 1

    def test_low_quality_sessions_count(self):
        """Verify low quality sessions are counted."""
        records = [
            {
                "total_commits": 10,
                "conventional_format_commits": 2,
                "commits_with_claude_coauthor": 8,
                "commits_with_why_focus": 1,
                "accurate_add_verbs": 1,
                "inaccurate_add_verbs": 5,
                "commits_with_prior_status": 2,
                "commits_with_prior_diff": 1,
                "commits_using_add_all": 9,
            }
        ]
        result = analyze_session_commit_message_quality(records)
        assert result["low_quality_sessions"] == 1
