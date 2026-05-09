"""Tests for session Task delegation analyzer."""

import pytest

from synthesis.session_task_delegation import analyze_session_task_delegation


class TestAnalyzeSessionTaskDelegation:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_task_delegation([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_task_tool"] == 0
        assert result["avg_task_calls"] == 0.0
        assert result["avg_bash_agent_ratio"] == 0.0
        assert result["avg_explore_agent_ratio"] == 0.0
        assert result["avg_plan_agent_ratio"] == 0.0
        assert result["avg_general_agent_ratio"] == 0.0
        assert result["avg_other_agent_ratio"] == 0.0
        assert result["avg_max_delegation_depth"] == 0.0
        assert result["avg_delegation_depth"] == 0.0
        assert result["avg_bash_success_rate"] == 0.0
        assert result["avg_explore_success_rate"] == 0.0
        assert result["avg_plan_success_rate"] == 0.0
        assert result["avg_general_success_rate"] == 0.0
        assert result["avg_selection_appropriateness"] == 0.0
        assert result["avg_resume_ratio"] == 0.0
        assert result["avg_task_duration"] == 0.0
        assert result["delegation_discipline_score"] == 0.0
        assert result["high_discipline_sessions"] == 0
        assert result["low_discipline_sessions"] == 0
        assert result["deep_delegation_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_task_delegation(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_task_delegation("not a list")

    def test_session_with_no_task_calls(self):
        """Verify session with zero Task calls handled gracefully."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_task_tool"] == 0

    def test_high_discipline_flat_delegation(self):
        """Verify high discipline with flat delegation and appropriate selection."""
        result = analyze_session_task_delegation([
            {
                "session_id": "high_discipline",
                "total_task_calls": 20,
                "bash_agent_calls": 8,
                "explore_agent_calls": 7,
                "plan_agent_calls": 3,
                "general_agent_calls": 2,
                "max_delegation_depth": 1,
                "avg_delegation_depth": 1.0,
                "bash_success_count": 8,
                "explore_success_count": 7,
                "plan_success_count": 3,
                "general_success_count": 2,
                "appropriate_selections": 18,
                "resume_calls": 2,
                "avg_task_duration_seconds": 45.0,
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 20.0
        # 8 / 20 = 40%
        assert result["avg_bash_agent_ratio"] == 40.0
        # 7 / 20 = 35%
        assert result["avg_explore_agent_ratio"] == 35.0
        # Max depth = 1
        assert result["avg_max_delegation_depth"] == 1.0
        # All successful: 20 / 20 = 100%
        # 18 / 20 = 90% appropriate
        assert result["avg_selection_appropriateness"] == 90.0
        # 2 / 20 = 10% resume
        assert result["avg_resume_ratio"] == 10.0
        # Should have high discipline score
        assert result["delegation_discipline_score"] > 80.0
        assert result["high_discipline_sessions"] == 1
        assert result["deep_delegation_sessions"] == 0

    def test_low_discipline_deep_delegation(self):
        """Verify low discipline with deep delegation and poor selection."""
        result = analyze_session_task_delegation([
            {
                "session_id": "low_discipline",
                "total_task_calls": 15,
                "bash_agent_calls": 3,
                "explore_agent_calls": 2,
                "plan_agent_calls": 5,
                "general_agent_calls": 5,
                "max_delegation_depth": 4,
                "avg_delegation_depth": 2.8,
                "bash_success_count": 1,
                "explore_success_count": 1,
                "plan_success_count": 2,
                "general_success_count": 2,
                "appropriate_selections": 5,
                "resume_calls": 10,
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        # Max depth = 4
        assert result["avg_max_delegation_depth"] == 4.0
        # Overall success: 6 / 15 = 40%
        # 5 / 15 = 33.33% appropriate
        assert 33.0 <= result["avg_selection_appropriateness"] <= 34.0
        # 10 / 15 = 66.67% resume
        assert 66.0 <= result["avg_resume_ratio"] <= 67.0
        # Should have low discipline score
        assert result["delegation_discipline_score"] < 50.0
        assert result["low_discipline_sessions"] == 1
        assert result["deep_delegation_sessions"] == 1

    def test_agent_type_distribution(self):
        """Verify agent type distribution calculated correctly."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 100,
                "bash_agent_calls": 30,
                "explore_agent_calls": 25,
                "plan_agent_calls": 20,
                "general_agent_calls": 15,
                "other_agent_calls": 10,
            }
        ])

        # 30 / 100 = 30%
        assert result["avg_bash_agent_ratio"] == 30.0
        # 25 / 100 = 25%
        assert result["avg_explore_agent_ratio"] == 25.0
        # 20 / 100 = 20%
        assert result["avg_plan_agent_ratio"] == 20.0
        # 15 / 100 = 15%
        assert result["avg_general_agent_ratio"] == 15.0
        # 10 / 100 = 10%
        assert result["avg_other_agent_ratio"] == 10.0

    def test_success_rates_by_agent_type(self):
        """Verify success rates calculated per agent type."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 50,
                "bash_agent_calls": 20,
                "explore_agent_calls": 15,
                "plan_agent_calls": 10,
                "general_agent_calls": 5,
                "bash_success_count": 18,
                "explore_success_count": 12,
                "plan_success_count": 9,
                "general_success_count": 4,
            }
        ])

        # 18 / 20 = 90%
        assert result["avg_bash_success_rate"] == 90.0
        # 12 / 15 = 80%
        assert result["avg_explore_success_rate"] == 80.0
        # 9 / 10 = 90%
        assert result["avg_plan_success_rate"] == 90.0
        # 4 / 5 = 80%
        assert result["avg_general_success_rate"] == 80.0

    def test_delegation_depth_tracking(self):
        """Verify delegation depth metrics calculated correctly."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "max_delegation_depth": 3,
                "avg_delegation_depth": 1.8,
            }
        ])

        assert result["avg_max_delegation_depth"] == 3.0
        assert result["avg_delegation_depth"] == 1.8
        assert result["deep_delegation_sessions"] == 1

    def test_selection_appropriateness_calculation(self):
        """Verify selection appropriateness calculated correctly."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 25,
                "appropriate_selections": 20,
            }
        ])

        # 20 / 25 = 80%
        assert result["avg_selection_appropriateness"] == 80.0

    def test_resume_ratio_calculation(self):
        """Verify resume usage ratio calculated correctly."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 30,
                "resume_calls": 6,
            }
        ])

        # 6 / 30 = 20%
        assert result["avg_resume_ratio"] == 20.0

    def test_task_duration_tracking(self):
        """Verify task duration tracked correctly."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "avg_task_duration_seconds": 62.5,
            }
        ])

        assert result["avg_task_duration"] == 62.5

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 20,
                "bash_agent_calls": 10,
                "max_delegation_depth": 1,
            },
            {
                "session_id": "session2",
                "total_task_calls": 30,
                "bash_agent_calls": 15,
                "max_delegation_depth": 2,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_task_tool"] == 2
        # (20 + 30) / 2 = 25
        assert result["avg_task_calls"] == 25.0
        # Both 50% bash
        assert result["avg_bash_agent_ratio"] == 50.0
        # (1 + 2) / 2 = 1.5
        assert result["avg_max_delegation_depth"] == 1.5

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_task_delegation([
            "not a dict",
            {
                "session_id": "session1",
                "total_task_calls": 10,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": True,
                "bash_agent_calls": False,
            }
        ])

        assert result["sessions_with_task_tool"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_bash_agent_ratio"] == 0.0

    def test_success_rate_with_zero_calls(self):
        """Verify success rate not calculated when agent type has zero calls."""
        result = analyze_session_task_delegation([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "bash_agent_calls": 0,
                "bash_success_count": 0,
            }
        ])

        assert result["avg_bash_success_rate"] == 0.0

    def test_discipline_score_excellent_all_metrics(self):
        """Verify discipline score with excellent metrics."""
        result = analyze_session_task_delegation([
            {
                "session_id": "excellent",
                "total_task_calls": 20,
                "bash_agent_calls": 10,
                "explore_agent_calls": 10,
                "max_delegation_depth": 1,  # 35pts
                "bash_success_count": 9,
                "explore_success_count": 9,  # 90% success = 25pts
                "appropriate_selections": 18,  # 90% = 30pts
                "resume_calls": 2,  # 10% = 10pts
            }
        ])

        # Should score: 35 + 30 + 25 + 10 = 100
        assert result["delegation_discipline_score"] == 100.0
        assert result["high_discipline_sessions"] == 1

    def test_discipline_score_poor_all_metrics(self):
        """Verify discipline score with poor metrics."""
        result = analyze_session_task_delegation([
            {
                "session_id": "poor",
                "total_task_calls": 20,
                "bash_agent_calls": 10,
                "general_agent_calls": 10,
                "max_delegation_depth": 5,  # 0pts
                "bash_success_count": 4,
                "general_success_count": 5,  # 45% success = 0pts
                "appropriate_selections": 8,  # 40% = 0pts
                "resume_calls": 15,  # 75% = 0pts
            }
        ])

        # Should score: 0 + 0 + 0 + 0 = 0
        assert result["delegation_discipline_score"] == 0.0
        assert result["low_discipline_sessions"] == 1

    def test_discipline_score_mixed_metrics(self):
        """Verify discipline score with mixed quality metrics."""
        result = analyze_session_task_delegation([
            {
                "session_id": "mixed",
                "total_task_calls": 20,
                "bash_agent_calls": 10,
                "explore_agent_calls": 10,
                "max_delegation_depth": 2,  # 25pts
                "bash_success_count": 7,
                "explore_success_count": 7,  # 70% success = 20pts
                "appropriate_selections": 14,  # 70% = 20pts
                "resume_calls": 7,  # 35% = 7pts
            }
        ])

        # Should score: 25 + 20 + 20 + 7 = 72
        assert result["delegation_discipline_score"] == 72.0

    def test_boundary_discipline_classification(self):
        """Verify boundary cases for discipline classification."""
        result = analyze_session_task_delegation([
            # Exactly 80: 25 (depth) + 30 (appropriateness) + 25 (success) + 0 (resume) = 80
            {
                "session_id": "s1",
                "total_task_calls": 20,
                "bash_agent_calls": 20,
                "max_delegation_depth": 2,  # 25pts
                "bash_success_count": 17,  # 85% = 25pts
                "appropriate_selections": 16,  # 80% = 30pts
                "resume_calls": 15,  # 75% = 0pts
            },
            # Just above 80: 35 (depth) + 30 (appropriateness) + 25 (success) + 0 (resume) = 90
            {
                "session_id": "s2",
                "total_task_calls": 20,
                "bash_agent_calls": 20,
                "max_delegation_depth": 1,  # 35pts
                "bash_success_count": 17,  # 85% = 25pts
                "appropriate_selections": 16,  # 80% = 30pts
                "resume_calls": 15,  # 75% = 0pts
            },
            # Exactly 50: 15 (depth) + 20 (appropriateness) + 15 (success) + 0 (resume) = 50
            {
                "session_id": "s3",
                "total_task_calls": 20,
                "bash_agent_calls": 20,
                "max_delegation_depth": 3,  # 15pts
                "bash_success_count": 12,  # 60% = 15pts
                "appropriate_selections": 13,  # 65% = 20pts
                "resume_calls": 15,  # 75% = 0pts
            },
            # Below 50: 0 (depth) + 10 (appropriateness) + 0 (success) + 4 (resume) = 14
            {
                "session_id": "s4",
                "total_task_calls": 20,
                "bash_agent_calls": 20,
                "max_delegation_depth": 5,  # 0pts
                "bash_success_count": 10,  # 50% = 0pts
                "appropriate_selections": 10,  # 50% = 10pts
                "resume_calls": 11,  # 55% = 4pts
            },
        ])

        # >80 means strictly greater
        assert result["high_discipline_sessions"] == 1
        # <50 means strictly less
        assert result["low_discipline_sessions"] == 1

    def test_deep_delegation_detection(self):
        """Verify deep delegation sessions detected (max depth >2)."""
        result = analyze_session_task_delegation([
            {
                "session_id": "shallow",
                "total_task_calls": 10,
                "max_delegation_depth": 2,
            },
            {
                "session_id": "deep",
                "total_task_calls": 10,
                "max_delegation_depth": 3,
            },
        ])

        assert result["deep_delegation_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_task_delegation([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_task_calls": 50,
                "bash_agent_calls": 20,
                "explore_agent_calls": 15,
                "plan_agent_calls": 10,
                "general_agent_calls": 3,
                "other_agent_calls": 2,
                "max_delegation_depth": 1,
                "avg_delegation_depth": 1.0,
                "bash_success_count": 19,
                "explore_success_count": 14,
                "plan_success_count": 9,
                "general_success_count": 3,
                "appropriate_selections": 45,
                "resume_calls": 5,
                "avg_task_duration_seconds": 48.5,
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 50.0
        # 20 / 50 = 40%
        assert result["avg_bash_agent_ratio"] == 40.0
        # 15 / 50 = 30%
        assert result["avg_explore_agent_ratio"] == 30.0
        # Max depth = 1
        assert result["avg_max_delegation_depth"] == 1.0
        # 19 / 20 = 95%
        assert result["avg_bash_success_rate"] == 95.0
        # 45 / 50 = 90%
        assert result["avg_selection_appropriateness"] == 90.0
        # 5 / 50 = 10%
        assert result["avg_resume_ratio"] == 10.0
        assert result["avg_task_duration"] == 48.5
        # Should have high discipline
        assert result["delegation_discipline_score"] > 80.0
