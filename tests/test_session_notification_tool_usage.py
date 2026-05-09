"""Tests for session notification tool usage analyzer."""

import pytest

from synthesis.session_notification_tool_usage import (
    analyze_session_notification_tool_usage,
)


class TestAnalyzeSessionNotificationToolUsage:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_notification_tool_usage([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_questions"] == 0
        assert result["avg_questions_per_session"] == 0.0
        assert result["avg_questions_per_task"] == 0.0
        assert result["avg_plan_mode_ratio"] == 0.0
        assert result["avg_execution_mode_ratio"] == 0.0
        assert result["avg_early_task_ratio"] == 0.0
        assert result["avg_mid_task_ratio"] == 0.0
        assert result["avg_late_task_ratio"] == 0.0
        assert result["avg_blocking_ratio"] == 0.0
        assert result["avg_parallel_work_ratio"] == 0.0
        assert result["avg_response_time_seconds"] == 0.0
        assert result["avg_max_response_time_seconds"] == 0.0
        assert result["avg_timeout_ratio"] == 0.0
        assert result["avg_wait_time_impact"] == 0.0
        assert result["high_blocking_sessions"] == 0
        assert result["low_blocking_sessions"] == 0
        assert result["high_wait_impact_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_notification_tool_usage(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_notification_tool_usage("not a list")

    def test_session_with_no_questions(self):
        """Verify session with zero AskUserQuestion calls handled gracefully."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 0,
                "total_tasks": 5,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_questions"] == 0
        assert result["avg_questions_per_session"] == 0.0

    def test_session_with_single_question(self):
        """Verify session with one question."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 1,
                "total_tasks": 2,
                "plan_mode_questions": 1,
                "execution_mode_questions": 0,
                "blocking_questions": 0,
                "parallel_work_questions": 1,
                "avg_response_time_seconds": 30,
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 1.0
        assert result["avg_questions_per_task"] == 0.5
        assert result["avg_plan_mode_ratio"] == 100.0
        assert result["avg_execution_mode_ratio"] == 0.0
        assert result["avg_blocking_ratio"] == 0.0
        assert result["avg_parallel_work_ratio"] == 100.0
        assert result["avg_response_time_seconds"] == 30.0

    def test_multiple_rapid_questions(self):
        """Verify session with multiple rapid questions in quick succession."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
                "total_tasks": 1,
                "early_task_questions": 5,
                "mid_task_questions": 0,
                "late_task_questions": 0,
                "avg_response_time_seconds": 10,
                "max_response_time_seconds": 15,
            }
        ])

        assert result["avg_questions_per_task"] == 5.0
        assert result["avg_early_task_ratio"] == 100.0
        assert result["avg_mid_task_ratio"] == 0.0
        assert result["avg_late_task_ratio"] == 0.0
        assert result["avg_response_time_seconds"] == 10.0
        assert result["avg_max_response_time_seconds"] == 15.0

    def test_questions_that_timeout(self):
        """Verify detection of questions that exceed response time threshold."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "timed_out_questions": 2,
                "avg_response_time_seconds": 600,
                "max_response_time_seconds": 1200,
            }
        ])

        # 2 / 10 = 20%
        assert result["avg_timeout_ratio"] == 20.0
        assert result["avg_response_time_seconds"] == 600.0
        assert result["avg_max_response_time_seconds"] == 1200.0

    def test_high_blocking_questions(self):
        """Verify detection of sessions with high blocking question ratio."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "blocking_questions": 8,
                "parallel_work_questions": 2,
            }
        ])

        # 8 / 10 = 80%
        assert result["avg_blocking_ratio"] == 80.0
        # 2 / 10 = 20%
        assert result["avg_parallel_work_ratio"] == 20.0
        assert result["high_blocking_sessions"] == 1
        assert result["low_blocking_sessions"] == 0

    def test_low_blocking_questions(self):
        """Verify detection of sessions with low blocking question ratio."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "blocking_questions": 1,
                "parallel_work_questions": 9,
            }
        ])

        # 1 / 10 = 10%
        assert result["avg_blocking_ratio"] == 10.0
        # 9 / 10 = 90%
        assert result["avg_parallel_work_ratio"] == 90.0
        assert result["high_blocking_sessions"] == 0
        assert result["low_blocking_sessions"] == 1

    def test_plan_mode_vs_execution_mode_distribution(self):
        """Verify plan mode vs execution mode question distribution."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 20,
                "plan_mode_questions": 15,
                "execution_mode_questions": 5,
            }
        ])

        # 15 / 20 = 75%
        assert result["avg_plan_mode_ratio"] == 75.0
        # 5 / 20 = 25%
        assert result["avg_execution_mode_ratio"] == 25.0

    def test_question_timing_distribution(self):
        """Verify question timing distribution within tasks."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 30,
                "early_task_questions": 18,
                "mid_task_questions": 9,
                "late_task_questions": 3,
            }
        ])

        # 18 / 30 = 60%
        assert result["avg_early_task_ratio"] == 60.0
        # 9 / 30 = 30%
        assert result["avg_mid_task_ratio"] == 30.0
        # 3 / 30 = 10%
        assert result["avg_late_task_ratio"] == 10.0

    def test_wait_time_impact_on_session_duration(self):
        """Verify calculation of wait time impact on session duration."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
                "session_duration_seconds": 1000,
                "question_wait_time_seconds": 400,
            }
        ])

        # 400 / 1000 = 40%
        assert result["avg_wait_time_impact"] == 40.0
        assert result["high_wait_impact_sessions"] == 1

    def test_low_wait_time_impact(self):
        """Verify sessions with low wait time impact."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
                "session_duration_seconds": 1000,
                "question_wait_time_seconds": 100,
            }
        ])

        # 100 / 1000 = 10%
        assert result["avg_wait_time_impact"] == 10.0
        assert result["high_wait_impact_sessions"] == 0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "total_tasks": 5,
                "blocking_questions": 2,
                "avg_response_time_seconds": 60,
            },
            {
                "session_id": "session2",
                "total_ask_user_questions": 20,
                "total_tasks": 10,
                "blocking_questions": 4,
                "avg_response_time_seconds": 120,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_questions"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_questions_per_session"] == 15.0
        # (10/5 + 20/10) / 2 = (2 + 2) / 2 = 2
        assert result["avg_questions_per_task"] == 2.0
        # (20% + 20%) / 2 = 20%
        assert result["avg_blocking_ratio"] == 20.0
        # (60 + 120) / 2 = 90
        assert result["avg_response_time_seconds"] == 90.0

    def test_boundary_blocking_classification(self):
        """Verify boundary cases for blocking classification."""
        result = analyze_session_notification_tool_usage([
            # Exactly 50% (should not be high)
            {
                "session_id": "s1",
                "total_ask_user_questions": 10,
                "blocking_questions": 5,
            },
            # Just above 50% (should be high)
            {
                "session_id": "s2",
                "total_ask_user_questions": 10,
                "blocking_questions": 6,
            },
            # Exactly 20% (should not be low)
            {
                "session_id": "s3",
                "total_ask_user_questions": 10,
                "blocking_questions": 2,
            },
            # Just below 20% (should be low)
            {
                "session_id": "s4",
                "total_ask_user_questions": 10,
                "blocking_questions": 1,
            },
        ])

        # >50% means strictly greater
        assert result["high_blocking_sessions"] == 1
        # <20% means strictly less
        assert result["low_blocking_sessions"] == 1

    def test_boundary_wait_impact_classification(self):
        """Verify boundary cases for wait impact classification."""
        result = analyze_session_notification_tool_usage([
            # Exactly 30% (should not be high)
            {
                "session_id": "s1",
                "total_ask_user_questions": 1,
                "session_duration_seconds": 1000,
                "question_wait_time_seconds": 300,
            },
            # Just above 30% (should be high)
            {
                "session_id": "s2",
                "total_ask_user_questions": 1,
                "session_duration_seconds": 1000,
                "question_wait_time_seconds": 350,
            },
        ])

        # >30% means strictly greater
        assert result["high_wait_impact_sessions"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_notification_tool_usage([
            "not a dict",
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": True,
                "blocking_questions": False,
            }
        ])

        assert result["sessions_with_questions"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_plan_mode_ratio"] == 0.0
        assert result["avg_blocking_ratio"] == 0.0

    def test_zero_questions_no_division_error(self):
        """Verify zero questions doesn't cause division errors."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 0,
                "blocking_questions": 0,
            }
        ])

        assert result["sessions_with_questions"] == 0
        assert result["avg_questions_per_session"] == 0.0

    def test_zero_session_duration_no_division_error(self):
        """Verify zero session duration doesn't cause division errors."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 5,
                "session_duration_seconds": 0,
                "question_wait_time_seconds": 100,
            }
        ])

        # Should not crash, wait time impact should be 0
        assert result["avg_wait_time_impact"] == 0.0

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10.5,
                "avg_response_time_seconds": 45.75,
                "session_duration_seconds": 1000.0,
                "question_wait_time_seconds": 200.5,
            }
        ])

        assert result["avg_questions_per_session"] == 10.5
        assert result["avg_response_time_seconds"] == 45.75
        assert result["avg_wait_time_impact"] == 20.05

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_ask_user_questions": 25,
                "total_tasks": 5,
                "plan_mode_questions": 18,
                "execution_mode_questions": 7,
                "early_task_questions": 15,
                "mid_task_questions": 8,
                "late_task_questions": 2,
                "blocking_questions": 5,
                "parallel_work_questions": 20,
                "avg_response_time_seconds": 90,
                "max_response_time_seconds": 300,
                "timed_out_questions": 1,
                "session_duration_seconds": 2000,
                "question_wait_time_seconds": 500,
            }
        ])

        assert result["sessions_with_questions"] == 1
        assert result["avg_questions_per_session"] == 25.0
        # 25 / 5 = 5
        assert result["avg_questions_per_task"] == 5.0
        # 18 / 25 = 72%
        assert result["avg_plan_mode_ratio"] == 72.0
        # 7 / 25 = 28%
        assert result["avg_execution_mode_ratio"] == 28.0
        # 15 / 25 = 60%
        assert result["avg_early_task_ratio"] == 60.0
        # 8 / 25 = 32%
        assert result["avg_mid_task_ratio"] == 32.0
        # 2 / 25 = 8%
        assert result["avg_late_task_ratio"] == 8.0
        # 5 / 25 = 20%
        assert result["avg_blocking_ratio"] == 20.0
        # 20 / 25 = 80%
        assert result["avg_parallel_work_ratio"] == 80.0
        assert result["avg_response_time_seconds"] == 90.0
        assert result["avg_max_response_time_seconds"] == 300.0
        # 1 / 25 = 4%
        assert result["avg_timeout_ratio"] == 4.0
        # 500 / 2000 = 25%
        assert result["avg_wait_time_impact"] == 25.0
        assert result["low_blocking_sessions"] == 0
        assert result["high_blocking_sessions"] == 0
        assert result["high_wait_impact_sessions"] == 0

    def test_all_plan_mode_questions(self):
        """Verify session with all questions in plan mode."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "plan_mode_questions": 10,
                "execution_mode_questions": 0,
            }
        ])

        assert result["avg_plan_mode_ratio"] == 100.0
        assert result["avg_execution_mode_ratio"] == 0.0

    def test_all_execution_mode_questions(self):
        """Verify session with all questions in execution mode."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "plan_mode_questions": 0,
                "execution_mode_questions": 10,
            }
        ])

        assert result["avg_plan_mode_ratio"] == 0.0
        assert result["avg_execution_mode_ratio"] == 100.0

    def test_mixed_session_quality(self):
        """Verify mixed session quality across multiple sessions."""
        result = analyze_session_notification_tool_usage([
            # High blocking
            {
                "session_id": "s1",
                "total_ask_user_questions": 10,
                "blocking_questions": 8,
            },
            # Medium blocking (not classified)
            {
                "session_id": "s2",
                "total_ask_user_questions": 10,
                "blocking_questions": 3,
            },
            # Low blocking
            {
                "session_id": "s3",
                "total_ask_user_questions": 10,
                "blocking_questions": 1,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_questions"] == 3
        # (80% + 30% + 10%) / 3 = 40%
        assert result["avg_blocking_ratio"] == 40.0
        assert result["high_blocking_sessions"] == 1
        assert result["low_blocking_sessions"] == 1

    def test_no_timeouts_all_answered(self):
        """Verify session with all questions answered promptly."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "timed_out_questions": 0,
                "avg_response_time_seconds": 30,
            }
        ])

        assert result["avg_timeout_ratio"] == 0.0
        assert result["avg_response_time_seconds"] == 30.0

    def test_all_timeouts(self):
        """Verify session with all questions timing out."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "timed_out_questions": 10,
            }
        ])

        assert result["avg_timeout_ratio"] == 100.0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 10,
                "total_tasks": 0,
            }
        ])

        # Should not crash, questions per task should not be calculated
        assert result["avg_questions_per_task"] == 0.0

    def test_high_questions_per_task_ratio(self):
        """Verify detection of high questions per task ratio."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 30,
                "total_tasks": 3,
            }
        ])

        # 30 / 3 = 10
        assert result["avg_questions_per_task"] == 10.0

    def test_low_questions_per_task_ratio(self):
        """Verify detection of low questions per task ratio."""
        result = analyze_session_notification_tool_usage([
            {
                "session_id": "session1",
                "total_ask_user_questions": 3,
                "total_tasks": 10,
            }
        ])

        # 3 / 10 = 0.3
        assert result["avg_questions_per_task"] == 0.3
