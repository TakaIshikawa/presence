"""Tests for session EnterPlanMode frequency analyzer."""

import pytest

from synthesis.session_enterplanmode_frequency import (
    analyze_session_enterplanmode_frequency,
)


class TestAnalyzeSessionEnterplanmodeFrequency:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_enterplanmode_frequency([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_planning"] == 0
        assert result["avg_enterplanmode_calls"] == 0.0
        assert result["avg_plan_mode_adoption"] == 0.0
        assert result["avg_appropriate_planning_rate"] == 0.0
        assert result["avg_inappropriate_skip_rate"] == 0.0
        assert result["avg_premature_implementation_rate"] == 0.0
        assert result["avg_planning_duration_turns"] == 0.0
        assert result["avg_planning_to_execution_ratio"] == 0.0
        assert result["avg_plan_mode_success_rate"] == 0.0
        assert result["avg_no_plan_mode_success_rate"] == 0.0
        assert result["high_adoption_sessions"] == 0
        assert result["low_adoption_sessions"] == 0
        assert result["sessions_with_premature_implementation"] == 0
        assert result["sessions_with_high_inappropriate_skips"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_enterplanmode_frequency(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_enterplanmode_frequency("not a list")

    def test_session_with_no_planning(self):
        """Verify session with zero EnterPlanMode calls."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 0,
                "total_tasks": 5,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_planning"] == 0

    def test_session_with_planning(self):
        """Verify session with EnterPlanMode usage."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 3,
                "total_tasks": 5,
                "tasks_with_planning": 3,
                "tasks_without_planning": 2,
            }
        ])

        assert result["sessions_with_planning"] == 1
        assert result["avg_enterplanmode_calls"] == 3.0
        # 3 / 5 = 60%
        assert result["avg_plan_mode_adoption"] == 60.0

    def test_high_plan_mode_adoption(self):
        """Verify detection of high plan mode adoption."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 8,
                "total_tasks": 10,
                "tasks_with_planning": 8,
            }
        ])

        # 8 / 10 = 80%
        assert result["avg_plan_mode_adoption"] == 80.0
        assert result["high_adoption_sessions"] == 1
        assert result["low_adoption_sessions"] == 0

    def test_low_plan_mode_adoption(self):
        """Verify detection of low plan mode adoption."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 2,
                "total_tasks": 10,
                "tasks_with_planning": 2,
            }
        ])

        # 2 / 10 = 20%
        assert result["avg_plan_mode_adoption"] == 20.0
        assert result["high_adoption_sessions"] == 0
        assert result["low_adoption_sessions"] == 1

    def test_appropriate_planning_usage(self):
        """Verify tracking of appropriate plan mode usage."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5,
                "tasks_with_planning": 5,
                "appropriate_planning": 5,
            }
        ])

        # 5 / 5 = 100%
        assert result["avg_appropriate_planning_rate"] == 100.0

    def test_inappropriate_planning_skips(self):
        """Verify detection of inappropriate planning skips."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_planning": 4,
                "tasks_without_planning": 6,
                "inappropriate_skips": 3,
            }
        ])

        # 3 / 6 = 50%
        assert result["avg_inappropriate_skip_rate"] == 50.0
        assert result["sessions_with_high_inappropriate_skips"] == 1

    def test_low_inappropriate_skips(self):
        """Verify sessions with low inappropriate skip rate."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_planning": 8,
                "tasks_without_planning": 2,
                "inappropriate_skips": 0,
            }
        ])

        assert result["avg_inappropriate_skip_rate"] == 0.0
        assert result["sessions_with_high_inappropriate_skips"] == 0

    def test_premature_implementation_detection(self):
        """Verify detection of code written before ExitPlanMode."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5,
                "premature_implementation_count": 2,
            }
        ])

        # 2 / 5 = 40%
        assert result["avg_premature_implementation_rate"] == 40.0
        assert result["sessions_with_premature_implementation"] == 1

    def test_no_premature_implementation(self):
        """Verify sessions with no premature implementation."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5,
                "premature_implementation_count": 0,
            }
        ])

        assert result["avg_premature_implementation_rate"] == 0.0
        assert result["sessions_with_premature_implementation"] == 0

    def test_planning_duration_tracking(self):
        """Verify tracking of average planning duration."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 3,
                "avg_planning_duration_turns": 5.5,
            }
        ])

        assert result["avg_planning_duration_turns"] == 5.5

    def test_planning_to_execution_ratio(self):
        """Verify calculation of planning vs execution time ratio."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_planning_turns": 20,
                "total_execution_turns": 80,
            }
        ])

        # 20 / 100 = 20%
        assert result["avg_planning_to_execution_ratio"] == 20.0

    def test_plan_mode_success_rate(self):
        """Verify success rate calculation for tasks with planning."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "tasks_with_planning": 10,
                "tasks_with_planning_completed": 9,
            }
        ])

        # 9 / 10 = 90%
        assert result["avg_plan_mode_success_rate"] == 90.0

    def test_no_plan_mode_success_rate(self):
        """Verify success rate calculation for tasks without planning."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "tasks_without_planning": 5,
                "tasks_completed": 11,
                "tasks_with_planning_completed": 8,
            }
        ])

        # (11 - 8) / 5 = 3 / 5 = 60%
        assert result["avg_no_plan_mode_success_rate"] == 60.0

    def test_planning_improves_success_correlation(self):
        """Verify that planning correlates with higher success rate."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "tasks_with_planning": 10,
                "tasks_with_planning_completed": 9,
                "tasks_without_planning": 10,
                "tasks_completed": 14,  # 9 with planning + 5 without
            }
        ])

        # With planning: 9 / 10 = 90%
        assert result["avg_plan_mode_success_rate"] == 90.0
        # Without planning: 5 / 10 = 50%
        assert result["avg_no_plan_mode_success_rate"] == 50.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 4,
                "total_tasks": 10,
                "tasks_with_planning": 6,
            },
            {
                "session_id": "session2",
                "total_enterplanmode_calls": 6,
                "total_tasks": 10,
                "tasks_with_planning": 8,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_planning"] == 2
        # (4 + 6) / 2 = 5
        assert result["avg_enterplanmode_calls"] == 5.0
        # (60% + 80%) / 2 = 70%
        assert result["avg_plan_mode_adoption"] == 70.0

    def test_boundary_adoption_classification(self):
        """Verify boundary cases for adoption classification."""
        result = analyze_session_enterplanmode_frequency([
            # Exactly 70% (should not be high)
            {
                "session_id": "s1",
                "total_tasks": 10,
                "tasks_with_planning": 7,
            },
            # Just above 70% (should be high)
            {
                "session_id": "s2",
                "total_tasks": 10,
                "tasks_with_planning": 8,
            },
            # Exactly 30% (should not be low)
            {
                "session_id": "s3",
                "total_tasks": 10,
                "tasks_with_planning": 3,
            },
            # Just below 30% (should be low)
            {
                "session_id": "s4",
                "total_tasks": 10,
                "tasks_with_planning": 2,
            },
        ])

        # >70% means strictly greater
        assert result["high_adoption_sessions"] == 1
        # <30% means strictly less
        assert result["low_adoption_sessions"] == 1

    def test_boundary_inappropriate_skip_classification(self):
        """Verify boundary cases for inappropriate skip classification."""
        result = analyze_session_enterplanmode_frequency([
            # Exactly 20% (should not trigger)
            {
                "session_id": "s1",
                "tasks_without_planning": 10,
                "inappropriate_skips": 2,
            },
            # Just above 20% (should trigger)
            {
                "session_id": "s2",
                "tasks_without_planning": 10,
                "inappropriate_skips": 3,
            },
        ])

        # >20% means strictly greater
        assert result["sessions_with_high_inappropriate_skips"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_enterplanmode_frequency([
            "not a dict",
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": True,
                "tasks_with_planning": False,
            }
        ])

        assert result["sessions_with_planning"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5,
                # Missing most fields
            }
        ])

        assert result["sessions_with_planning"] == 1
        assert result["avg_enterplanmode_calls"] == 5.0
        # Missing fields result in 0.0 averages
        assert result["avg_plan_mode_adoption"] == 0.0

    def test_zero_enterplanmode_no_division_error(self):
        """Verify zero EnterPlanMode calls doesn't cause division errors."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 0,
                "premature_implementation_count": 0,
            }
        ])

        assert result["sessions_with_planning"] == 0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_tasks": 0,
                "tasks_with_planning": 0,
            }
        ])

        assert result["avg_plan_mode_adoption"] == 0.0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_enterplanmode_calls": 8,
                "total_tasks": 10,
                "tasks_with_planning": 8,
                "tasks_without_planning": 2,
                "appropriate_planning": 7,
                "inappropriate_skips": 0,
                "premature_implementation_count": 1,
                "avg_planning_duration_turns": 6.5,
                "total_planning_turns": 52,
                "total_execution_turns": 148,
                "tasks_completed": 9,
                "tasks_with_planning_completed": 7,
            }
        ])

        assert result["sessions_with_planning"] == 1
        assert result["avg_enterplanmode_calls"] == 8.0
        # 8 / 10 = 80%
        assert result["avg_plan_mode_adoption"] == 80.0
        # 7 / 8 = 87.5%
        assert result["avg_appropriate_planning_rate"] == 87.5
        # 0 / 2 = 0%
        assert result["avg_inappropriate_skip_rate"] == 0.0
        # 1 / 8 = 12.5%
        assert result["avg_premature_implementation_rate"] == 12.5
        assert result["avg_planning_duration_turns"] == 6.5
        # 52 / 200 = 26%
        assert result["avg_planning_to_execution_ratio"] == 26.0
        # 7 / 8 = 87.5%
        assert result["avg_plan_mode_success_rate"] == 87.5
        # (9 - 7) / 2 = 2 / 2 = 100%
        assert result["avg_no_plan_mode_success_rate"] == 100.0
        assert result["high_adoption_sessions"] == 1
        assert result["sessions_with_premature_implementation"] == 1
        assert result["sessions_with_high_inappropriate_skips"] == 0

    def test_all_tasks_with_planning(self):
        """Verify session where all tasks use plan mode."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 10,
                "total_tasks": 10,
                "tasks_with_planning": 10,
                "tasks_without_planning": 0,
            }
        ])

        assert result["avg_plan_mode_adoption"] == 100.0
        assert result["high_adoption_sessions"] == 1

    def test_no_tasks_with_planning(self):
        """Verify session where no tasks use plan mode."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 0,
                "total_tasks": 10,
                "tasks_with_planning": 0,
                "tasks_without_planning": 10,
            }
        ])

        assert result["avg_plan_mode_adoption"] == 0.0
        assert result["low_adoption_sessions"] == 1

    def test_mixed_session_quality(self):
        """Verify mixed session quality across multiple sessions."""
        result = analyze_session_enterplanmode_frequency([
            # High adoption
            {
                "session_id": "s1",
                "total_tasks": 10,
                "tasks_with_planning": 9,
            },
            # Medium adoption (not classified)
            {
                "session_id": "s2",
                "total_tasks": 10,
                "tasks_with_planning": 5,
            },
            # Low adoption
            {
                "session_id": "s3",
                "total_tasks": 10,
                "tasks_with_planning": 1,
            },
        ])

        assert result["total_sessions"] == 3
        # (90% + 50% + 10%) / 3 = 50%
        assert result["avg_plan_mode_adoption"] == 50.0
        assert result["high_adoption_sessions"] == 1
        assert result["low_adoption_sessions"] == 1

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_enterplanmode_calls": 5.5,
                "avg_planning_duration_turns": 7.25,
            }
        ])

        assert result["avg_enterplanmode_calls"] == 5.5
        assert result["avg_planning_duration_turns"] == 7.25

    def test_zero_planning_turns_no_division_error(self):
        """Verify zero planning turns doesn't cause division errors."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_planning_turns": 0,
                "total_execution_turns": 100,
            }
        ])

        # 0 / 100 = 0%
        assert result["avg_planning_to_execution_ratio"] == 0.0

    def test_only_planning_turns_no_execution(self):
        """Verify session with only planning, no execution."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "total_planning_turns": 50,
                "total_execution_turns": 0,
            }
        ])

        # 50 / 50 = 100%
        assert result["avg_planning_to_execution_ratio"] == 100.0

    def test_partial_planning_completion_data(self):
        """Verify handling of partial completion data."""
        result = analyze_session_enterplanmode_frequency([
            {
                "session_id": "session1",
                "tasks_with_planning": 5,
                "tasks_with_planning_completed": 4,
            }
        ])

        # 4 / 5 = 80%
        assert result["avg_plan_mode_success_rate"] == 80.0
