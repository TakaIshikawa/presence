"""Tests for session TodoWrite tracking coverage analyzer."""

import pytest

from synthesis.session_todowrite_tracking_coverage import (
    analyze_session_todowrite_tracking_coverage,
)


class TestAnalyzeSessionTodowriteTrackingCoverage:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_todowrite_tracking_coverage([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_todos"] == 0
        assert result["avg_todo_adoption_rate"] == 0.0
        assert result["avg_todo_completion_rate"] == 0.0
        assert result["avg_orphaned_todo_rate"] == 0.0
        assert result["avg_batch_violation_rate"] == 0.0
        assert result["avg_multi_in_progress_violation_rate"] == 0.0
        assert result["avg_missing_activeform_rate"] == 0.0
        assert result["avg_clean_transition_rate"] == 0.0
        assert result["high_adoption_sessions"] == 0
        assert result["low_adoption_sessions"] == 0
        assert result["sessions_with_batch_violations"] == 0
        assert result["sessions_with_orphaned_todos"] == 0
        assert result["sessions_with_multi_in_progress"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_todowrite_tracking_coverage(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_todowrite_tracking_coverage("not a list")

    def test_session_without_todos(self):
        """Verify session with zero TodoWrite usage."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 5,
                "tasks_with_todos": 0,
                "tasks_without_todos": 5,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todos"] == 0
        assert result["avg_todo_adoption_rate"] == 0.0

    def test_session_with_todos(self):
        """Verify session with TodoWrite usage."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 5,
                "tasks_with_todos": 4,
                "total_todos_created": 20,
                "total_todos_completed": 18,
            }
        ])

        assert result["sessions_with_todos"] == 1
        # 4 / 5 = 80%
        assert result["avg_todo_adoption_rate"] == 80.0
        # 18 / 20 = 90%
        assert result["avg_todo_completion_rate"] == 90.0

    def test_high_todo_adoption(self):
        """Verify detection of high todo adoption rate."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_todos": 9,
            }
        ])

        # 9 / 10 = 90%
        assert result["avg_todo_adoption_rate"] == 90.0
        assert result["high_adoption_sessions"] == 1
        assert result["low_adoption_sessions"] == 0

    def test_low_todo_adoption(self):
        """Verify detection of low todo adoption rate."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_todos": 3,
            }
        ])

        # 3 / 10 = 30%
        assert result["avg_todo_adoption_rate"] == 30.0
        assert result["high_adoption_sessions"] == 0
        assert result["low_adoption_sessions"] == 1

    def test_high_completion_rate(self):
        """Verify tracking of high todo completion rate."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "total_todos_completed": 19,
            }
        ])

        # 19 / 20 = 95%
        assert result["avg_todo_completion_rate"] == 95.0

    def test_low_completion_rate(self):
        """Verify tracking of low todo completion rate."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "total_todos_completed": 10,
            }
        ])

        # 10 / 20 = 50%
        assert result["avg_todo_completion_rate"] == 50.0

    def test_orphaned_in_progress_detection(self):
        """Verify detection of orphaned in_progress todos."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "orphaned_in_progress": 3,
            }
        ])

        # 3 / 20 = 15%
        assert result["avg_orphaned_todo_rate"] == 15.0
        assert result["sessions_with_orphaned_todos"] == 1

    def test_no_orphaned_todos(self):
        """Verify sessions with no orphaned todos."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "orphaned_in_progress": 0,
            }
        ])

        assert result["avg_orphaned_todo_rate"] == 0.0
        assert result["sessions_with_orphaned_todos"] == 0

    def test_batch_completion_violations(self):
        """Verify detection of batch completion violations."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_completed": 20,
                "batch_completion_violations": 4,
            }
        ])

        # 4 / 20 = 20%
        assert result["avg_batch_violation_rate"] == 20.0
        assert result["sessions_with_batch_violations"] == 1

    def test_no_batch_violations(self):
        """Verify sessions with proper one-at-a-time completion."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_completed": 20,
                "batch_completion_violations": 0,
            }
        ])

        assert result["avg_batch_violation_rate"] == 0.0
        assert result["sessions_with_batch_violations"] == 0

    def test_multiple_in_progress_violations(self):
        """Verify detection of multiple simultaneous in_progress todos."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "multiple_in_progress_violations": 5,
            }
        ])

        # 5 / 20 = 25%
        assert result["avg_multi_in_progress_violation_rate"] == 25.0
        assert result["sessions_with_multi_in_progress"] == 1

    def test_no_multi_in_progress_violations(self):
        """Verify sessions with exactly one in_progress at a time."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "multiple_in_progress_violations": 0,
            }
        ])

        assert result["avg_multi_in_progress_violation_rate"] == 0.0
        assert result["sessions_with_multi_in_progress"] == 0

    def test_missing_activeform_detection(self):
        """Verify detection of todos missing activeForm field."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "missing_activeform_count": 3,
            }
        ])

        # 3 / 20 = 15%
        assert result["avg_missing_activeform_rate"] == 15.0

    def test_all_activeform_present(self):
        """Verify sessions where all todos have activeForm."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20,
                "missing_activeform_count": 0,
            }
        ])

        assert result["avg_missing_activeform_rate"] == 0.0

    def test_clean_state_transitions(self):
        """Verify tracking of clean state transitions."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "clean_state_transitions": 18,
                "improper_state_transitions": 2,
            }
        ])

        # 18 / 20 = 90%
        assert result["avg_clean_transition_rate"] == 90.0

    def test_all_clean_transitions(self):
        """Verify sessions with all clean state transitions."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "clean_state_transitions": 20,
                "improper_state_transitions": 0,
            }
        ])

        assert result["avg_clean_transition_rate"] == 100.0

    def test_improper_state_transitions(self):
        """Verify detection of improper state transitions."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "clean_state_transitions": 10,
                "improper_state_transitions": 10,
            }
        ])

        # 10 / 20 = 50%
        assert result["avg_clean_transition_rate"] == 50.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_todos": 8,
                "total_todos_created": 30,
                "total_todos_completed": 27,
            },
            {
                "session_id": "session2",
                "total_tasks": 10,
                "tasks_with_todos": 6,
                "total_todos_created": 20,
                "total_todos_completed": 18,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_todos"] == 2
        # (80% + 60%) / 2 = 70%
        assert result["avg_todo_adoption_rate"] == 70.0
        # (90% + 90%) / 2 = 90%
        assert result["avg_todo_completion_rate"] == 90.0

    def test_boundary_adoption_classification(self):
        """Verify boundary cases for adoption classification."""
        result = analyze_session_todowrite_tracking_coverage([
            # Exactly 80% (should not be high)
            {
                "session_id": "s1",
                "total_tasks": 10,
                "tasks_with_todos": 8,
            },
            # Just above 80% (should be high)
            {
                "session_id": "s2",
                "total_tasks": 10,
                "tasks_with_todos": 9,
            },
            # Exactly 50% (should not be low)
            {
                "session_id": "s3",
                "total_tasks": 10,
                "tasks_with_todos": 5,
            },
            # Just below 50% (should be low)
            {
                "session_id": "s4",
                "total_tasks": 10,
                "tasks_with_todos": 4,
            },
        ])

        # >80% means strictly greater
        assert result["high_adoption_sessions"] == 1
        # <50% means strictly less
        assert result["low_adoption_sessions"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_todowrite_tracking_coverage([
            "not a dict",
            {
                "session_id": "session1",
                "total_tasks": 5,
                "tasks_with_todos": 4,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "tasks_with_todos": True,
                "total_todos_created": False,
            }
        ])

        assert result["sessions_with_todos"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "tasks_with_todos": 5,
                # Missing most fields
            }
        ])

        assert result["sessions_with_todos"] == 1
        # Missing fields result in 0.0 averages
        assert result["avg_todo_adoption_rate"] == 0.0

    def test_zero_todos_no_division_error(self):
        """Verify zero todos created doesn't cause division errors."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 0,
                "total_todos_completed": 0,
            }
        ])

        assert result["avg_todo_completion_rate"] == 0.0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 0,
                "tasks_with_todos": 0,
            }
        ])

        assert result["avg_todo_adoption_rate"] == 0.0

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_tasks": 10,
                "tasks_with_todos": 9,
                "tasks_without_todos": 1,
                "total_todos_created": 45,
                "total_todos_completed": 40,
                "total_todos_abandoned": 5,
                "orphaned_in_progress": 2,
                "batch_completion_violations": 3,
                "multiple_in_progress_violations": 4,
                "missing_activeform_count": 1,
                "clean_state_transitions": 38,
                "improper_state_transitions": 7,
            }
        ])

        assert result["sessions_with_todos"] == 1
        # 9 / 10 = 90%
        assert result["avg_todo_adoption_rate"] == 90.0
        # 40 / 45 = 88.89%
        assert 88.0 <= result["avg_todo_completion_rate"] <= 89.0
        # 2 / 45 = 4.44%
        assert 4.0 <= result["avg_orphaned_todo_rate"] <= 5.0
        # 3 / 40 = 7.5%
        assert result["avg_batch_violation_rate"] == 7.5
        # 4 / 45 = 8.89%
        assert 8.0 <= result["avg_multi_in_progress_violation_rate"] <= 9.0
        # 1 / 45 = 2.22%
        assert 2.0 <= result["avg_missing_activeform_rate"] <= 3.0
        # 38 / 45 = 84.44%
        assert 84.0 <= result["avg_clean_transition_rate"] <= 85.0
        assert result["high_adoption_sessions"] == 1
        assert result["sessions_with_batch_violations"] == 1
        assert result["sessions_with_orphaned_todos"] == 1
        assert result["sessions_with_multi_in_progress"] == 1

    def test_perfect_todo_hygiene(self):
        """Verify session with perfect todo tracking hygiene."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_todos": 10,
                "total_todos_created": 50,
                "total_todos_completed": 50,
                "orphaned_in_progress": 0,
                "batch_completion_violations": 0,
                "multiple_in_progress_violations": 0,
                "missing_activeform_count": 0,
                "clean_state_transitions": 50,
                "improper_state_transitions": 0,
            }
        ])

        assert result["avg_todo_adoption_rate"] == 100.0
        assert result["avg_todo_completion_rate"] == 100.0
        assert result["avg_orphaned_todo_rate"] == 0.0
        assert result["avg_batch_violation_rate"] == 0.0
        assert result["avg_multi_in_progress_violation_rate"] == 0.0
        assert result["avg_missing_activeform_rate"] == 0.0
        assert result["avg_clean_transition_rate"] == 100.0
        assert result["high_adoption_sessions"] == 1

    def test_poor_todo_hygiene(self):
        """Verify session with poor todo tracking hygiene."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_tasks": 10,
                "tasks_with_todos": 2,
                "total_todos_created": 10,
                "total_todos_completed": 3,
                "orphaned_in_progress": 5,
                "batch_completion_violations": 2,
                "multiple_in_progress_violations": 4,
                "missing_activeform_count": 6,
            }
        ])

        # 2 / 10 = 20%
        assert result["avg_todo_adoption_rate"] == 20.0
        # 3 / 10 = 30%
        assert result["avg_todo_completion_rate"] == 30.0
        # 5 / 10 = 50%
        assert result["avg_orphaned_todo_rate"] == 50.0
        # 6 / 10 = 60%
        assert result["avg_missing_activeform_rate"] == 60.0
        assert result["low_adoption_sessions"] == 1

    def test_mixed_session_quality(self):
        """Verify mixed session quality across multiple sessions."""
        result = analyze_session_todowrite_tracking_coverage([
            # High quality
            {
                "session_id": "s1",
                "total_tasks": 10,
                "tasks_with_todos": 9,
                "total_todos_created": 30,
                "total_todos_completed": 29,
            },
            # Medium quality
            {
                "session_id": "s2",
                "total_tasks": 10,
                "tasks_with_todos": 6,
                "total_todos_created": 20,
                "total_todos_completed": 16,
            },
            # Low quality
            {
                "session_id": "s3",
                "total_tasks": 10,
                "tasks_with_todos": 2,
                "total_todos_created": 8,
                "total_todos_completed": 4,
            },
        ])

        assert result["total_sessions"] == 3
        # (90% + 60% + 20%) / 3 = 56.67%
        assert 56.0 <= result["avg_todo_adoption_rate"] <= 57.0
        # (96.67% + 80% + 50%) / 3 = 75.56%
        assert 75.0 <= result["avg_todo_completion_rate"] <= 76.0
        assert result["high_adoption_sessions"] == 1
        assert result["low_adoption_sessions"] == 1

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_created": 20.5,
                "total_todos_completed": 18.5,
            }
        ])

        # 18.5 / 20.5 = 90.24%
        assert 90.0 <= result["avg_todo_completion_rate"] <= 91.0

    def test_zero_completions_no_batch_violations(self):
        """Verify zero completions doesn't cause batch violation calculation."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "total_todos_completed": 0,
                "batch_completion_violations": 0,
            }
        ])

        # No completions means no batch violation rate calculated
        assert result["avg_batch_violation_rate"] == 0.0

    def test_zero_transitions_no_division_error(self):
        """Verify zero state transitions doesn't cause division errors."""
        result = analyze_session_todowrite_tracking_coverage([
            {
                "session_id": "session1",
                "clean_state_transitions": 0,
                "improper_state_transitions": 0,
            }
        ])

        assert result["avg_clean_transition_rate"] == 0.0
