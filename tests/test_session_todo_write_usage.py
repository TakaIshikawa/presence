"""Tests for session TodoWrite usage pattern analyzer."""

import pytest

from synthesis.session_todo_write_usage import analyze_session_todo_write_usage


class TestAnalyzeSessionTodoWriteUsage:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_todo_write_usage([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_todo_write"] == 0
        assert result["avg_todo_write_calls"] == 0.0
        assert result["avg_todos_per_call"] == 0.0
        assert result["total_todos_analyzed"] == 0
        assert result["avg_completion_rate"] == 0.0
        assert result["avg_in_progress_rate"] == 0.0
        assert result["avg_pending_rate"] == 0.0
        assert result["avg_task_description_length"] == 0.0
        assert result["avg_active_form_consistency"] == 0.0
        assert result["high_completion_sessions"] == 0
        assert result["low_completion_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_todo_write_usage(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_todo_write_usage("not a list")

    def test_session_with_no_todo_write_calls(self):
        """Verify session with zero TodoWrite calls handled gracefully."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 0,
                "total_todos": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 0
        assert result["avg_todo_write_calls"] == 0.0
        assert result["total_todos_analyzed"] == 0

    def test_single_session_perfect_completion(self):
        """Verify session with 100% completion rate."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 3,
                "total_todos": 10,
                "pending_todos": 0,
                "in_progress_todos": 0,
                "completed_todos": 10,
                "total_description_length": 500,
                "todos_with_active_form": 10,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 1
        assert result["avg_todo_write_calls"] == 3.0
        assert result["avg_todos_per_call"] == round(10 / 3, 2)
        assert result["total_todos_analyzed"] == 10
        assert result["avg_completion_rate"] == 100.0
        assert result["avg_in_progress_rate"] == 0.0
        assert result["avg_pending_rate"] == 0.0
        assert result["avg_task_description_length"] == 50.0
        assert result["avg_active_form_consistency"] == 100.0
        assert result["high_completion_sessions"] == 1
        assert result["low_completion_sessions"] == 0

    def test_single_session_low_completion(self):
        """Verify session with low completion rate."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 10,
                "pending_todos": 6,
                "in_progress_todos": 2,
                "completed_todos": 2,
                "total_description_length": 400,
                "todos_with_active_form": 10,
            }
        ])

        assert result["avg_completion_rate"] == 20.0
        assert result["avg_pending_rate"] == 60.0
        assert result["avg_in_progress_rate"] == 20.0
        assert result["high_completion_sessions"] == 0
        assert result["low_completion_sessions"] == 1

    def test_mixed_status_distribution(self):
        """Verify mixed status distribution calculated correctly."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 4,
                "total_todos": 20,
                "pending_todos": 5,
                "in_progress_todos": 3,
                "completed_todos": 12,
                "total_description_length": 1000,
                "todos_with_active_form": 20,
            }
        ])

        # 12 completed / 20 total = 60%
        assert result["avg_completion_rate"] == 60.0
        # 3 in_progress / 20 total = 15%
        assert result["avg_in_progress_rate"] == 15.0
        # 5 pending / 20 total = 25%
        assert result["avg_pending_rate"] == 25.0
        assert result["avg_task_description_length"] == 50.0

    def test_incomplete_active_form_usage(self):
        """Verify incomplete activeForm usage detected."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 10,
                "pending_todos": 3,
                "in_progress_todos": 2,
                "completed_todos": 5,
                "total_description_length": 300,
                "todos_with_active_form": 7,
            }
        ])

        # 7 with activeForm / 10 total = 70%
        assert result["avg_active_form_consistency"] == 70.0
        assert result["avg_task_description_length"] == 30.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 10,
                "pending_todos": 0,
                "in_progress_todos": 0,
                "completed_todos": 10,
                "total_description_length": 500,
                "todos_with_active_form": 10,
            },
            {
                "session_id": "session2",
                "todo_write_calls": 3,
                "total_todos": 15,
                "pending_todos": 5,
                "in_progress_todos": 5,
                "completed_todos": 5,
                "total_description_length": 600,
                "todos_with_active_form": 14,
            },
            {
                "session_id": "session3",
                "todo_write_calls": 1,
                "total_todos": 5,
                "pending_todos": 1,
                "in_progress_todos": 1,
                "completed_todos": 3,
                "total_description_length": 200,
                "todos_with_active_form": 5,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["sessions_with_todo_write"] == 3
        # (2 + 3 + 1) / 3 = 2.0
        assert result["avg_todo_write_calls"] == 2.0
        # (10/2 + 15/3 + 5/1) / 3 = (5 + 5 + 5) / 3 = 5.0
        assert result["avg_todos_per_call"] == 5.0
        # 10 + 15 + 5 = 30
        assert result["total_todos_analyzed"] == 30
        # Session 1: 100%, Session 2: 33.33%, Session 3: 60%
        # Average: (100 + 33.33 + 60) / 3 = 64.44
        assert 64.0 <= result["avg_completion_rate"] <= 65.0
        assert result["high_completion_sessions"] == 1
        assert result["low_completion_sessions"] == 1

    def test_session_without_status_data(self):
        """Verify session without status data handled gracefully."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 10,
                # No status fields provided
                "total_description_length": 400,
                "todos_with_active_form": 10,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 1
        # No status data means no rates calculated
        assert result["avg_completion_rate"] == 0.0
        assert result["avg_in_progress_rate"] == 0.0
        assert result["avg_pending_rate"] == 0.0
        # Other metrics still calculated
        assert result["avg_task_description_length"] == 40.0
        assert result["avg_active_form_consistency"] == 100.0

    def test_session_with_zero_todos(self):
        """Verify session with TodoWrite calls but zero todos."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 0,
            }
        ])

        assert result["sessions_with_todo_write"] == 1
        assert result["total_todos_analyzed"] == 0
        # No todos means no per-call calculation
        assert result["avg_todos_per_call"] == 0.0

    def test_high_completion_classification(self):
        """Verify high completion rate classification (>80%)."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 1,
                "in_progress_todos": 0,
                "completed_todos": 9,
            },
            {
                "session_id": "session2",
                "todo_write_calls": 1,
                "total_todos": 20,
                "pending_todos": 0,
                "in_progress_todos": 3,
                "completed_todos": 17,
            },
        ])

        # Both sessions have >80% completion
        assert result["high_completion_sessions"] == 2
        assert result["low_completion_sessions"] == 0

    def test_low_completion_classification(self):
        """Verify low completion rate classification (<50%)."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 7,
                "in_progress_todos": 2,
                "completed_todos": 1,
            },
            {
                "session_id": "session2",
                "todo_write_calls": 1,
                "total_todos": 20,
                "pending_todos": 15,
                "in_progress_todos": 3,
                "completed_todos": 2,
            },
        ])

        # Both sessions have <50% completion
        assert result["high_completion_sessions"] == 0
        assert result["low_completion_sessions"] == 2

    def test_medium_completion_not_classified(self):
        """Verify medium completion rates not classified as high or low."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 3,
                "in_progress_todos": 1,
                "completed_todos": 6,
            },
        ])

        # 60% completion (between 50% and 80%)
        assert result["avg_completion_rate"] == 60.0
        assert result["high_completion_sessions"] == 0
        assert result["low_completion_sessions"] == 0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_todo_write_usage([
            "not a dict",
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 5,
            },
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": True,
                "total_todos": False,
            }
        ])

        # Booleans should be ignored
        assert result["sessions_with_todo_write"] == 0
        assert result["total_todos_analyzed"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 8,
                # Missing most fields
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 1
        assert result["avg_todos_per_call"] == 4.0
        # Missing status fields result in 0.0 rates
        assert result["avg_completion_rate"] == 0.0

    def test_partial_status_data(self):
        """Verify partial status data handled correctly."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 3,
                # Missing in_progress
                "completed_todos": 7,
            }
        ])

        # Total status = 3 + 7 = 10
        # Completion rate = 7/10 = 70%
        assert result["avg_completion_rate"] == 70.0
        assert result["avg_pending_rate"] == 30.0
        # Missing in_progress means 0.0 average
        assert result["avg_in_progress_rate"] == 0.0

    def test_long_description_lengths(self):
        """Verify long description lengths calculated correctly."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 5,
                "total_description_length": 1000,
            }
        ])

        # 1000 / 5 = 200 chars average
        assert result["avg_task_description_length"] == 200.0

    def test_short_description_lengths(self):
        """Verify short description lengths calculated correctly."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "total_description_length": 100,
            }
        ])

        # 100 / 10 = 10 chars average
        assert result["avg_task_description_length"] == 10.0

    def test_zero_active_form_consistency(self):
        """Verify sessions with no activeForm fields."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "todos_with_active_form": 0,
            }
        ])

        assert result["avg_active_form_consistency"] == 0.0

    def test_mixed_completion_across_sessions(self):
        """Verify varied completion rates averaged correctly."""
        result = analyze_session_todo_write_usage([
            # Perfect completion
            {
                "session_id": "s1",
                "todo_write_calls": 1,
                "total_todos": 5,
                "pending_todos": 0,
                "in_progress_todos": 0,
                "completed_todos": 5,
            },
            # No completion
            {
                "session_id": "s2",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 10,
                "in_progress_todos": 0,
                "completed_todos": 0,
            },
            # Partial completion
            {
                "session_id": "s3",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 5,
                "in_progress_todos": 0,
                "completed_todos": 5,
            },
        ])

        # (100 + 0 + 50) / 3 = 50.0
        assert result["avg_completion_rate"] == 50.0
        assert result["high_completion_sessions"] == 1
        assert result["low_completion_sessions"] == 1

    def test_large_number_of_todo_calls(self):
        """Verify session with many TodoWrite calls."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 20,
                "total_todos": 40,
                "pending_todos": 10,
                "in_progress_todos": 10,
                "completed_todos": 20,
            }
        ])

        assert result["avg_todo_write_calls"] == 20.0
        # 40 / 20 = 2.0 todos per call
        assert result["avg_todos_per_call"] == 2.0
        # 20 completed / 40 total = 50%
        assert result["avg_completion_rate"] == 50.0

    def test_single_todo_per_call(self):
        """Verify pattern with one todo per call."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 10,
                "total_todos": 10,
                "completed_todos": 10,
            }
        ])

        # 10 / 10 = 1.0 todo per call
        assert result["avg_todos_per_call"] == 1.0

    def test_many_todos_per_call(self):
        """Verify pattern with many todos per call."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 2,
                "total_todos": 50,
                "completed_todos": 30,
            }
        ])

        # 50 / 2 = 25.0 todos per call
        assert result["avg_todos_per_call"] == 25.0

    def test_sessions_without_description_length(self):
        """Verify sessions missing description length field."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                # Missing total_description_length
            }
        ])

        # No description data means 0.0 average
        assert result["avg_task_description_length"] == 0.0

    def test_sessions_without_active_form_field(self):
        """Verify sessions missing activeForm field."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "session1",
                "todo_write_calls": 1,
                "total_todos": 10,
                # Missing todos_with_active_form
            }
        ])

        # No activeForm data means 0.0 average
        assert result["avg_active_form_consistency"] == 0.0

    def test_edge_case_boundary_completion_rates(self):
        """Verify boundary cases for completion classification."""
        result = analyze_session_todo_write_usage([
            # Exactly 80% (should not be high)
            {
                "session_id": "s1",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 2,
                "in_progress_todos": 0,
                "completed_todos": 8,
            },
            # Exactly 50% (should not be low)
            {
                "session_id": "s2",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 5,
                "in_progress_todos": 0,
                "completed_todos": 5,
            },
            # Just above 80% (should be high)
            {
                "session_id": "s3",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 1,
                "in_progress_todos": 0,
                "completed_todos": 9,
            },
            # Just below 50% (should be low)
            {
                "session_id": "s4",
                "todo_write_calls": 1,
                "total_todos": 10,
                "pending_todos": 6,
                "in_progress_todos": 0,
                "completed_todos": 4,
            },
        ])

        # >80% means strictly greater
        assert result["high_completion_sessions"] == 1
        # <50% means strictly less
        assert result["low_completion_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_todo_write_usage([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "todo_write_calls": 5,
                "total_todos": 25,
                "pending_todos": 5,
                "in_progress_todos": 3,
                "completed_todos": 17,
                "total_description_length": 1500,
                "todos_with_active_form": 24,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todo_write"] == 1
        assert result["avg_todo_write_calls"] == 5.0
        assert result["avg_todos_per_call"] == 5.0
        assert result["total_todos_analyzed"] == 25
        # 17 / 25 = 68%
        assert result["avg_completion_rate"] == 68.0
        # 3 / 25 = 12%
        assert result["avg_in_progress_rate"] == 12.0
        # 5 / 25 = 20%
        assert result["avg_pending_rate"] == 20.0
        # 1500 / 25 = 60 chars
        assert result["avg_task_description_length"] == 60.0
        # 24 / 25 = 96%
        assert result["avg_active_form_consistency"] == 96.0
        assert result["high_completion_sessions"] == 0
        assert result["low_completion_sessions"] == 0
