"""Tests for session TodoWrite frequency analyzer."""

import pytest

from synthesis.session_todowrite_frequency import (
    analyze_session_todowrite_frequency,
)


class TestAnalyzeSessionTodowriteFrequency:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_todowrite_frequency([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_todowrite"] == 0
        assert result["avg_todowrite_calls"] == 0.0
        assert result["avg_time_between_updates"] == 0.0
        assert result["avg_todos_per_update"] == 0.0
        assert result["avg_completion_batching_ratio"] == 0.0
        assert result["avg_pending_staleness"] == 0.0
        assert result["avg_inprogress_staleness"] == 0.0
        assert result["update_discipline_score"] == 0.0
        assert result["high_discipline_sessions"] == 0
        assert result["low_discipline_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_todowrite_frequency(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_todowrite_frequency("not a list")

    def test_session_with_no_todowrite_calls(self):
        """Verify session with zero TodoWrite calls handled gracefully."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_todowrite"] == 0

    def test_high_discipline_frequent_updates(self):
        """Verify high discipline with frequent updates and immediate completions."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 20,
                "total_update_interval_seconds": 3800,  # 19 intervals * 200s avg
                "total_todo_count": 60,
                "completion_batching_calls": 1,
                "immediate_completion_calls": 19,
                "total_pending_duration_seconds": 4000,  # 200s avg per todo
                "total_inprogress_duration_seconds": 6000,  # 300s avg per todo
                "total_todos_tracked": 20,
            }
        ])

        assert result["sessions_with_todowrite"] == 1
        assert result["avg_todowrite_calls"] == 20.0
        # 3800 / 19 = 200s
        assert result["avg_time_between_updates"] == 200.0
        # 60 / 20 = 3 todos per update
        assert result["avg_todos_per_update"] == 3.0
        # 1 / (1 + 19) = 5%
        assert result["avg_completion_batching_ratio"] == 5.0
        # 4000 / 20 = 200s
        assert result["avg_pending_staleness"] == 200.0
        # 6000 / 20 = 300s
        assert result["avg_inprogress_staleness"] == 300.0
        # High score for frequent updates, low batching, low staleness
        assert result["update_discipline_score"] > 80.0
        assert result["high_discipline_sessions"] == 1
        assert result["low_discipline_sessions"] == 0

    def test_low_discipline_batched_completions(self):
        """Verify low discipline with infrequent updates and batched completions."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 5,
                "total_update_interval_seconds": 6000,  # 4 intervals * 1500s avg
                "total_todo_count": 25,
                "completion_batching_calls": 4,
                "immediate_completion_calls": 1,
                "total_pending_duration_seconds": 36000,  # 3600s avg per todo
                "total_inprogress_duration_seconds": 27000,  # 2700s avg per todo
                "total_todos_tracked": 10,
            }
        ])

        assert result["sessions_with_todowrite"] == 1
        # 6000 / 4 = 1500s
        assert result["avg_time_between_updates"] == 1500.0
        # 25 / 5 = 5 todos per update
        assert result["avg_todos_per_update"] == 5.0
        # 4 / (4 + 1) = 80%
        assert result["avg_completion_batching_ratio"] == 80.0
        # 36000 / 10 = 3600s
        assert result["avg_pending_staleness"] == 3600.0
        # 27000 / 10 = 2700s
        assert result["avg_inprogress_staleness"] == 2700.0
        # Low score for infrequent updates, high batching, high staleness
        assert result["update_discipline_score"] < 50.0
        assert result["high_discipline_sessions"] == 0
        assert result["low_discipline_sessions"] == 1

    def test_completion_batching_ratio_calculation(self):
        """Verify completion batching ratio calculated correctly."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 10,
                "completion_batching_calls": 3,
                "immediate_completion_calls": 7,
            }
        ])

        # 3 / (3 + 7) = 30%
        assert result["avg_completion_batching_ratio"] == 30.0

    def test_staleness_calculation(self):
        """Verify staleness calculation for pending and in_progress states."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 10,
                "total_pending_duration_seconds": 12000,
                "total_inprogress_duration_seconds": 18000,
                "total_todos_tracked": 10,
            }
        ])

        # 12000 / 10 = 1200s
        assert result["avg_pending_staleness"] == 1200.0
        # 18000 / 10 = 1800s
        assert result["avg_inprogress_staleness"] == 1800.0

    def test_avg_todos_per_update_from_field(self):
        """Verify avg_todos_per_update used from record when available."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 10,
                "avg_todos_per_update": 4.5,
            }
        ])

        assert result["avg_todos_per_update"] == 4.5

    def test_avg_todos_per_update_calculated(self):
        """Verify avg_todos_per_update calculated when not provided."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 8,
                "total_todo_count": 40,
            }
        ])

        # 40 / 8 = 5.0
        assert result["avg_todos_per_update"] == 5.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 20,
                "total_update_interval_seconds": 3800,
                "completion_batching_calls": 2,
                "immediate_completion_calls": 18,
            },
            {
                "session_id": "session2",
                "total_todowrite_calls": 10,
                "total_update_interval_seconds": 2700,
                "completion_batching_calls": 1,
                "immediate_completion_calls": 9,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_todowrite"] == 2
        # (20 + 10) / 2 = 15
        assert result["avg_todowrite_calls"] == 15.0
        # (3800/19 + 2700/9) / 2 = (200 + 300) / 2 = 250
        assert result["avg_time_between_updates"] == 250.0
        # (2/20 + 1/10) / 2 = (10% + 10%) / 2 = 10%
        assert result["avg_completion_batching_ratio"] == 10.0

    def test_single_todowrite_call_no_interval(self):
        """Verify session with single TodoWrite call has no interval."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 1,
                "total_update_interval_seconds": 0,
            }
        ])

        assert result["sessions_with_todowrite"] == 1
        assert result["avg_time_between_updates"] == 0.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_todowrite_frequency([
            "not a dict",
            {
                "session_id": "session1",
                "total_todowrite_calls": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": True,
                "completion_batching_calls": False,
            }
        ])

        assert result["sessions_with_todowrite"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_todowrite"] == 1
        assert result["avg_todowrite_calls"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_time_between_updates"] == 0.0

    def test_boundary_discipline_classification(self):
        """Verify boundary cases for discipline classification."""
        result = analyze_session_todowrite_frequency([
            # Exactly 80: 30 (freq) + 30 (batch) + 20 (stale) = 80
            {
                "session_id": "s1",
                "total_todowrite_calls": 10,
                "total_update_interval_seconds": 5400,  # 600s avg (good, 30pts)
                "completion_batching_calls": 2,
                "immediate_completion_calls": 8,  # 20% (excellent, 30pts)
                "total_pending_duration_seconds": 12000,
                "total_inprogress_duration_seconds": 12000,
                "total_todos_tracked": 10,  # 1200s avg (good, 20pts)
            },
            # Just above 80: 40 (freq) + 20 (batch) + 30 (stale) = 90
            {
                "session_id": "s2",
                "total_todowrite_calls": 10,
                "total_update_interval_seconds": 1800,  # 200s avg (excellent, 40pts)
                "completion_batching_calls": 3,
                "immediate_completion_calls": 7,  # 30% (good, 20pts)
                "total_pending_duration_seconds": 4000,
                "total_inprogress_duration_seconds": 4000,
                "total_todos_tracked": 10,  # 400s avg (excellent, 30pts)
            },
            # Exactly 50: 20 (freq) + 20 (batch) + 10 (stale) = 50
            {
                "session_id": "s3",
                "total_todowrite_calls": 10,
                "total_update_interval_seconds": 10800,  # 1200s avg (acceptable, 20pts)
                "completion_batching_calls": 3,
                "immediate_completion_calls": 7,  # 30% (good, 20pts)
                "total_pending_duration_seconds": 18000,
                "total_inprogress_duration_seconds": 18000,
                "total_todos_tracked": 10,  # 1800s avg (acceptable, 10pts)
            },
            # Below 50: 10 (freq) + 0 (batch) + 0 (stale) = 10
            {
                "session_id": "s4",
                "total_todowrite_calls": 5,
                "total_update_interval_seconds": 6000,  # 1500s avg (poor, 10pts)
                "completion_batching_calls": 4,
                "immediate_completion_calls": 1,  # 80% (poor, 0pts)
                "total_pending_duration_seconds": 36000,
                "total_inprogress_duration_seconds": 27000,
                "total_todos_tracked": 10,  # 3150s avg (poor, 0pts)
            },
        ])

        # >80 means strictly greater
        assert result["high_discipline_sessions"] == 1
        # <50 means strictly less
        assert result["low_discipline_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_todowrite_calls": 50,
                "total_update_interval_seconds": 14700,  # 49 intervals * 300s avg
                "total_todo_count": 200,
                "completion_batching_calls": 5,
                "immediate_completion_calls": 45,
                "total_pending_duration_seconds": 15000,
                "total_inprogress_duration_seconds": 22500,
                "total_todos_tracked": 50,
                "avg_todos_per_update": 4.0,
            }
        ])

        assert result["sessions_with_todowrite"] == 1
        assert result["avg_todowrite_calls"] == 50.0
        # 14700 / 49 = 300s
        assert result["avg_time_between_updates"] == 300.0
        # avg_todos_per_update provided
        assert result["avg_todos_per_update"] == 4.0
        # 5 / (5 + 45) = 10%
        assert result["avg_completion_batching_ratio"] == 10.0
        # 15000 / 50 = 300s
        assert result["avg_pending_staleness"] == 300.0
        # 22500 / 50 = 450s
        assert result["avg_inprogress_staleness"] == 450.0
        # Should have high discipline score
        assert result["update_discipline_score"] > 70.0

    def test_zero_todos_tracked_no_staleness(self):
        """Verify zero todos tracked results in no staleness calculation."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 5,
                "total_pending_duration_seconds": 5000,
                "total_inprogress_duration_seconds": 3000,
                "total_todos_tracked": 0,
            }
        ])

        assert result["avg_pending_staleness"] == 0.0
        assert result["avg_inprogress_staleness"] == 0.0

    def test_no_completion_calls_no_batching_ratio(self):
        """Verify no completion calls results in no batching ratio."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "session1",
                "total_todowrite_calls": 5,
                "completion_batching_calls": 0,
                "immediate_completion_calls": 0,
            }
        ])

        assert result["avg_completion_batching_ratio"] == 0.0

    def test_discipline_score_excellent_all_metrics(self):
        """Verify discipline score calculation with excellent metrics."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "excellent",
                "total_todowrite_calls": 30,
                "total_update_interval_seconds": 5800,  # 200s avg (excellent)
                "completion_batching_calls": 2,
                "immediate_completion_calls": 28,  # 7% batching (excellent)
                "total_pending_duration_seconds": 9000,
                "total_inprogress_duration_seconds": 12000,
                "total_todos_tracked": 30,  # 300s + 400s / 2 = 350s avg (excellent)
            }
        ])

        # Should score: 40 (frequency) + 30 (batching) + 30 (staleness) = 100
        assert result["update_discipline_score"] == 100.0
        assert result["high_discipline_sessions"] == 1

    def test_discipline_score_poor_all_metrics(self):
        """Verify discipline score calculation with poor metrics."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "poor",
                "total_todowrite_calls": 3,
                "total_update_interval_seconds": 5000,  # 2500s avg (poor)
                "completion_batching_calls": 2,
                "immediate_completion_calls": 1,  # 67% batching (poor)
                "total_pending_duration_seconds": 10800,
                "total_inprogress_duration_seconds": 14400,
                "total_todos_tracked": 5,  # 2160s + 2880s / 2 = 2520s avg (poor)
            }
        ])

        # Should score: 10 (frequency) + 0 (batching) + 0 (staleness) = 10
        assert result["update_discipline_score"] == 10.0
        assert result["low_discipline_sessions"] == 1

    def test_discipline_score_mixed_metrics(self):
        """Verify discipline score calculation with mixed quality metrics."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "mixed",
                "total_todowrite_calls": 15,
                "total_update_interval_seconds": 7000,  # 500s avg (good, 30pts)
                "completion_batching_calls": 4,
                "immediate_completion_calls": 11,  # 27% batching (good, 20pts)
                "total_pending_duration_seconds": 15000,
                "total_inprogress_duration_seconds": 18000,
                "total_todos_tracked": 15,  # 1000s + 1200s / 2 = 1100s avg (good, 20pts)
            }
        ])

        # Should score: 30 (frequency) + 20 (batching) + 20 (staleness) = 70
        assert result["update_discipline_score"] == 70.0

    def test_discipline_score_only_frequency_data(self):
        """Verify discipline score with only frequency data available."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "frequency_only",
                "total_todowrite_calls": 25,
                "total_update_interval_seconds": 4800,  # 200s avg (excellent, 40pts)
            }
        ])

        # Should score: 40 (frequency) + 0 (no batching data) + 0 (no staleness data) = 40
        assert result["update_discipline_score"] == 40.0

    def test_discipline_score_many_calls_no_interval_data(self):
        """Verify discipline score assumes good when many calls but no interval."""
        result = analyze_session_todowrite_frequency([
            {
                "session_id": "many_calls",
                "total_todowrite_calls": 20,
                # No interval data, but many calls suggest good discipline
            }
        ])

        # Should score: 30 (assumed good for many calls) = 30
        assert result["update_discipline_score"] == 30.0
