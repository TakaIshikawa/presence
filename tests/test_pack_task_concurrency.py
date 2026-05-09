"""Tests for pack Task agent concurrent execution analyzer."""

import pytest

from synthesis.pack_task_concurrency import analyze_pack_task_concurrency


class TestAnalyzePackTaskConcurrency:
    """Test main analyzer function."""

    def test_empty_pack_returns_zeroed_metrics(self):
        """Verify empty pack returns zero metrics."""
        result = analyze_pack_task_concurrency([])

        assert result["total_sessions"] == 0
        assert result["total_task_invocations"] == 0
        assert result["concurrent_task_calls"] == 0
        assert result["sequential_task_chains"] == 0
        assert result["concurrent_ratio"] == 0.0
        assert result["parallel_execution_time_seconds"] == 0.0
        assert result["sequential_equivalent_time_seconds"] == 0.0
        assert result["time_savings_seconds"] == 0.0
        assert result["time_savings_percentage"] == 0.0
        assert result["independent_tasks_count"] == 0
        assert result["actually_parallelized_tasks"] == 0
        assert result["parallelization_ratio"] == 0.0
        assert result["missed_opportunities"] == 0
        assert result["missed_opportunity_ratio"] == 0.0
        assert result["sessions_with_parallelization"] == 0
        assert result["parallelization_adoption_rate"] == 0.0
        assert result["concurrency_efficiency_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_concurrency(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_concurrency("not a list")

    def test_single_session_no_tasks(self):
        """Verify session with no Task invocations."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_task_invocations"] == 0

    def test_single_session_sequential_tasks(self):
        """Verify session with only sequential Task execution."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 5,
                "concurrent_task_calls": 0,
                "sequential_task_chains": 5,
                "parallel_execution_time_seconds": 0,
                "sequential_equivalent_time_seconds": 100.0,
                "independent_tasks_count": 3,
                "actually_parallelized_tasks": 0,
            }
        ])

        assert result["total_task_invocations"] == 5
        assert result["concurrent_task_calls"] == 0
        assert result["sequential_task_chains"] == 5
        assert result["concurrent_ratio"] == 0.0
        assert result["missed_opportunities"] == 3
        assert result["sessions_with_parallelization"] == 0

    def test_single_session_concurrent_tasks(self):
        """Verify session with concurrent Task execution."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 4,
                "concurrent_task_calls": 4,
                "sequential_task_chains": 0,
                "parallel_execution_time_seconds": 30.0,
                "sequential_equivalent_time_seconds": 60.0,
                "independent_tasks_count": 4,
                "actually_parallelized_tasks": 4,
            }
        ])

        assert result["total_task_invocations"] == 4
        assert result["concurrent_task_calls"] == 4
        assert result["concurrent_ratio"] == 100.0
        assert result["time_savings_seconds"] == 30.0
        assert result["time_savings_percentage"] == 50.0
        assert result["parallelization_ratio"] == 1.0
        assert result["missed_opportunities"] == 0
        assert result["sessions_with_parallelization"] == 1

    def test_multiple_sessions_aggregation(self):
        """Verify aggregation across multiple sessions."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 3,
                "concurrent_task_calls": 2,
                "sequential_task_chains": 1,
                "parallel_execution_time_seconds": 20.0,
                "sequential_equivalent_time_seconds": 30.0,
                "independent_tasks_count": 2,
                "actually_parallelized_tasks": 2,
            },
            {
                "session_id": "session2",
                "total_task_invocations": 5,
                "concurrent_task_calls": 3,
                "sequential_task_chains": 2,
                "parallel_execution_time_seconds": 40.0,
                "sequential_equivalent_time_seconds": 70.0,
                "independent_tasks_count": 4,
                "actually_parallelized_tasks": 3,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["total_task_invocations"] == 8
        assert result["concurrent_task_calls"] == 5
        assert result["sequential_task_chains"] == 3
        # 5/8 = 62.5%
        assert result["concurrent_ratio"] == 62.5
        # 20 + 40 = 60
        assert result["parallel_execution_time_seconds"] == 60.0
        # 30 + 70 = 100
        assert result["sequential_equivalent_time_seconds"] == 100.0
        # 100 - 60 = 40
        assert result["time_savings_seconds"] == 40.0
        # 40/100 = 40%
        assert result["time_savings_percentage"] == 40.0
        # 2 + 4 = 6
        assert result["independent_tasks_count"] == 6
        # 2 + 3 = 5
        assert result["actually_parallelized_tasks"] == 5
        # 5/6 = 0.833
        assert result["parallelization_ratio"] == 0.833
        # 6 - 5 = 1
        assert result["missed_opportunities"] == 1
        assert result["sessions_with_parallelization"] == 2

    def test_time_savings_calculation(self):
        """Verify time savings calculation."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "parallel_execution_time_seconds": 50.0,
                "sequential_equivalent_time_seconds": 100.0,
            }
        ])

        assert result["time_savings_seconds"] == 50.0
        assert result["time_savings_percentage"] == 50.0

    def test_no_time_savings_when_no_parallelization(self):
        """Verify no time savings without parallelization."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "parallel_execution_time_seconds": 0,
                "sequential_equivalent_time_seconds": 0,
            }
        ])

        assert result["time_savings_seconds"] == 0.0
        assert result["time_savings_percentage"] == 0.0

    def test_parallelization_ratio_calculation(self):
        """Verify parallelization ratio calculation."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "independent_tasks_count": 10,
                "actually_parallelized_tasks": 7,
            }
        ])

        # 7/10 = 0.7
        assert result["parallelization_ratio"] == 0.7
        assert result["missed_opportunities"] == 3
        assert result["missed_opportunity_ratio"] == 30.0

    def test_parallelization_ratio_capped_at_one(self):
        """Verify parallelization ratio cannot exceed 1.0."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "independent_tasks_count": 5,
                "actually_parallelized_tasks": 8,  # More than independent
            }
        ])

        # Should be capped at 1.0
        assert result["parallelization_ratio"] == 1.0

    def test_parallelization_adoption_rate(self):
        """Verify parallelization adoption rate calculation."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "concurrent_task_calls": 2,
            },
            {
                "session_id": "session2",
                "concurrent_task_calls": 0,
            },
            {
                "session_id": "session3",
                "concurrent_task_calls": 5,
            },
            {
                "session_id": "session4",
                "concurrent_task_calls": 0,
            },
        ])

        # 2/4 = 50%
        assert result["sessions_with_parallelization"] == 2
        assert result["parallelization_adoption_rate"] == 50.0

    def test_efficiency_score_perfect_parallelization(self):
        """Verify efficiency score with perfect parallelization."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 10,
                "concurrent_task_calls": 10,
                "parallel_execution_time_seconds": 50.0,
                "sequential_equivalent_time_seconds": 100.0,
                "independent_tasks_count": 10,
                "actually_parallelized_tasks": 10,
            }
        ])

        # 100% concurrent, 50% time savings, 1.0 parallelization, 0% missed
        assert result["concurrent_ratio"] == 100.0
        assert result["time_savings_percentage"] == 50.0
        assert result["parallelization_ratio"] == 1.0
        assert result["missed_opportunity_ratio"] == 0.0
        # Should be near perfect score
        assert result["concurrency_efficiency_score"] >= 0.9

    def test_efficiency_score_no_parallelization(self):
        """Verify efficiency score with no parallelization."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 10,
                "concurrent_task_calls": 0,
                "sequential_task_chains": 10,
                "parallel_execution_time_seconds": 0,
                "sequential_equivalent_time_seconds": 0,
                "independent_tasks_count": 8,
                "actually_parallelized_tasks": 0,
            }
        ])

        # 0% concurrent, 0% time savings, 0 parallelization, 100% missed
        assert result["concurrency_efficiency_score"] == 0.0

    def test_efficiency_score_moderate_parallelization(self):
        """Verify efficiency score with moderate parallelization."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 10,
                "concurrent_task_calls": 5,
                "parallel_execution_time_seconds": 60.0,
                "sequential_equivalent_time_seconds": 100.0,
                "independent_tasks_count": 8,
                "actually_parallelized_tasks": 5,
            }
        ])

        # 50% concurrent, 40% time savings, 0.625 parallelization, 37.5% missed
        assert result["concurrent_ratio"] == 50.0
        assert result["time_savings_percentage"] == 40.0
        assert result["parallelization_ratio"] == 0.625
        assert result["missed_opportunity_ratio"] == 37.5
        # Should be good score (targets are met/close)
        assert 0.7 <= result["concurrency_efficiency_score"] <= 0.9

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled gracefully."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                # All fields missing
            }
        ])

        assert result["total_sessions"] == 1
        assert result["total_task_invocations"] == 0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_concurrency([
            "not a dict",
            {
                "session_id": "session1",
                "total_task_invocations": 3,
            },
            None,
            42,
        ])

        assert result["total_sessions"] == 1
        assert result["total_task_invocations"] == 3

    def test_negative_values_handled(self):
        """Verify negative values are handled gracefully."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": -5,  # Invalid but will be used as-is
                "concurrent_task_calls": -2,
                "parallel_execution_time_seconds": -10.0,
            }
        ])

        # Negative values are summed as-is, but percentages handle gracefully
        assert result["total_task_invocations"] == -5
        assert result["concurrent_task_calls"] == -2
        # Concurrent ratio should be 0 due to denominator check
        assert result["concurrent_ratio"] == 0.0

    def test_string_numeric_values_parsed(self):
        """Verify string numeric values are parsed."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": "5",
                "concurrent_task_calls": "3",
                "parallel_execution_time_seconds": "30.5",
                "sequential_equivalent_time_seconds": "60.0",
            }
        ])

        assert result["total_task_invocations"] == 5
        assert result["concurrent_task_calls"] == 3
        assert result["parallel_execution_time_seconds"] == 30.5
        assert result["sequential_equivalent_time_seconds"] == 60.0

    def test_float_task_counts_truncated(self):
        """Verify float task counts are truncated to integers."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "total_task_invocations": 5.9,
                "concurrent_task_calls": 3.2,
            }
        ])

        assert result["total_task_invocations"] == 5
        assert result["concurrent_task_calls"] == 3

    def test_zero_independent_tasks_no_division_error(self):
        """Verify no division by zero when independent_tasks_count is 0."""
        result = analyze_pack_task_concurrency([
            {
                "session_id": "session1",
                "independent_tasks_count": 0,
                "actually_parallelized_tasks": 0,
            }
        ])

        assert result["parallelization_ratio"] == 0.0
        assert result["missed_opportunity_ratio"] == 0.0

    def test_comprehensive_pack_analysis(self):
        """Verify comprehensive pack with mixed patterns."""
        result = analyze_pack_task_concurrency([
            # Session 1: Perfect parallelization
            {
                "session_id": "session1",
                "total_task_invocations": 4,
                "concurrent_task_calls": 4,
                "sequential_task_chains": 0,
                "parallel_execution_time_seconds": 20.0,
                "sequential_equivalent_time_seconds": 40.0,
                "independent_tasks_count": 4,
                "actually_parallelized_tasks": 4,
            },
            # Session 2: No parallelization
            {
                "session_id": "session2",
                "total_task_invocations": 3,
                "concurrent_task_calls": 0,
                "sequential_task_chains": 3,
                "parallel_execution_time_seconds": 0,
                "sequential_equivalent_time_seconds": 0,
                "independent_tasks_count": 2,
                "actually_parallelized_tasks": 0,
            },
            # Session 3: Mixed pattern
            {
                "session_id": "session3",
                "total_task_invocations": 6,
                "concurrent_task_calls": 4,
                "sequential_task_chains": 2,
                "parallel_execution_time_seconds": 35.0,
                "sequential_equivalent_time_seconds": 60.0,
                "independent_tasks_count": 5,
                "actually_parallelized_tasks": 4,
            },
        ])

        assert result["total_sessions"] == 3
        assert result["total_task_invocations"] == 13
        assert result["concurrent_task_calls"] == 8
        assert result["sequential_task_chains"] == 5
        # 8/13 = 61.54%
        assert result["concurrent_ratio"] == 61.54
        assert result["parallel_execution_time_seconds"] == 55.0
        assert result["sequential_equivalent_time_seconds"] == 100.0
        assert result["time_savings_seconds"] == 45.0
        assert result["time_savings_percentage"] == 45.0
        assert result["independent_tasks_count"] == 11
        assert result["actually_parallelized_tasks"] == 8
        # 8/11 = 0.727
        assert result["parallelization_ratio"] == 0.727
        assert result["missed_opportunities"] == 3
        # 3/11 = 27.27%
        assert result["missed_opportunity_ratio"] == 27.27
        assert result["sessions_with_parallelization"] == 2
        # 2/3 = 66.67%
        assert result["parallelization_adoption_rate"] == 66.67
