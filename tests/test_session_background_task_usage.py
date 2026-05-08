"""Tests for session background task usage analyzer."""

import pytest

from synthesis.session_background_task_usage import (
    analyze_session_background_task_usage,
    _percentage,
    _average,
)


class TestAnalyzeSessionBackgroundTaskUsage:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_background_task_usage([])

        assert result["total_tasks"] == 0
        assert result["background_tasks"] == 0
        assert result["background_rate"] == 0.0
        assert result["tool_distribution"] == {}
        assert result["completion_rate"] == 0.0
        assert result["abandoned_tasks"] == 0
        assert result["avg_duration_ms"] == 0.0
        assert result["missed_opportunities"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_background_task_usage(None)
        assert result["total_tasks"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_background_task_usage("not a list")

    def test_single_foreground_task(self):
        """Verify single foreground task is counted."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": False,
                "completed": True,
            }
        ])

        assert result["total_tasks"] == 1
        assert result["background_tasks"] == 0
        assert result["background_rate"] == 0.0

    def test_single_background_task(self):
        """Verify single background task is counted."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
            }
        ])

        assert result["total_tasks"] == 1
        assert result["background_tasks"] == 1
        assert result["background_rate"] == 100.0
        assert result["tool_distribution"]["Bash"] == 1

    def test_background_task_completion_rate(self):
        """Verify completion rate calculation."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task3", "tool_name": "Bash", "run_in_background": True, "completed": False},
        ])

        # 2 of 3 completed = 66.67%
        assert result["completion_rate"] == 66.67
        assert result["abandoned_tasks"] == 1

    def test_tool_distribution_tracking(self):
        """Verify tool distribution is tracked."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task3", "tool_name": "Task", "run_in_background": True, "completed": True},
        ])

        assert result["tool_distribution"]["Bash"] == 2
        assert result["tool_distribution"]["Task"] == 1

    def test_duration_tracking(self):
        """Verify duration is tracked for background tasks."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 1000,
            },
            {
                "task_id": "task2",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 3000,
            },
        ])

        # Average: (1000 + 3000) / 2 = 2000
        assert result["avg_duration_ms"] == 2000.0

    def test_missed_opportunities_detection(self):
        """Verify missed opportunities for backgrounding are detected."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": False,
                "completed": True,
                "duration_ms": 10000,  # 10 seconds, should be backgrounded
            },
            {
                "task_id": "task2",
                "tool_name": "Bash",
                "run_in_background": False,
                "completed": True,
                "duration_ms": 1000,  # 1 second, too fast
            },
        ])

        assert result["missed_opportunities"] == 1

    def test_mixed_background_and_foreground(self):
        """Verify mixed background and foreground tasks."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task2", "tool_name": "Read", "run_in_background": False, "completed": True},
            {"task_id": "task3", "tool_name": "Task", "run_in_background": True, "completed": True},
            {"task_id": "task4", "tool_name": "Edit", "run_in_background": False, "completed": True},
        ])

        assert result["total_tasks"] == 4
        assert result["background_tasks"] == 2
        assert result["background_rate"] == 50.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_background_task_usage([
            "not a dict",
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
            },
        ])

        assert result["total_tasks"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "run_in_background": True,
                # Missing tool_name, completed, duration_ms
            }
        ])

        assert result["background_tasks"] == 1
        # Without completed field, should be treated as False (abandoned)
        assert result["abandoned_tasks"] == 1

    def test_zero_background_tasks_completion_rate(self):
        """Verify completion rate is 0 when no background tasks."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Read", "run_in_background": False, "completed": True},
        ])

        assert result["completion_rate"] == 0.0

    def test_all_background_tasks_completed(self):
        """Verify 100% completion rate when all complete."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": True, "completed": True},
        ])

        assert result["completion_rate"] == 100.0
        assert result["abandoned_tasks"] == 0

    def test_duration_only_for_background_tasks(self):
        """Verify duration is only tracked for background tasks."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 2000,
            },
            {
                "task_id": "task2",
                "tool_name": "Read",
                "run_in_background": False,
                "completed": True,
                "duration_ms": 100,  # Should not affect avg
            },
        ])

        # Only background task duration counted
        assert result["avg_duration_ms"] == 2000.0

    def test_invalid_duration_ignored(self):
        """Verify invalid duration values are ignored."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": "not a number",
            },
            {
                "task_id": "task2",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 1000,
            },
        ])

        # Only valid duration counted
        assert result["avg_duration_ms"] == 1000.0

    def test_zero_duration_ignored(self):
        """Verify zero duration is ignored."""
        result = analyze_session_background_task_usage([
            {
                "task_id": "task1",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 0,
            },
            {
                "task_id": "task2",
                "tool_name": "Bash",
                "run_in_background": True,
                "completed": True,
                "duration_ms": 2000,
            },
        ])

        # Zero duration ignored
        assert result["avg_duration_ms"] == 2000.0


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_simple_percentage(self):
        """Verify simple percentage calculation."""
        assert _percentage(1, 4) == 25.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestAverage:
    """Test average calculation helper."""

    def test_zero_count_returns_zero(self):
        """Verify zero count returns 0.0."""
        assert _average(10.0, 0) == 0.0

    def test_negative_count_returns_zero(self):
        """Verify negative count returns 0.0."""
        assert _average(10.0, -5) == 0.0

    def test_simple_average(self):
        """Verify simple average calculation."""
        assert _average(10.0, 4) == 2.5

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _average(10.0, 3) == 3.33


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_heavy_background_usage_session(self):
        """Simulate session with heavy background task usage."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True, "duration_ms": 5000},
            {"task_id": "task2", "tool_name": "Task", "run_in_background": True, "completed": True, "duration_ms": 10000},
            {"task_id": "task3", "tool_name": "Bash", "run_in_background": True, "completed": True, "duration_ms": 7500},
        ])

        assert result["background_rate"] == 100.0
        assert result["completion_rate"] == 100.0
        assert result["avg_duration_ms"] == 7500.0

    def test_no_background_usage_session(self):
        """Simulate session with no background task usage."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Read", "run_in_background": False, "completed": True},
            {"task_id": "task2", "tool_name": "Edit", "run_in_background": False, "completed": True},
            {"task_id": "task3", "tool_name": "Write", "run_in_background": False, "completed": True},
        ])

        assert result["background_rate"] == 0.0
        assert result["background_tasks"] == 0

    def test_selective_backgrounding_session(self):
        """Simulate session with selective background usage."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Read", "run_in_background": False, "completed": True, "duration_ms": 100},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": True, "completed": True, "duration_ms": 5000},
            {"task_id": "task3", "tool_name": "Edit", "run_in_background": False, "completed": True, "duration_ms": 200},
            {"task_id": "task4", "tool_name": "Task", "run_in_background": True, "completed": True, "duration_ms": 15000},
        ])

        assert result["background_rate"] == 50.0
        assert result["tool_distribution"]["Bash"] == 1
        assert result["tool_distribution"]["Task"] == 1

    def test_abandoned_tasks_scenario(self):
        """Simulate scenario with some abandoned background tasks."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": True, "completed": True},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": True, "completed": False},
            {"task_id": "task3", "tool_name": "Task", "run_in_background": True, "completed": False},
        ])

        assert result["completion_rate"] == 33.33
        assert result["abandoned_tasks"] == 2

    def test_missed_opportunities_scenario(self):
        """Simulate scenario with missed backgrounding opportunities."""
        result = analyze_session_background_task_usage([
            {"task_id": "task1", "tool_name": "Bash", "run_in_background": False, "completed": True, "duration_ms": 15000},
            {"task_id": "task2", "tool_name": "Bash", "run_in_background": False, "completed": True, "duration_ms": 8000},
            {"task_id": "task3", "tool_name": "Bash", "run_in_background": False, "completed": True, "duration_ms": 1000},
        ])

        # Two tasks took >5 seconds and weren't backgrounded
        assert result["missed_opportunities"] == 2
