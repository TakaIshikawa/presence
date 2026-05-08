"""Tests for session background task usage analyzer."""

import pytest

from synthesis.session_background_task_usage import (
    analyze_session_background_task_usage,
    _average,
    _classify_efficiency_pattern,
    _is_backgroundable,
    _percentage,
)


class TestAnalyzeSessionBackgroundTaskUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_background_task_usage([])

        assert result["total_tool_calls"] == 0
        assert result["background_task_count"] == 0
        assert result["background_usage_rate"] == 0.0
        assert result["tools_backgrounded"] == {}
        assert result["completed_tasks"] == 0
        assert result["abandoned_tasks"] == 0
        assert result["completion_rate"] == 0.0
        assert result["average_duration"] == 0.0
        assert result["missed_opportunities"] == 0
        assert result["efficiency_pattern"] == "empty"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_background_task_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_background_task_usage("not a list")

    def test_no_background_tasks(self):
        """Verify session with no background tasks."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False, "turn_index": 0},
            {"tool_name": "Edit", "run_in_background": False, "turn_index": 1},
            {"tool_name": "Bash", "command": "ls", "run_in_background": False, "turn_index": 2},
        ])

        assert result["total_tool_calls"] == 3
        assert result["background_task_count"] == 0
        assert result["background_usage_rate"] == 0.0
        assert result["efficiency_pattern"] == "empty"

    def test_single_background_task(self):
        """Verify single background task is tracked."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_checked": True, "was_completed": True, "duration_seconds": 10.5},
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 1
        assert result["background_usage_rate"] == 100.0
        assert result["tools_backgrounded"] == {"Bash": 1}
        assert result["completed_tasks"] == 1
        assert result["abandoned_tasks"] == 0
        assert result["completion_rate"] == 100.0
        assert result["average_duration"] == 10.5

    def test_multiple_background_tasks_different_tools(self):
        """Verify multiple background tasks across different tools."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm build", "run_in_background": True,
             "was_completed": True, "duration_seconds": 15.0},
            {"tool_name": "Task", "run_in_background": True,
             "was_completed": True, "duration_seconds": 8.0},
            {"tool_name": "Bash", "command": "pytest", "run_in_background": True,
             "was_checked": True, "was_completed": True, "duration_seconds": 12.0},
            {"tool_name": "Read", "run_in_background": False},
        ])

        assert result["total_tool_calls"] == 4
        assert result["background_task_count"] == 3
        assert result["background_usage_rate"] == 75.0
        assert result["tools_backgrounded"] == {"Bash": 2, "Task": 1}
        assert result["completed_tasks"] == 3
        assert result["average_duration"] == 11.67

    def test_abandoned_background_tasks(self):
        """Verify abandoned tasks (not checked) are tracked."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm install", "run_in_background": True,
             "was_checked": False, "was_completed": False},
            {"tool_name": "Bash", "command": "make", "run_in_background": True,
             "was_checked": False, "was_completed": False},
        ])

        assert result["background_task_count"] == 2
        assert result["abandoned_tasks"] == 2
        assert result["completed_tasks"] == 0
        assert result["completion_rate"] == 0.0
        assert result["efficiency_pattern"] == "abandoned"

    def test_missed_backgrounding_opportunities(self):
        """Verify detection of commands that should have been backgrounded."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm install express", "run_in_background": False,
             "duration_seconds": 8.0},
            {"tool_name": "Bash", "command": "pytest tests/", "run_in_background": False,
             "duration_seconds": 12.0},
            {"tool_name": "Bash", "command": "cargo build --release", "run_in_background": False,
             "duration_seconds": 45.0},
        ])

        assert result["missed_opportunities"] == 3
        assert result["efficiency_pattern"] == "underutilized"

    def test_mixed_usage_pattern(self):
        """Verify mixed foreground and background usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True, "duration_seconds": 5.0},
            {"tool_name": "Edit", "run_in_background": False},
            {"tool_name": "Bash", "command": "git status", "run_in_background": False},
        ])

        assert result["total_tool_calls"] == 4
        assert result["background_task_count"] == 1
        assert result["background_usage_rate"] == 25.0
        assert result["completed_tasks"] == 1

    def test_optimal_efficiency_pattern(self):
        """Verify optimal pattern detection (high usage, high completion)."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm build", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Task", "run_in_background": True, "was_completed": True},
            {"tool_name": "Read", "run_in_background": False},
        ])

        assert result["background_usage_rate"] == 75.0
        assert result["completion_rate"] == 100.0
        assert result["efficiency_pattern"] == "optimal"

    def test_effective_efficiency_pattern(self):
        """Verify effective pattern (moderate usage, good completion)."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Edit", "run_in_background": False},
            {"tool_name": "Bash", "command": "git status", "run_in_background": False},
            {"tool_name": "Bash", "command": "npm build", "run_in_background": True,
             "was_completed": True},
        ])

        # 2/5 = 40% usage rate, 100% completion
        assert result["background_usage_rate"] == 40.0
        assert result["completion_rate"] == 100.0
        assert result["efficiency_pattern"] == "effective"

    def test_minimal_efficiency_pattern(self):
        """Verify minimal pattern (low usage, no issues)."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Edit", "run_in_background": False},
            {"tool_name": "Write", "run_in_background": False},
            {"tool_name": "Grep", "run_in_background": False},
            {"tool_name": "Bash", "command": "ls", "run_in_background": False,
             "duration_seconds": 0.5},
        ])

        # 1/6 = 16.67% but treated as low, no missed opportunities
        assert result["background_usage_rate"] < 20.0
        assert result["missed_opportunities"] == 0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_background_task_usage([
            "not a dict",
            {"tool_name": "Bash", "run_in_background": True, "was_completed": True},
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 1

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_background_task_usage([
            {"run_in_background": True},
            {"tool_name": "Bash", "run_in_background": True, "was_completed": True},
        ])

        assert result["total_tool_calls"] == 1

    def test_duration_aggregation(self):
        """Verify average duration calculation."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 10.0,
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 20.0,
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 30.0,
             "was_completed": True},
        ])

        assert result["average_duration"] == 20.0

    def test_duration_with_missing_values(self):
        """Verify duration calculation handles missing values."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 10.0,
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True,  # No duration
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 20.0,
             "was_completed": True},
        ])

        # Average of only the two with values: (10 + 20) / 2 = 15
        assert result["average_duration"] == 15.0

    def test_zero_duration_ignored(self):
        """Verify zero durations are ignored in average."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 10.0,
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 0,
             "was_completed": True},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 20.0,
             "was_completed": True},
        ])

        assert result["average_duration"] == 15.0


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_normal(self):
        """Verify percentage calculation."""
        assert _percentage(3, 10) == 30.0
        assert _percentage(1, 4) == 25.0

    def test_percentage_zero_denominator(self):
        """Verify percentage returns 0.0 for zero denominator."""
        assert _percentage(5, 0) == 0.0

    def test_percentage_rounding(self):
        """Verify percentage is rounded to 2 decimals."""
        assert _percentage(1, 3) == 33.33

    def test_average_normal(self):
        """Verify average calculation."""
        assert _average([10.0, 20.0, 30.0]) == 20.0

    def test_average_empty_list(self):
        """Verify average returns 0.0 for empty list."""
        assert _average([]) == 0.0

    def test_average_rounding(self):
        """Verify average is rounded to 2 decimals."""
        assert _average([10.0, 15.0, 20.0]) == 15.0

    def test_is_backgroundable_npm_install(self):
        """Verify npm install is detected as backgroundable."""
        record = {"tool_name": "Bash", "command": "npm install express"}
        assert _is_backgroundable(record) is True

    def test_is_backgroundable_pytest(self):
        """Verify pytest is detected as backgroundable."""
        record = {"tool_name": "Bash", "command": "pytest tests/"}
        assert _is_backgroundable(record) is True

    def test_is_backgroundable_cargo_build(self):
        """Verify cargo build is detected as backgroundable."""
        record = {"tool_name": "Bash", "command": "cargo build --release"}
        assert _is_backgroundable(record) is True

    def test_is_backgroundable_task_tool(self):
        """Verify Task tool is detected as backgroundable."""
        record = {"tool_name": "Task"}
        assert _is_backgroundable(record) is True

    def test_is_backgroundable_quick_command(self):
        """Verify quick commands are not backgroundable."""
        record = {"tool_name": "Bash", "command": "ls -la"}
        assert _is_backgroundable(record) is False

    def test_is_backgroundable_read_tool(self):
        """Verify Read tool is not backgroundable."""
        record = {"tool_name": "Read"}
        assert _is_backgroundable(record) is False

    def test_classify_efficiency_pattern_optimal(self):
        """Verify optimal pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=75.0,
            completion_rate=90.0,
            abandoned_tasks=0,
            missed_opportunities=0,
        )
        assert pattern == "optimal"

    def test_classify_efficiency_pattern_underutilized(self):
        """Verify underutilized pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=5.0,
            completion_rate=80.0,
            abandoned_tasks=0,
            missed_opportunities=5,
        )
        assert pattern == "underutilized"

    def test_classify_efficiency_pattern_abandoned(self):
        """Verify abandoned pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=30.0,
            completion_rate=20.0,
            abandoned_tasks=5,
            missed_opportunities=0,
        )
        assert pattern == "abandoned"

    def test_classify_efficiency_pattern_effective(self):
        """Verify effective pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=15.0,
            completion_rate=80.0,
            abandoned_tasks=1,
            missed_opportunities=1,
        )
        assert pattern == "effective"

    def test_classify_efficiency_pattern_minimal(self):
        """Verify minimal pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=5.0,
            completion_rate=100.0,
            abandoned_tasks=0,
            missed_opportunities=0,
        )
        assert pattern == "minimal"

    def test_classify_efficiency_pattern_empty(self):
        """Verify empty pattern classification."""
        pattern = _classify_efficiency_pattern(
            usage_rate=0.0,
            completion_rate=0.0,
            abandoned_tasks=0,
            missed_opportunities=0,
        )
        assert pattern == "empty"


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_efficient_background_workflow(self):
        """Simulate efficient workflow with appropriate background usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Bash", "command": "npm install", "run_in_background": True,
             "was_completed": True, "duration_seconds": 15.0},
            {"tool_name": "Edit", "run_in_background": False},
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True, "duration_seconds": 10.0},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Bash", "command": "npm run build", "run_in_background": True,
             "was_completed": True, "duration_seconds": 20.0},
        ])

        assert result["background_usage_rate"] == 50.0
        assert result["completion_rate"] == 100.0
        assert result["efficiency_pattern"] in ("optimal", "effective")
        assert result["average_duration"] == 15.0

    def test_no_background_with_missed_opportunities(self):
        """Simulate workflow missing background opportunities."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm install", "run_in_background": False,
             "duration_seconds": 12.0},
            {"tool_name": "Bash", "command": "cargo build", "run_in_background": False,
             "duration_seconds": 30.0},
            {"tool_name": "Bash", "command": "pytest", "run_in_background": False,
             "duration_seconds": 15.0},
        ])

        assert result["background_task_count"] == 0
        assert result["missed_opportunities"] == 3
        assert result["efficiency_pattern"] == "underutilized"

    def test_heavy_background_usage(self):
        """Simulate session with heavy background task usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Task", "run_in_background": True, "was_completed": True},
            {"tool_name": "Bash", "command": "npm build", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Bash", "command": "docker build", "run_in_background": True,
             "was_completed": True},
            {"tool_name": "Read", "run_in_background": False},
        ])

        assert result["background_task_count"] == 4
        assert result["background_usage_rate"] == 80.0
        assert result["efficiency_pattern"] == "optimal"

    def test_background_tasks_with_partial_completion(self):
        """Simulate background tasks with mixed completion status."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "command": "npm install", "run_in_background": True,
             "was_checked": True, "was_completed": True},
            {"tool_name": "Bash", "command": "npm test", "run_in_background": True,
             "was_checked": True, "was_completed": True},
            {"tool_name": "Bash", "command": "npm build", "run_in_background": True,
             "was_checked": False, "was_completed": False},
            {"tool_name": "Task", "run_in_background": True,
             "was_checked": True, "was_completed": True},
        ])

        assert result["background_task_count"] == 4
        assert result["completed_tasks"] == 3
        assert result["abandoned_tasks"] == 1
        assert result["completion_rate"] == 75.0
