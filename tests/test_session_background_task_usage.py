"""Tests for session background task usage analyzer."""

import pytest

from synthesis.session_background_task_usage import (
    analyze_session_background_task_usage,
<<<<<<< HEAD
    _detect_missed_opportunities,
    _determine_reason,
    _calculate_efficiency_impact,
=======
    _average,
    _classify_efficiency_pattern,
    _is_backgroundable,
    _percentage,
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
)


class TestAnalyzeSessionBackgroundTaskUsage:
    """Test main analyzer function."""

<<<<<<< HEAD
    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
=======
    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
        result = analyze_session_background_task_usage([])

        assert result["total_tool_calls"] == 0
        assert result["background_task_count"] == 0
        assert result["background_usage_rate"] == 0.0
<<<<<<< HEAD
        assert result["background_tool_types"] == {}
        assert result["avg_background_duration"] == 0.0
        assert result["completion_rate"] == 100.0  # Edge case
        assert result["abandonment_rate"] == 0.0
        assert result["missed_opportunities"] == []
        assert result["efficiency_impact"] == "none"
=======
        assert result["tools_backgrounded"] == {}
        assert result["completed_tasks"] == 0
        assert result["abandoned_tasks"] == 0
        assert result["completion_rate"] == 0.0
        assert result["average_duration"] == 0.0
        assert result["missed_opportunities"] == 0
        assert result["efficiency_pattern"] == "empty"
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_background_task_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_background_task_usage("not a list")

<<<<<<< HEAD
    def test_single_foreground_task_no_background(self):
        """Verify single foreground task shows no background usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False}
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 0
        assert result["background_usage_rate"] == 0.0

    def test_background_task_detected(self):
        """Verify background task is detected."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True}
        ])

        assert result["background_task_count"] == 1
        assert result["background_usage_rate"] == 100.0

    def test_mixed_background_and_foreground(self):
        """Verify mixed background and foreground tasks."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Bash", "run_in_background": True},
            {"tool_name": "Edit", "run_in_background": False},
        ])

        # 1 out of 3 = 33.33%
        assert result["total_tool_calls"] == 3
        assert result["background_task_count"] == 1
        assert result["background_usage_rate"] == 33.33

    def test_background_tool_types_tracked(self):
        """Verify background tool types are tracked."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True},
            {"tool_name": "Bash", "run_in_background": True},
            {"tool_name": "Task", "run_in_background": True},
        ])

        assert result["background_tool_types"]["Bash"] == 2
        assert result["background_tool_types"]["Task"] == 1

    def test_background_duration_calculated(self):
        """Verify background duration is calculated."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 10.0},
            {"tool_name": "Bash", "run_in_background": True, "duration_seconds": 20.0},
        ])

        # Average of 10 and 20 = 15.0
        assert result["avg_background_duration"] == 15.0

    def test_completion_rate_calculated(self):
        """Verify completion rate is calculated correctly."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Bash", "run_in_background": True, "completed": False},
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
        ])

        # 3 out of 4 = 75%
        assert result["completion_rate"] == 75.0
        assert result["abandonment_rate"] == 25.0

    def test_completion_defaults_to_true(self):
        """Verify completion defaults to True when not specified."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True},
        ])

        assert result["completion_rate"] == 100.0
        assert result["abandonment_rate"] == 0.0

    def test_missed_opportunity_long_running_command(self):
        """Verify missed opportunity for long-running command."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 15.0,
                "command": "npm run build",
            }
        ])

        opportunities = result["missed_opportunities"]
        assert len(opportunities) > 0
        assert opportunities[0]["reason"] == "build_command"

    def test_missed_opportunity_test_command(self):
        """Verify missed opportunity for test command."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 12.0,
                "command": "pytest tests/",
            }
        ])

        opportunities = result["missed_opportunities"]
        assert any(opp["reason"] == "test_command" for opp in opportunities)

    def test_no_missed_opportunity_short_command(self):
        """Verify short commands don't trigger missed opportunities."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 2.0,
                "command": "ls -la",
            }
        ])

        assert len(result["missed_opportunities"]) == 0

    def test_no_missed_opportunity_for_non_bash_tools(self):
        """Verify non-Bash tools don't trigger missed opportunities."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Read",
                "run_in_background": False,
                "duration_seconds": 15.0,
            }
        ])

        assert len(result["missed_opportunities"]) == 0

    def test_efficiency_impact_high(self):
        """Verify high efficiency impact classification."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
        ])

        # 50% usage with 100% completion = high
        assert result["efficiency_impact"] == "high"

    def test_efficiency_impact_medium(self):
        """Verify medium efficiency impact classification."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
        ])

        # 20% usage with 100% completion = medium
        assert result["efficiency_impact"] == "medium"

    def test_efficiency_impact_low(self):
        """Verify low efficiency impact classification."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Read", "run_in_background": False},
        ])

        # Low usage and poor completion = low
        assert result["efficiency_impact"] == "low"

    def test_efficiency_impact_none(self):
        """Verify none efficiency impact classification."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False},
        ])

        assert result["efficiency_impact"] == "none"

    def test_missed_opportunities_limited_to_ten(self):
        """Verify missed opportunities are limited to 10."""
        records = [
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 15.0,
                "command": f"npm run build-{i}",
            }
            for i in range(20)
        ]

        result = analyze_session_background_task_usage(records)
        assert len(result["missed_opportunities"]) <= 10
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_background_task_usage([
            "not a dict",
<<<<<<< HEAD
            {"tool_name": "Bash", "run_in_background": True},
        ])

        assert result["total_tool_calls"] == 1
=======
            {"tool_name": "Bash", "run_in_background": True, "was_completed": True},
        ])

        assert result["total_tool_calls"] == 1
        assert result["background_task_count"] == 1
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_background_task_usage([
            {"run_in_background": True},
<<<<<<< HEAD
            {"tool_name": "Bash", "run_in_background": True},
=======
            {"tool_name": "Bash", "run_in_background": True, "was_completed": True},
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
        ])

        assert result["total_tool_calls"] == 1

<<<<<<< HEAD
    def test_empty_tool_name_skipped(self):
        """Verify records with empty tool_name are skipped."""
        result = analyze_session_background_task_usage([
            {"tool_name": "", "run_in_background": True},
            {"tool_name": "Bash", "run_in_background": True},
        ])

        assert result["total_tool_calls"] == 1


class TestDetectMissedOpportunities:
    """Test missed opportunity detection helper."""

    def test_empty_input_returns_empty(self):
        """Verify empty input returns no opportunities."""
        opportunities = _detect_missed_opportunities([])
        assert opportunities == []

    def test_build_command_detected(self):
        """Verify build commands are detected."""
        commands = [{
            "tool_name": "Bash",
            "duration_seconds": 15.0,
            "command": "npm run build",
        }]
        opportunities = _detect_missed_opportunities(commands)

        assert len(opportunities) == 1
        assert opportunities[0]["reason"] == "build_command"

    def test_test_command_detected(self):
        """Verify test commands are detected."""
        commands = [{
            "tool_name": "Bash",
            "duration_seconds": 12.0,
            "command": "pytest tests/",
        }]
        opportunities = _detect_missed_opportunities(commands)

        assert len(opportunities) == 1
        assert opportunities[0]["reason"] == "test_command"

    def test_install_command_detected(self):
        """Verify install commands are detected."""
        commands = [{
            "tool_name": "Bash",
            "duration_seconds": 20.0,
            "command": "npm install",
        }]
        opportunities = _detect_missed_opportunities(commands)

        assert len(opportunities) == 1
        assert opportunities[0]["reason"] == "install_command"

    def test_very_long_command_detected(self):
        """Verify very long commands are detected."""
        commands = [{
            "tool_name": "Bash",
            "duration_seconds": 35.0,
            "command": "some long running command",
        }]
        opportunities = _detect_missed_opportunities(commands)

        assert len(opportunities) == 1
        assert opportunities[0]["reason"] == "long_running"

    def test_non_bash_tools_not_detected(self):
        """Verify non-Bash/Task tools are not detected."""
        commands = [{
            "tool_name": "Read",
            "duration_seconds": 15.0,
            "command": "some command",
        }]
        opportunities = _detect_missed_opportunities(commands)

        assert len(opportunities) == 0


class TestDetermineReason:
    """Test reason determination helper."""

    def test_build_command_reason(self):
        """Verify build command reason."""
        assert _determine_reason("npm run build", 10.0) == "build_command"
        assert _determine_reason("cargo build", 10.0) == "build_command"

    def test_test_command_reason(self):
        """Verify test command reason."""
        assert _determine_reason("pytest tests/", 10.0) == "test_command"
        assert _determine_reason("npm test", 10.0) == "test_command"

    def test_install_command_reason(self):
        """Verify install command reason."""
        assert _determine_reason("npm install", 10.0) == "install_command"
        assert _determine_reason("pip install -r requirements.txt", 10.0) == "install_command"

    def test_long_running_reason(self):
        """Verify long-running reason."""
        assert _determine_reason("some command", 35.0) == "long_running"


class TestCalculateEfficiencyImpact:
    """Test efficiency impact calculation helper."""

    def test_none_impact(self):
        """Verify none impact for no background usage."""
        assert _calculate_efficiency_impact(0.0, 100.0) == "none"

    def test_high_impact(self):
        """Verify high impact for high usage and completion."""
        assert _calculate_efficiency_impact(25.0, 85.0) == "high"

    def test_medium_impact(self):
        """Verify medium impact for moderate usage."""
        assert _calculate_efficiency_impact(15.0, 70.0) == "medium"

    def test_low_impact(self):
        """Verify low impact for low usage or completion."""
        assert _calculate_efficiency_impact(5.0, 50.0) == "low"
        assert _calculate_efficiency_impact(15.0, 50.0) == "low"
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

<<<<<<< HEAD
    def test_no_background_usage_session(self):
        """Simulate session with no background usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Read", "run_in_background": False},
            {"tool_name": "Edit", "run_in_background": False},
            {"tool_name": "Bash", "run_in_background": False, "command": "git status"},
        ])

        assert result["background_usage_rate"] == 0.0
        assert result["efficiency_impact"] == "none"

    def test_high_background_usage_session(self):
        """Simulate session with high background usage."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": True, "duration_seconds": 10.0},
            {"tool_name": "Task", "run_in_background": True, "completed": True, "duration_seconds": 15.0},
            {"tool_name": "Read", "run_in_background": False},
        ])

        # 2 out of 3 = 66.67%
        assert result["background_usage_rate"] == 66.67
        assert result["completion_rate"] == 100.0
        assert result["efficiency_impact"] == "high"

    def test_session_with_missed_opportunities(self):
        """Simulate session with missed background opportunities."""
        result = analyze_session_background_task_usage([
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 25.0,
                "command": "npm run build",
            },
            {
                "tool_name": "Bash",
                "run_in_background": False,
                "duration_seconds": 18.0,
                "command": "pytest tests/",
            },
        ])

        assert len(result["missed_opportunities"]) == 2

    def test_empty_session(self):
        """Simulate empty session."""
        result = analyze_session_background_task_usage([])

        assert result["total_tool_calls"] == 0
        assert result["efficiency_impact"] == "none"

    def test_mixed_completion_session(self):
        """Simulate session with mixed completion rates."""
        result = analyze_session_background_task_usage([
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Bash", "run_in_background": True, "completed": False},
            {"tool_name": "Bash", "run_in_background": True, "completed": True},
            {"tool_name": "Bash", "run_in_background": True, "completed": False},
        ])

        # 50% completion
        assert result["completion_rate"] == 50.0
        assert result["abandonment_rate"] == 50.0
=======
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
>>>>>>> relay/claude-code/add-session-background-task-usage-analyzer-01KR3GME
