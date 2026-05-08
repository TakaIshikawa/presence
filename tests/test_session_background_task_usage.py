"""Tests for session background task usage analyzer."""

import pytest

from synthesis.session_background_task_usage import (
    analyze_session_background_task_usage,
    _detect_missed_opportunities,
    _determine_reason,
    _calculate_efficiency_impact,
)


class TestAnalyzeSessionBackgroundTaskUsage:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_background_task_usage([])

        assert result["total_tool_calls"] == 0
        assert result["background_task_count"] == 0
        assert result["background_usage_rate"] == 0.0
        assert result["background_tool_types"] == {}
        assert result["avg_background_duration"] == 0.0
        assert result["completion_rate"] == 100.0  # Edge case
        assert result["abandonment_rate"] == 0.0
        assert result["missed_opportunities"] == []
        assert result["efficiency_impact"] == "none"

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_background_task_usage(None)
        assert result["total_tool_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_background_task_usage("not a list")

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

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_background_task_usage([
            "not a dict",
            {"tool_name": "Bash", "run_in_background": True},
        ])

        assert result["total_tool_calls"] == 1

    def test_missing_tool_name_skipped(self):
        """Verify records without tool_name are skipped."""
        result = analyze_session_background_task_usage([
            {"run_in_background": True},
            {"tool_name": "Bash", "run_in_background": True},
        ])

        assert result["total_tool_calls"] == 1

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


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

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
