"""Tests for session EnterPlanMode usage analyzer."""

import pytest

from synthesis.session_enterplanmode_usage import analyze_session_enterplanmode_usage


class TestAnalyzeSessionEnterPlanModeUsage:
    """Test main analyzer function."""

    def test_empty_session(self):
        result = analyze_session_enterplanmode_usage([])
        assert result["total_turns"] == 0
        assert result["enterplanmode_invocations"] == 0

    def test_planning_used(self):
        result = analyze_session_enterplanmode_usage([
            {"turn_index": 0, "tool_name": "EnterPlanMode"},
            {
                "turn_index": 1,
                "is_implementation": True,
                "used_planning": True,
                "task_completed": True,
            },
        ])

        assert result["enterplanmode_invocations"] == 1
        assert result["tasks_with_planning"] == 1
        assert result["planning_success_rate"] == 100.0

    def test_direct_implementation(self):
        result = analyze_session_enterplanmode_usage([
            {
                "turn_index": 0,
                "is_implementation": True,
                "used_planning": False,
                "task_completed": True,
            },
        ])

        assert result["direct_implementations"] == 1
        assert result["direct_success_rate"] == 100.0

    def test_complex_task_correlation(self):
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
                "task_completed": False,
            },
        ])

        assert result["complex_tasks_planned"] == 1
        assert result["complex_tasks_direct"] == 1
        assert result["complex_planning_ratio"] == 50.0
