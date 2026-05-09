"""Tests for session EnterPlanMode usage appropriateness analyzer."""

import pytest

from synthesis.session_enterplanmode_usage import analyze_session_enterplanmode_usage


class TestAnalyzeSessionEnterPlanModeUsage:
    """Test main analyzer function."""

    def test_empty_session(self):
        """Test analyzer with no records."""
        result = analyze_session_enterplanmode_usage([])
        assert result["total_turns"] == 0
        assert result["enterplanmode_invocations"] == 0
        assert result["appropriateness_score"] == 0.0
        assert result["usage_score"] == 0.0

    def test_none_input(self):
        """Test analyzer with None input."""
        result = analyze_session_enterplanmode_usage(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type(self):
        """Test analyzer rejects non-list input."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_enterplanmode_usage("not a list")

    def test_non_mapping_records_ignored(self):
        """Test that non-mapping records are skipped."""
        result = analyze_session_enterplanmode_usage([
            "invalid",
            123,
            None,
            {"tool_name": "EnterPlanMode"},
        ])
        assert result["enterplanmode_invocations"] == 1
        assert result["total_turns"] == 4


class TestEnterPlanModeTracking:
    """Test EnterPlanMode invocation tracking."""

    def test_enterplanmode_invocation_counted(self):
        """Test EnterPlanMode tool calls are counted."""
        result = analyze_session_enterplanmode_usage([
            {"turn_index": 0, "tool_name": "EnterPlanMode"},
            {"turn_index": 1, "tool_name": "EnterPlanMode"},
        ])
        assert result["enterplanmode_invocations"] == 2
        assert result["plan_mode_count"] == 2

    def test_exitplanmode_invocation_counted(self):
        """Test ExitPlanMode tool calls are counted."""
        result = analyze_session_enterplanmode_usage([
            {"turn_index": 0, "tool_name": "EnterPlanMode"},
            {"turn_index": 1, "tool_name": "ExitPlanMode"},
        ])
        assert result["enterplanmode_invocations"] == 1
        assert result["exitplanmode_invocations"] == 1
        assert result["plans_with_exit_tool"] == 1

    def test_exit_pattern_with_user_approval(self):
        """Test exit pattern tracking with user approval."""
        result = analyze_session_enterplanmode_usage([
            {"turn_index": 0, "tool_name": "EnterPlanMode"},
            {
                "turn_index": 1,
                "tool_name": "ExitPlanMode",
                "user_approved_plan": True,
            },
        ])
        assert result["plans_with_user_approval"] == 1
        assert result["exit_pattern_adherence"] == 100.0


class TestTaskCategorization:
    """Test task type categorization."""

    def test_appropriate_complex_task_planning(self):
        """Test planning is used for complex tasks (appropriate)."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "complex",
                "is_complex_task": True,
                "task_completed": True,
            },
        ])
        assert result["complex_tasks_planned"] == 1
        assert result["tasks_with_planning"] == 1
        assert result["planning_successes"] == 1
        assert result["over_planning_count"] == 0
        assert result["skipped_planning_opportunities"] == 0

    def test_appropriate_multi_file_task_planning(self):
        """Test planning is used for multi-file tasks (appropriate)."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_multi_file": True,
                "task_completed": True,
            },
        ])
        assert result["multi_file_tasks_planned"] == 1
        assert result["tasks_with_planning"] == 1
        assert result["over_planning_count"] == 0

    def test_appropriate_simple_task_direct_implementation(self):
        """Test simple task implemented directly (appropriate)."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_completed": True,
            },
        ])
        assert result["simple_tasks_direct"] == 1
        assert result["direct_implementations"] == 1
        assert result["over_planning_count"] == 0

    def test_appropriate_research_task_direct(self):
        """Test research task done directly (appropriate)."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "research",
                "task_completed": True,
            },
        ])
        assert result["research_tasks_direct"] == 1
        assert result["research_planning_count"] == 0


class TestAntiPatterns:
    """Test anti-pattern detection."""

    def test_over_planning_simple_task(self):
        """Test over-planning detection for simple tasks."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
                "task_description": "fix typo in README",
                "task_completed": True,
            },
        ])
        assert result["simple_tasks_planned"] == 1
        assert result["over_planning_count"] == 1
        assert result["total_anti_patterns"] == 1

    def test_research_task_planning_anti_pattern(self):
        """Test planning for research task (anti-pattern)."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "research",
                "task_description": "explore the codebase to understand routing",
                "task_completed": False,
            },
        ])
        assert result["research_tasks_planned"] == 1
        assert result["research_planning_count"] == 1
        assert result["total_anti_patterns"] == 1

    def test_skipped_planning_complex_task(self):
        """Test skipped planning for complex task (anti-pattern)."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
                "task_completed": False,
            },
        ])
        assert result["complex_tasks_direct"] == 1
        assert result["skipped_planning_opportunities"] == 1
        assert result["total_anti_patterns"] == 1

    def test_skipped_planning_multi_file_task(self):
        """Test skipped planning for multi-file task (anti-pattern)."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": False,
                "is_multi_file": True,
                "task_completed": False,
            },
        ])
        assert result["multi_file_tasks_direct"] == 1
        assert result["skipped_planning_opportunities"] == 1
        assert result["total_anti_patterns"] == 1

    def test_multiple_anti_patterns(self):
        """Test multiple anti-patterns in one session."""
        result = analyze_session_enterplanmode_usage([
            # Over-planning simple task
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
                "task_completed": True,
            },
            # Skipping planning for complex task
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
                "task_completed": False,
            },
            # Planning for research task
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "research",
                "task_completed": False,
            },
        ])
        assert result["total_anti_patterns"] == 3
        assert result["over_planning_count"] == 1
        assert result["skipped_planning_opportunities"] == 1
        assert result["research_planning_count"] == 1


class TestTaskDescriptionInference:
    """Test task type inference from descriptions."""

    def test_infer_research_task_from_description(self):
        """Test research task detection from description keywords."""
        descriptions = [
            "explore the authentication module",
            "search for error handling patterns",
            "find all uses of the API",
            "investigate the performance issue",
            "understand how routing works",
        ]
        for desc in descriptions:
            result = analyze_session_enterplanmode_usage([
                {"tool_name": "EnterPlanMode"},
                {
                    "is_implementation": True,
                    "used_planning": True,
                    "task_description": desc,
                },
            ])
            assert result["research_planning_count"] == 1, f"Failed for: {desc}"

    def test_infer_simple_task_from_description(self):
        """Test simple task detection from description keywords."""
        descriptions = [
            "fix typo in the docs",
            "add comment to function",
            "single line change",
            "trivial update",
            "small fix to README",
        ]
        for desc in descriptions:
            result = analyze_session_enterplanmode_usage([
                {"tool_name": "EnterPlanMode"},
                {
                    "is_implementation": True,
                    "used_planning": True,
                    "task_description": desc,
                },
            ])
            assert result["over_planning_count"] == 1, f"Failed for: {desc}"


class TestSuccessTracking:
    """Test success rate tracking."""

    def test_planning_success_rate(self):
        """Test planning success rate calculation."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_completed": True,
            },
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_completed": True,
            },
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_completed": False,
            },
        ])
        assert result["tasks_with_planning"] == 3
        assert result["planning_successes"] == 2
        assert result["planning_success_rate"] == 66.67

    def test_direct_success_rate(self):
        """Test direct implementation success rate calculation."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": False,
                "task_completed": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_completed": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_completed": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_completed": False,
            },
        ])
        assert result["tasks_without_planning"] == 4
        assert result["direct_successes"] == 3
        assert result["direct_success_rate"] == 75.0


class TestPlanAbandonment:
    """Test plan abandonment tracking."""

    def test_plan_abandonment_counted(self):
        """Test plan abandonment detection."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {"plan_abandoned": True},
            {"tool_name": "EnterPlanMode"},
            {"plan_abandoned": True},
            {"tool_name": "EnterPlanMode"},
        ])
        assert result["plan_abandonments"] == 2
        assert result["abandonment_ratio"] == 66.67

    def test_no_abandonment(self):
        """Test sessions without plan abandonment."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {"tool_name": "ExitPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_completed": True,
            },
        ])
        assert result["plan_abandonments"] == 0
        assert result["abandonment_ratio"] == 0.0


class TestCorrelationMetrics:
    """Test task type correlation metrics."""

    def test_complex_planning_ratio(self):
        """Test complex task planning ratio calculation."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
            },
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
            },
        ])
        assert result["complex_tasks_planned"] == 2
        assert result["complex_tasks_direct"] == 1
        assert result["complex_planning_ratio"] == 66.67

    def test_simple_planning_ratio(self):
        """Test simple task planning ratio calculation."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
            },
        ])
        assert result["simple_tasks_planned"] == 1
        assert result["simple_tasks_direct"] == 3
        assert result["simple_planning_ratio"] == 25.0

    def test_multi_file_planning_ratio(self):
        """Test multi-file task planning ratio calculation."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": True,
                "is_multi_file": True,
            },
            {
                "is_implementation": True,
                "used_planning": True,
                "is_multi_file": True,
            },
            {
                "is_implementation": True,
                "used_planning": True,
                "is_multi_file": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "is_multi_file": True,
            },
        ])
        assert result["multi_file_tasks_planned"] == 3
        assert result["multi_file_tasks_direct"] == 1
        assert result["multi_file_planning_ratio"] == 75.0


class TestAppropriatenessScore:
    """Test appropriateness score calculation."""

    def test_high_appropriateness_score(self):
        """Test high score for appropriate planning decisions."""
        result = analyze_session_enterplanmode_usage([
            # Complex tasks with planning (good)
            {"tool_name": "EnterPlanMode"},
            {"tool_name": "ExitPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            {"tool_name": "EnterPlanMode"},
            {"tool_name": "ExitPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            # Simple tasks without planning (good)
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_completed": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_completed": True,
            },
        ])
        assert result["complex_planning_ratio"] == 100.0
        assert result["simple_planning_ratio"] == 0.0
        assert result["anti_pattern_rate"] == 0.0
        assert result["exit_pattern_adherence"] == 100.0
        assert result["appropriateness_score"] >= 0.9

    def test_low_appropriateness_score_anti_patterns(self):
        """Test low score when anti-patterns are present."""
        result = analyze_session_enterplanmode_usage([
            # Planning for simple tasks (bad)
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
            },
            # Skipping planning for complex tasks (bad)
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
            },
            # Planning for research (bad)
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "research",
            },
        ])
        assert result["total_anti_patterns"] == 3
        assert result["anti_pattern_rate"] == 100.0
        assert result["appropriateness_score"] < 0.5


class TestUsageScore:
    """Test overall usage score calculation."""

    def test_high_usage_score(self):
        """Test high score for effective plan mode usage."""
        result = analyze_session_enterplanmode_usage([
            # Successful planning
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            # Successful direct implementation
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_completed": True,
            },
        ])
        assert result["planning_success_rate"] == 100.0
        assert result["direct_success_rate"] == 100.0
        assert result["complex_planning_ratio"] == 100.0
        assert result["abandonment_ratio"] == 0.0
        assert result["usage_score"] >= 0.9

    def test_low_usage_score_abandonments(self):
        """Test low score when plans are frequently abandoned."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "EnterPlanMode"},
            {"plan_abandoned": True},
            {"tool_name": "EnterPlanMode"},
            {"plan_abandoned": True},
            {"tool_name": "EnterPlanMode"},
            {"plan_abandoned": True},
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_completed": False,
            },
        ])
        assert result["enterplanmode_invocations"] == 4
        assert result["plan_abandonments"] == 3
        assert result["abandonment_ratio"] == 75.0
        assert result["usage_score"] < 0.5


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_zero_division_safety(self):
        """Test that zero division is handled gracefully."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "Read"},  # No implementations
        ])
        assert result["plan_mode_ratio"] == 0.0
        assert result["planning_success_rate"] == 0.0
        assert result["direct_success_rate"] == 0.0
        assert result["complex_planning_ratio"] == 0.0

    def test_plan_mode_ratio_calculation(self):
        """Test plan mode ratio calculation."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                "used_planning": True,
            },
            {
                "is_implementation": True,
                "used_planning": True,
            },
            {
                "is_implementation": True,
                "used_planning": False,
            },
        ])
        assert result["total_implementations"] == 3
        assert result["plan_mode_ratio"] == 66.67

    def test_case_insensitive_tool_names(self):
        """Test tool name matching is case insensitive."""
        result = analyze_session_enterplanmode_usage([
            {"tool_name": "enterplanmode"},
            {"tool_name": "ENTERPLANMODE"},
            {"tool_name": "EnterPlanMode"},
            {"tool_name": "exitplanmode"},
            {"tool_name": "EXITPLANMODE"},
        ])
        assert result["enterplanmode_invocations"] == 3
        assert result["exitplanmode_invocations"] == 2

    def test_missing_optional_fields(self):
        """Test handling of records with missing optional fields."""
        result = analyze_session_enterplanmode_usage([
            {
                "is_implementation": True,
                # Missing: used_planning, task_completed, is_complex_task, etc.
            },
        ])
        # Should not crash, should use defaults
        assert result["total_implementations"] == 1
        assert result["tasks_without_planning"] == 1


class TestRealWorldScenarios:
    """Test realistic session scenarios."""

    def test_ideal_planning_workflow(self):
        """Test ideal workflow with appropriate planning."""
        result = analyze_session_enterplanmode_usage([
            # Enter plan mode for complex feature
            {"turn_index": 0, "tool_name": "EnterPlanMode"},
            # Exit plan mode with approval
            {
                "turn_index": 1,
                "tool_name": "ExitPlanMode",
                "user_approved_plan": True,
            },
            # Implement with planning
            {
                "turn_index": 2,
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "is_multi_file": True,
                "task_completed": True,
            },
            # Simple fix done directly
            {
                "turn_index": 3,
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_description": "fix typo",
                "task_completed": True,
            },
        ])
        assert result["appropriateness_score"] >= 0.8
        assert result["usage_score"] >= 0.8
        assert result["total_anti_patterns"] == 0

    def test_poor_planning_judgment(self):
        """Test session with poor planning judgment."""
        result = analyze_session_enterplanmode_usage([
            # Over-plan simple task
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
                "task_description": "add comment",
            },
            # Skip planning for complex task
            {
                "is_implementation": True,
                "used_planning": False,
                "is_complex_task": True,
                "is_multi_file": True,
                "task_completed": False,
            },
            # Plan for research
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "research",
                "task_description": "explore routing",
            },
        ])
        assert result["total_anti_patterns"] == 3
        assert result["appropriateness_score"] < 0.5

    def test_mixed_quality_session(self):
        """Test session with mixed quality planning decisions."""
        result = analyze_session_enterplanmode_usage([
            # Good: Complex task with planning
            {"tool_name": "EnterPlanMode"},
            {"tool_name": "ExitPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "is_complex_task": True,
                "task_completed": True,
            },
            # Bad: Simple task with planning
            {"tool_name": "EnterPlanMode"},
            {
                "is_implementation": True,
                "used_planning": True,
                "task_type": "simple",
            },
            # Good: Simple task without planning
            {
                "is_implementation": True,
                "used_planning": False,
                "task_type": "simple",
                "task_completed": True,
            },
        ])
        assert result["total_implementations"] == 3
        assert result["total_anti_patterns"] == 1
        assert 0.5 <= result["appropriateness_score"] <= 0.9
