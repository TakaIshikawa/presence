"""Tests for session Task tool coordination analyzer."""

import pytest

from synthesis.session_task_tool_coordination import (
    analyze_session_task_tool_coordination,
)


class TestAnalyzeSessionTaskToolCoordination:
    """Test main analyzer function."""

    def test_empty_sessions_returns_zeroed_metrics(self):
        """Verify empty session list returns zero metrics."""
        result = analyze_session_task_tool_coordination([])

        assert result["total_sessions"] == 0
        assert result["sessions_with_task_tool"] == 0
        assert result["avg_task_calls"] == 0.0
        assert result["avg_bash_subagent_ratio"] == 0.0
        assert result["avg_general_subagent_ratio"] == 0.0
        assert result["avg_explore_subagent_ratio"] == 0.0
        assert result["avg_plan_subagent_ratio"] == 0.0
        assert result["avg_task_description_length"] == 0.0
        assert result["avg_parallel_task_ratio"] == 0.0
        assert result["avg_background_task_ratio"] == 0.0
        assert result["avg_output_correlation_rate"] == 0.0
        assert result["high_coordination_sessions"] == 0
        assert result["low_coordination_sessions"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_task_tool_coordination(None)
        assert result["total_sessions"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_task_tool_coordination("not a list")

    def test_session_with_no_task_calls(self):
        """Verify session with zero Task calls handled gracefully."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 0,
            }
        ])

        assert result["total_sessions"] == 1
        assert result["sessions_with_task_tool"] == 0

    def test_high_coordination_perfect_correlation(self):
        """Verify high coordination with perfect output correlation."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "bash_subagent_calls": 5,
                "explore_subagent_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 10,
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 10.0
        # 10 correlated / 10 outputs = 100%
        assert result["avg_output_correlation_rate"] == 100.0
        assert result["high_coordination_sessions"] == 1
        assert result["low_coordination_sessions"] == 0

    def test_low_coordination_poor_correlation(self):
        """Verify low coordination with poor output correlation."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "task_output_calls": 10,
                "correlated_outputs": 2,
            }
        ])

        # 2 correlated / 10 outputs = 20%
        assert result["avg_output_correlation_rate"] == 20.0
        assert result["high_coordination_sessions"] == 0
        assert result["low_coordination_sessions"] == 1

    def test_subagent_type_distribution(self):
        """Verify subagent type distribution calculated correctly."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 20,
                "bash_subagent_calls": 8,
                "general_subagent_calls": 5,
                "explore_subagent_calls": 4,
                "plan_subagent_calls": 3,
            }
        ])

        # 8 / 20 = 40%
        assert result["avg_bash_subagent_ratio"] == 40.0
        # 5 / 20 = 25%
        assert result["avg_general_subagent_ratio"] == 25.0
        # 4 / 20 = 20%
        assert result["avg_explore_subagent_ratio"] == 20.0
        # 3 / 20 = 15%
        assert result["avg_plan_subagent_ratio"] == 15.0

    def test_parallel_task_calls(self):
        """Verify parallel Task call ratio calculation."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 20,
                "parallel_task_calls": 8,
            }
        ])

        # 8 / 20 = 40%
        assert result["avg_parallel_task_ratio"] == 40.0

    def test_background_task_usage(self):
        """Verify background task usage ratio."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "background_task_calls": 3,
            }
        ])

        # 3 / 10 = 30%
        assert result["avg_background_task_ratio"] == 30.0

    def test_task_description_length(self):
        """Verify task description length calculation."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 5,
                "total_description_length": 500,
            }
        ])

        # 500 / 5 = 100 chars average
        assert result["avg_task_description_length"] == 100.0

    def test_multiple_sessions_averaged(self):
        """Verify metrics averaged across multiple sessions."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                "bash_subagent_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 10,
            },
            {
                "session_id": "session2",
                "total_task_calls": 20,
                "bash_subagent_calls": 10,
                "task_output_calls": 20,
                "correlated_outputs": 10,
            },
        ])

        assert result["total_sessions"] == 2
        assert result["sessions_with_task_tool"] == 2
        # (10 + 20) / 2 = 15
        assert result["avg_task_calls"] == 15.0
        # Both 50% bash
        assert result["avg_bash_subagent_ratio"] == 50.0
        # (100% + 50%) / 2 = 75%
        assert result["avg_output_correlation_rate"] == 75.0

    def test_orphaned_task_outputs(self):
        """Verify handling of TaskOutput calls without matching Tasks."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 5,
            }
        ])

        # 5 correlated / 10 outputs = 50%
        assert result["avg_output_correlation_rate"] == 50.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_task_tool_coordination([
            "not a dict",
            {
                "session_id": "session1",
                "total_task_calls": 5,
            },
        ])

        assert result["total_sessions"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for integer fields."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": True,
                "bash_subagent_calls": False,
            }
        ])

        assert result["sessions_with_task_tool"] == 0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "session1",
                "total_task_calls": 10,
                # Missing most fields
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 10.0
        # Missing fields result in 0.0 averages
        assert result["avg_bash_subagent_ratio"] == 0.0

    def test_boundary_coordination_classification(self):
        """Verify boundary cases for coordination classification."""
        result = analyze_session_task_tool_coordination([
            # Exactly 80% (should not be high)
            {
                "session_id": "s1",
                "total_task_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 8,
            },
            # Just above 80% (should be high)
            {
                "session_id": "s2",
                "total_task_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 9,
            },
            # Exactly 50% (should not be low)
            {
                "session_id": "s3",
                "total_task_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 5,
            },
            # Just below 50% (should be low)
            {
                "session_id": "s4",
                "total_task_calls": 5,
                "task_output_calls": 10,
                "correlated_outputs": 4,
            },
        ])

        # >80% means strictly greater
        assert result["high_coordination_sessions"] == 1
        # <50% means strictly less
        assert result["low_coordination_sessions"] == 1

    def test_comprehensive_session_all_fields(self):
        """Verify comprehensive session with all fields populated."""
        result = analyze_session_task_tool_coordination([
            {
                "session_id": "comprehensive",
                "session_title": "Test Session",
                "total_task_calls": 50,
                "bash_subagent_calls": 15,
                "general_subagent_calls": 10,
                "explore_subagent_calls": 20,
                "plan_subagent_calls": 5,
                "total_description_length": 5000,
                "parallel_task_calls": 20,
                "background_task_calls": 15,
                "task_output_calls": 45,
                "correlated_outputs": 40,
            }
        ])

        assert result["sessions_with_task_tool"] == 1
        assert result["avg_task_calls"] == 50.0
        # 15 / 50 = 30%
        assert result["avg_bash_subagent_ratio"] == 30.0
        # 10 / 50 = 20%
        assert result["avg_general_subagent_ratio"] == 20.0
        # 20 / 50 = 40%
        assert result["avg_explore_subagent_ratio"] == 40.0
        # 5 / 50 = 10%
        assert result["avg_plan_subagent_ratio"] == 10.0
        # 5000 / 50 = 100
        assert result["avg_task_description_length"] == 100.0
        # 20 / 50 = 40%
        assert result["avg_parallel_task_ratio"] == 40.0
        # 15 / 50 = 30%
        assert result["avg_background_task_ratio"] == 30.0
        # 40 / 45 = 88.89%
        assert 88.0 <= result["avg_output_correlation_rate"] <= 89.0
        assert result["high_coordination_sessions"] == 1
