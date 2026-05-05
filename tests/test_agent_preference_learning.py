"""Tests for agent preference learning analysis."""

import pytest

from engagement.agent_preference_learning import (
    ToolUsage,
    SessionBehavior,
    ToolAffinityScore,
    StrategyEvolution,
    OptimizationAdoption,
    analyze_agent_preference_learning,
    _calculate_tool_affinity_scores,
    _track_strategy_evolution,
    _calculate_optimization_adoption,
    MIN_TOOL_USAGE_FOR_AFFINITY,
    STRONG_AFFINITY_THRESHOLD,
    MIN_SESSIONS_FOR_EVOLUTION,
)


class TestToolUsage:
    """Test ToolUsage dataclass."""

    def test_create_tool_usage(self):
        """Verify tool usage can be created."""
        usage = ToolUsage(
            tool_name="Read",
            task_type="file_read",
            usage_count=5,
            session_id="session_1",
        )
        assert usage.tool_name == "Read"
        assert usage.task_type == "file_read"
        assert usage.usage_count == 5
        assert usage.session_id == "session_1"

    def test_tool_usage_frozen(self):
        """Verify tool usage is immutable."""
        usage = ToolUsage("Read", "file_read", 5, "session_1")
        with pytest.raises(AttributeError):
            usage.usage_count = 10


class TestSessionBehavior:
    """Test SessionBehavior dataclass."""

    def test_create_session_behavior(self):
        """Verify session behavior can be created."""
        session = SessionBehavior(
            session_id="session_1",
            session_number=0,
            tool_usages=[],
            read_with_offset_count=10,
            read_total_count=20,
            verify_command_count=2,
            cache_query_count=5,
            cache_snapshot_count=3,
            optimization_mode="optimized",
        )
        assert session.session_id == "session_1"
        assert session.optimization_mode == "optimized"

    def test_session_behavior_frozen(self):
        """Verify session behavior is immutable."""
        session = SessionBehavior(
            "session_1", 0, [], 10, 20, 2, 5, 3, "optimized"
        )
        with pytest.raises(AttributeError):
            session.session_number = 1


class TestCalculateToolAffinityScores:
    """Test tool affinity score calculation."""

    def test_empty_sessions(self):
        """Verify empty sessions returns no affinities."""
        affinity = _calculate_tool_affinity_scores([])
        assert affinity == {}

    def test_single_tool_below_threshold(self):
        """Verify tool below minimum usage threshold is excluded."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 2, "s1"),  # Below MIN_TOOL_USAGE_FOR_AFFINITY
                ],
                read_with_offset_count=0,
                read_total_count=2,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            )
        ]
        affinity = _calculate_tool_affinity_scores(sessions)
        assert affinity == {}

    def test_single_tool_above_threshold(self):
        """Verify tool above minimum usage threshold creates affinity."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 5, "s1"),
                ],
                read_with_offset_count=0,
                read_total_count=5,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            )
        ]
        affinity = _calculate_tool_affinity_scores(sessions)
        assert "file_read" in affinity
        assert affinity["file_read"].preferred_tool == "Read"
        assert affinity["file_read"].affinity_score == 1.0  # 100% usage
        assert affinity["file_read"].usage_count == 5

    def test_multiple_tools_for_task(self):
        """Verify affinity calculation with multiple tools for same task."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 7, "s1"),
                    ToolUsage("Cat", "file_read", 3, "s1"),
                ],
                read_with_offset_count=0,
                read_total_count=10,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            )
        ]
        affinity = _calculate_tool_affinity_scores(sessions)
        assert "file_read" in affinity
        assert affinity["file_read"].preferred_tool == "Read"
        assert affinity["file_read"].affinity_score == 0.7  # 7/10
        assert affinity["file_read"].alternative_tools == {"Cat": 3}

    def test_multiple_task_types(self):
        """Verify affinity calculation for multiple task types."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 5, "s1"),
                    ToolUsage("Grep", "search", 8, "s1"),
                    ToolUsage("Glob", "search", 2, "s1"),
                ],
                read_with_offset_count=0,
                read_total_count=15,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            )
        ]
        affinity = _calculate_tool_affinity_scores(sessions)
        assert "file_read" in affinity
        assert "search" in affinity
        assert affinity["file_read"].preferred_tool == "Read"
        assert affinity["search"].preferred_tool == "Grep"
        assert affinity["search"].affinity_score == 0.8  # 8/10

    def test_aggregate_across_sessions(self):
        """Verify tool usage is aggregated across multiple sessions."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 2, "s1"),
                ],
                read_with_offset_count=0,
                read_total_count=2,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            ),
            SessionBehavior(
                session_id="s2",
                session_number=1,
                tool_usages=[
                    ToolUsage("Read", "file_read", 3, "s2"),
                ],
                read_with_offset_count=0,
                read_total_count=3,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode=None,
            ),
        ]
        affinity = _calculate_tool_affinity_scores(sessions)
        assert "file_read" in affinity
        assert affinity["file_read"].usage_count == 5  # 2 + 3


class TestTrackStrategyEvolution:
    """Test strategy evolution tracking."""

    def test_insufficient_sessions(self):
        """Verify insufficient sessions returns empty list."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 0, 0, 0, None),
        ]
        evolution = _track_strategy_evolution(sessions)
        assert evolution == []

    def test_targeted_reads_evolution(self):
        """Verify targeted reads strategy evolution tracking."""
        sessions = [
            SessionBehavior("s1", 0, [], 2, 10, 0, 0, 0, None),  # 20% targeted
            SessionBehavior("s2", 1, [], 5, 10, 0, 0, 0, None),  # 50% targeted
            SessionBehavior("s3", 2, [], 8, 10, 0, 0, 0, None),  # 80% targeted
        ]
        evolution = _track_strategy_evolution(sessions)

        targeted_reads = [e for e in evolution if e.strategy_name == "targeted_reads"]
        assert len(targeted_reads) == 1
        assert targeted_reads[0].initial_adoption_rate == 0.2
        assert targeted_reads[0].final_adoption_rate == 0.8
        assert targeted_reads[0].adoption_delta == 0.6
        assert targeted_reads[0].trend == "increasing"

    def test_verification_strategy_evolution(self):
        """Verify verification command strategy evolution tracking."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 5, 0, 0, None),  # 50% verify
            SessionBehavior("s2", 1, [], 0, 10, 2, 0, 0, None),  # 20% verify
        ]
        evolution = _track_strategy_evolution(sessions)

        verify = [e for e in evolution if e.strategy_name == "verification_commands"]
        assert len(verify) == 1
        assert verify[0].initial_adoption_rate == 0.5
        assert verify[0].final_adoption_rate == 0.2
        assert verify[0].trend == "decreasing"

    def test_cache_utilization_evolution(self):
        """Verify cache utilization strategy evolution tracking."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 0, 1, 1, None),  # 2 cache uses
            SessionBehavior("s2", 1, [], 0, 10, 0, 5, 5, None),  # 10 cache uses
        ]
        evolution = _track_strategy_evolution(sessions)

        cache = [e for e in evolution if e.strategy_name == "cache_utilization"]
        assert len(cache) == 1
        assert cache[0].initial_adoption_rate == 2.0
        assert cache[0].final_adoption_rate == 10.0
        assert cache[0].trend == "increasing"

    def test_stable_strategy(self):
        """Verify stable strategy detection."""
        sessions = [
            SessionBehavior("s1", 0, [], 5, 10, 0, 0, 0, None),  # 50%
            SessionBehavior("s2", 1, [], 5, 10, 0, 0, 0, None),  # 50%
        ]
        evolution = _track_strategy_evolution(sessions)

        targeted_reads = [e for e in evolution if e.strategy_name == "targeted_reads"]
        assert len(targeted_reads) == 1
        assert targeted_reads[0].trend == "stable"


class TestCalculateOptimizationAdoption:
    """Test optimization adoption calculation."""

    def test_empty_sessions(self):
        """Verify empty sessions returns zero adoption."""
        adoption = _calculate_optimization_adoption([])
        assert adoption.baseline_sessions == 0
        assert adoption.optimized_sessions == 0
        assert adoption.adoption_rate == 0.0

    def test_all_baseline_sessions(self):
        """Verify all baseline sessions calculation."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 0, 0, 0, "baseline"),
            SessionBehavior("s2", 1, [], 0, 10, 0, 0, 0, "baseline"),
        ]
        adoption = _calculate_optimization_adoption(sessions)
        assert adoption.baseline_sessions == 2
        assert adoption.optimized_sessions == 0
        assert adoption.adoption_rate == 0.0

    def test_all_optimized_sessions(self):
        """Verify all optimized sessions calculation."""
        sessions = [
            SessionBehavior("s1", 0, [], 8, 10, 0, 5, 3, "optimized"),
            SessionBehavior("s2", 1, [], 9, 10, 0, 7, 2, "optimized"),
        ]
        adoption = _calculate_optimization_adoption(sessions)
        assert adoption.baseline_sessions == 0
        assert adoption.optimized_sessions == 2
        assert adoption.adoption_rate == 1.0
        assert adoption.avg_targeted_read_rate == 0.85  # (0.8 + 0.9) / 2
        assert adoption.avg_cache_utilization == 8.5  # (8 + 9) / 2

    def test_mixed_sessions(self):
        """Verify mixed baseline and optimized sessions."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 0, 0, 0, "baseline"),
            SessionBehavior("s2", 1, [], 8, 10, 0, 5, 3, "optimized"),
            SessionBehavior("s3", 2, [], 9, 10, 0, 7, 2, "optimized"),
        ]
        adoption = _calculate_optimization_adoption(sessions)
        assert adoption.baseline_sessions == 1
        assert adoption.optimized_sessions == 2
        assert adoption.adoption_rate == round(2 / 3, 3)  # 0.667

    def test_unspecified_mode_sessions(self):
        """Verify sessions with None optimization mode."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, 10, 0, 0, 0, None),
            SessionBehavior("s2", 1, [], 8, 10, 0, 5, 3, "optimized"),
        ]
        adoption = _calculate_optimization_adoption(sessions)
        assert adoption.baseline_sessions == 0
        assert adoption.optimized_sessions == 1
        assert adoption.adoption_rate == 0.5


class TestAnalyzeAgentPreferenceLearning:
    """Test complete agent preference learning analysis."""

    def test_empty_sessions(self):
        """Verify analysis of empty sessions."""
        result = analyze_agent_preference_learning([])
        assert result["preference_trends"] == {}
        assert result["strategy_evolution"] == []
        assert result["tool_affinity_scores"] == {}
        assert result["optimization_adoption_rate"]["adoption_rate"] == 0.0

    def test_single_session(self):
        """Verify analysis of single session."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 5, "s1"),
                ],
                read_with_offset_count=3,
                read_total_count=5,
                verify_command_count=0,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode="optimized",
            )
        ]
        result = analyze_agent_preference_learning(sessions)
        assert "file_read" in result["tool_affinity_scores"]
        assert result["strategy_evolution"] == []  # Need 2+ sessions
        assert result["optimization_adoption_rate"]["adoption_rate"] == 1.0

    def test_multi_session_evolution(self):
        """Verify multi-session preference learning analysis."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 10, "s1"),
                    ToolUsage("Grep", "search", 5, "s1"),
                ],
                read_with_offset_count=2,
                read_total_count=10,
                verify_command_count=1,
                cache_query_count=0,
                cache_snapshot_count=0,
                optimization_mode="baseline",
            ),
            SessionBehavior(
                session_id="s2",
                session_number=1,
                tool_usages=[
                    ToolUsage("Read", "file_read", 12, "s2"),
                    ToolUsage("Grep", "search", 8, "s2"),
                ],
                read_with_offset_count=8,
                read_total_count=12,
                verify_command_count=0,
                cache_query_count=5,
                cache_snapshot_count=3,
                optimization_mode="optimized",
            ),
            SessionBehavior(
                session_id="s3",
                session_number=2,
                tool_usages=[
                    ToolUsage("Read", "file_read", 15, "s3"),
                    ToolUsage("Grep", "search", 10, "s3"),
                ],
                read_with_offset_count=13,
                read_total_count=15,
                verify_command_count=0,
                cache_query_count=7,
                cache_snapshot_count=5,
                optimization_mode="optimized",
            ),
        ]
        result = analyze_agent_preference_learning(sessions)

        # Check tool affinities
        assert "file_read" in result["tool_affinity_scores"]
        assert "search" in result["tool_affinity_scores"]
        assert result["tool_affinity_scores"]["file_read"]["preferred_tool"] == "Read"
        assert result["tool_affinity_scores"]["search"]["preferred_tool"] == "Grep"

        # Check strategy evolution
        assert len(result["strategy_evolution"]) >= 1
        strategy_names = [s["strategy_name"] for s in result["strategy_evolution"]]
        assert "targeted_reads" in strategy_names

        # Check preference trends (should be same as strategy_evolution but in dict form)
        assert "targeted_reads" in result["preference_trends"]

        # Check optimization adoption
        assert result["optimization_adoption_rate"]["baseline_sessions"] == 1
        assert result["optimization_adoption_rate"]["optimized_sessions"] == 2
        assert result["optimization_adoption_rate"]["adoption_rate"] == round(2 / 3, 3)

    def test_invalid_sessions_not_sequence(self):
        """Verify error on non-sequence input."""
        with pytest.raises(ValueError, match="sessions must be a sequence"):
            analyze_agent_preference_learning("not a sequence")

    def test_invalid_sessions_wrong_type(self):
        """Verify error on wrong element type."""
        with pytest.raises(ValueError, match="must contain SessionBehavior instances"):
            analyze_agent_preference_learning([{"session": "s1"}])

    def test_invalid_negative_session_number(self):
        """Verify error on negative session number."""
        sessions = [
            SessionBehavior("s1", -1, [], 0, 10, 0, 0, 0, None)
        ]
        with pytest.raises(ValueError, match="session_number must be non-negative"):
            analyze_agent_preference_learning(sessions)

    def test_invalid_negative_read_count(self):
        """Verify error on negative read count."""
        sessions = [
            SessionBehavior("s1", 0, [], 0, -1, 0, 0, 0, None)
        ]
        with pytest.raises(ValueError, match="read_total_count must be non-negative"):
            analyze_agent_preference_learning(sessions)

    def test_invalid_offset_exceeds_total(self):
        """Verify error when offset count exceeds total."""
        sessions = [
            SessionBehavior("s1", 0, [], 15, 10, 0, 0, 0, None)
        ]
        with pytest.raises(ValueError, match="read_with_offset_count cannot exceed read_total_count"):
            analyze_agent_preference_learning(sessions)

    def test_result_structure(self):
        """Verify result structure contains all required fields."""
        sessions = [
            SessionBehavior(
                session_id="s1",
                session_number=0,
                tool_usages=[
                    ToolUsage("Read", "file_read", 5, "s1"),
                ],
                read_with_offset_count=3,
                read_total_count=5,
                verify_command_count=0,
                cache_query_count=2,
                cache_snapshot_count=1,
                optimization_mode="optimized",
            ),
            SessionBehavior(
                session_id="s2",
                session_number=1,
                tool_usages=[
                    ToolUsage("Read", "file_read", 7, "s2"),
                ],
                read_with_offset_count=6,
                read_total_count=7,
                verify_command_count=0,
                cache_query_count=4,
                cache_snapshot_count=2,
                optimization_mode="optimized",
            ),
        ]
        result = analyze_agent_preference_learning(sessions)

        # Check top-level keys
        assert "preference_trends" in result
        assert "strategy_evolution" in result
        assert "tool_affinity_scores" in result
        assert "optimization_adoption_rate" in result

        # Check strategy_evolution structure
        if result["strategy_evolution"]:
            strategy = result["strategy_evolution"][0]
            assert "strategy_name" in strategy
            assert "initial_adoption_rate" in strategy
            assert "final_adoption_rate" in strategy
            assert "adoption_delta" in strategy
            assert "sessions_tracked" in strategy
            assert "trend" in strategy

        # Check tool_affinity_scores structure
        if result["tool_affinity_scores"]:
            affinity = list(result["tool_affinity_scores"].values())[0]
            assert "task_type" in affinity
            assert "preferred_tool" in affinity
            assert "affinity_score" in affinity
            assert "usage_count" in affinity
            assert "alternative_tools" in affinity

        # Check optimization_adoption_rate structure
        adoption = result["optimization_adoption_rate"]
        assert "baseline_sessions" in adoption
        assert "optimized_sessions" in adoption
        assert "adoption_rate" in adoption
        assert "avg_targeted_read_rate" in adoption
        assert "avg_cache_utilization" in adoption
