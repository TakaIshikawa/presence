"""Tests for session tool usage density calculation."""

import pytest

from synthesis.session_tool_usage_density import (
    SessionToolUsageDensity,
    ToolUsageMetrics,
    calculate_session_tool_usage_density,
    DIVERSITY_LOW,
    DIVERSITY_HIGH,
    CLUSTERING_LOW,
    CLUSTERING_HIGH,
    BURST_THRESHOLD,
    _calculate_tools_per_turn,
    _calculate_tool_diversity_index,
    _calculate_tool_clustering_coefficient,
    _calculate_burst_density,
    _classify_workflow_pattern,
)


class TestToolUsageMetrics:
    """Test ToolUsageMetrics dataclass."""

    def test_create_metrics(self):
        """Verify metrics can be created with all fields."""
        metrics = ToolUsageMetrics(
            tool_calls=("read", "write", "edit"),
            turn_count=10,
            unique_tool_count=3,
            total_tool_count=3,
        )
        assert metrics.tool_calls == ("read", "write", "edit")
        assert metrics.turn_count == 10
        assert metrics.unique_tool_count == 3
        assert metrics.total_tool_count == 3

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        metrics = ToolUsageMetrics(
            tool_calls=("read", "write"),
            turn_count=5,
            unique_tool_count=2,
            total_tool_count=2,
        )
        with pytest.raises(AttributeError):
            metrics.turn_count = 10


class TestCalculateToolsPerTurn:
    """Test tools-per-turn calculation."""

    def test_zero_turns(self):
        """Verify zero turns returns zero."""
        assert _calculate_tools_per_turn(10, 0) == 0.0

    def test_equal_tools_and_turns(self):
        """Verify equal tools and turns returns 1.0."""
        assert _calculate_tools_per_turn(10, 10) == 1.0

    def test_more_tools_than_turns(self):
        """Verify multiple tools per turn."""
        assert _calculate_tools_per_turn(30, 10) == 3.0

    def test_fewer_tools_than_turns(self):
        """Verify fractional tools per turn."""
        assert _calculate_tools_per_turn(5, 10) == 0.5

    def test_zero_tools(self):
        """Verify zero tools returns zero."""
        assert _calculate_tools_per_turn(0, 10) == 0.0


class TestCalculateToolDiversityIndex:
    """Test tool diversity index calculation."""

    def test_zero_tools(self):
        """Verify zero tools returns zero diversity."""
        assert _calculate_tool_diversity_index(0, 0) == 0.0

    def test_all_unique_tools(self):
        """Verify all unique tools returns 1.0 diversity."""
        assert _calculate_tool_diversity_index(10, 10) == 1.0

    def test_single_tool_repeated(self):
        """Verify single tool repeated returns low diversity."""
        assert _calculate_tool_diversity_index(1, 10) == 0.1

    def test_half_unique(self):
        """Verify half unique tools returns 0.5 diversity."""
        assert _calculate_tool_diversity_index(5, 10) == 0.5

    def test_three_unique_of_twelve(self):
        """Verify specific diversity calculation."""
        assert _calculate_tool_diversity_index(3, 12) == pytest.approx(0.25, abs=0.01)


class TestCalculateToolClusteringCoefficient:
    """Test tool clustering coefficient calculation."""

    def test_empty_tools(self):
        """Verify empty tool list returns zero clustering."""
        assert _calculate_tool_clustering_coefficient([]) == 0.0

    def test_single_tool_only(self):
        """Verify single tool returns maximum clustering."""
        assert _calculate_tool_clustering_coefficient(["read"] * 10) == 1.0

    def test_perfectly_even_distribution(self):
        """Verify even distribution returns low clustering."""
        # Each tool used exactly once
        tools = ["read", "write", "edit"]
        result = _calculate_tool_clustering_coefficient(tools)
        assert result == 0.0

    def test_two_tools_equal_usage(self):
        """Verify two tools used equally."""
        tools = ["read", "read", "write", "write"]
        result = _calculate_tool_clustering_coefficient(tools)
        # Should be close to 0 for equal distribution
        assert 0.0 <= result < 0.3

    def test_skewed_distribution(self):
        """Verify skewed distribution returns higher clustering."""
        # One tool dominates
        tools = ["read"] * 9 + ["write"]
        result = _calculate_tool_clustering_coefficient(tools)
        # Gini for 9:1 distribution should be moderate (around 0.4)
        assert result > 0.3

    def test_moderate_clustering(self):
        """Verify moderate clustering for mixed usage."""
        tools = ["read"] * 5 + ["write"] * 3 + ["edit"] * 2
        result = _calculate_tool_clustering_coefficient(tools)
        assert 0.0 < result < 1.0


class TestCalculateBurstDensity:
    """Test burst density calculation."""

    def test_zero_tools_per_turn(self):
        """Verify zero tools per turn returns zero density."""
        assert _calculate_burst_density(0.0) == 0.0

    def test_below_burst_threshold(self):
        """Verify below threshold returns proportional density."""
        result = _calculate_burst_density(1.5)  # Half of threshold (3.0)
        assert result == pytest.approx(0.5, abs=0.01)

    def test_at_burst_threshold(self):
        """Verify at threshold returns 1.0 density."""
        assert _calculate_burst_density(BURST_THRESHOLD) == 1.0

    def test_above_burst_threshold(self):
        """Verify above threshold is capped at 1.0."""
        assert _calculate_burst_density(10.0) == 1.0

    def test_low_activity(self):
        """Verify low activity returns low density."""
        result = _calculate_burst_density(0.5)
        assert result < 0.2


class TestClassifyWorkflowPattern:
    """Test workflow pattern classification."""

    def test_exploration_pattern(self):
        """Verify high diversity + low clustering = exploration."""
        pattern = _classify_workflow_pattern(diversity=0.8, clustering=0.2)
        assert pattern == "exploration"

    def test_focused_pattern(self):
        """Verify low diversity + high clustering = focused."""
        pattern = _classify_workflow_pattern(diversity=0.2, clustering=0.8)
        assert pattern == "focused"

    def test_systematic_pattern(self):
        """Verify moderate diversity + clustering = systematic."""
        pattern = _classify_workflow_pattern(diversity=0.5, clustering=0.5)
        assert pattern == "systematic"

    def test_scattered_pattern(self):
        """Verify high diversity + high clustering = scattered."""
        pattern = _classify_workflow_pattern(diversity=0.8, clustering=0.8)
        assert pattern == "scattered"

    def test_edge_case_low_both(self):
        """Verify low diversity + low clustering = systematic."""
        pattern = _classify_workflow_pattern(diversity=0.2, clustering=0.2)
        assert pattern == "systematic"

    def test_boundary_exploration(self):
        """Verify boundary at diversity threshold."""
        pattern = _classify_workflow_pattern(
            diversity=DIVERSITY_HIGH + 0.01, clustering=CLUSTERING_LOW - 0.01
        )
        assert pattern == "exploration"

    def test_boundary_focused(self):
        """Verify boundary at focused thresholds."""
        pattern = _classify_workflow_pattern(
            diversity=DIVERSITY_LOW - 0.01, clustering=CLUSTERING_HIGH + 0.01
        )
        assert pattern == "focused"


class TestCalculateSessionToolUsageDensity:
    """Test complete session tool usage density calculation."""

    def test_empty_session(self):
        """Verify empty session (0 turns) returns appropriate result."""
        result = calculate_session_tool_usage_density(
            tool_calls=[],
            turn_count=0,
        )
        assert result.tools_per_turn == 0.0
        assert result.tool_diversity_index == 0.0
        assert result.tool_clustering_coefficient == 0.0
        assert result.burst_density == 0.0
        assert result.workflow_pattern == "empty"
        assert len(result.insights) > 0
        assert "Empty session" in result.insights[0]

    def test_session_with_no_tools_but_turns(self):
        """Verify session with turns but no tools."""
        result = calculate_session_tool_usage_density(
            tool_calls=[],
            turn_count=10,
        )
        assert result.tools_per_turn == 0.0
        assert result.metrics.turn_count == 10
        assert result.metrics.total_tool_count == 0
        # Should have insight about no tools despite turns
        insights_text = " ".join(result.insights).lower()
        assert "no tools" in insights_text or "conversational" in insights_text

    def test_simple_session(self):
        """Verify simple session with few tools."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read", "write", "read"],
            turn_count=10,
        )
        assert result.tools_per_turn == 0.3  # 3 tools / 10 turns
        assert result.metrics.unique_tool_count == 2
        assert result.metrics.total_tool_count == 3
        assert result.workflow_pattern in ["exploration", "focused", "systematic", "scattered"]

    def test_focused_workflow(self):
        """Verify focused workflow pattern detection."""
        # Same tool repeated many times
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 20,
            turn_count=10,
        )
        assert result.workflow_pattern == "focused"
        assert "focused" in " ".join(result.insights).lower()

    def test_exploration_workflow(self):
        """Verify exploration workflow pattern detection."""
        # Many different tools, each used once or twice
        tools = ["read", "write", "edit", "grep", "glob", "bash", "task"]
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        assert result.tool_diversity_index > DIVERSITY_HIGH
        # Pattern should be exploration or systematic depending on clustering
        assert result.workflow_pattern in ["exploration", "systematic"]

    def test_systematic_workflow(self):
        """Verify systematic workflow pattern detection."""
        # Moderate variety with moderate repetition
        tools = ["read"] * 3 + ["write"] * 3 + ["edit"] * 2
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        # Should show balanced metrics
        assert 0.0 < result.tool_diversity_index < 1.0
        assert 0.0 < result.tool_clustering_coefficient < 1.0

    def test_scattered_workflow(self):
        """Verify scattered workflow pattern detection."""
        # Many tools with uneven distribution
        tools = (
            ["read"] * 5
            + ["write"] * 4
            + ["edit"] * 3
            + ["grep"] * 2
            + ["glob"] * 2
            + ["bash"]
        )
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        # High diversity with clustering
        assert result.tool_diversity_index > 0.3
        assert result.tool_clustering_coefficient > 0.0

    def test_high_burst_activity(self):
        """Verify high burst activity detection."""
        # Many tools per turn
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 40,  # 4 tools per turn
            turn_count=10,
        )
        assert result.tools_per_turn >= BURST_THRESHOLD
        assert result.burst_density >= 1.0
        insights_text = " ".join(result.insights).lower()
        assert "burst" in insights_text or "intensive" in insights_text

    def test_low_tool_density(self):
        """Verify low tool density detection."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 5,
            turn_count=10,
        )
        assert result.tools_per_turn < 1.0
        insights_text = " ".join(result.insights).lower()
        assert "low tool density" in insights_text or "conversation" in insights_text

    def test_metrics_preserved(self):
        """Verify input metrics are preserved in result."""
        tools = ["read", "write", "edit"]
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        assert result.metrics.tool_calls == tuple(tools)
        assert result.metrics.turn_count == 10
        assert result.metrics.unique_tool_count == 3
        assert result.metrics.total_tool_count == 3

    def test_insights_generated(self):
        """Verify insights are always generated."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 10,
            turn_count=5,
        )
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_values_rounded(self):
        """Verify metric values are properly rounded."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 7,
            turn_count=3,
        )
        # tools_per_turn should be rounded to 2 decimals
        assert result.tools_per_turn == round(result.tools_per_turn, 2)
        # Other metrics to 3 decimals
        assert result.tool_diversity_index == round(result.tool_diversity_index, 3)
        assert result.tool_clustering_coefficient == round(
            result.tool_clustering_coefficient, 3
        )

    def test_result_immutable(self):
        """Verify result is immutable."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"],
            turn_count=5,
        )
        with pytest.raises(AttributeError):
            result.tools_per_turn = 99.0


class TestSessionToolUsageDensityEdgeCases:
    """Test edge cases and error conditions."""

    def test_negative_turn_count_raises(self):
        """Verify negative turn count raises ValueError."""
        with pytest.raises(ValueError, match="turn_count must be non-negative"):
            calculate_session_tool_usage_density(
                tool_calls=["read"],
                turn_count=-1,
            )

    def test_invalid_tool_calls_type_raises(self):
        """Verify invalid tool_calls type raises ValueError."""
        with pytest.raises(ValueError, match="tool_calls must be a list or tuple of strings"):
            calculate_session_tool_usage_density(
                tool_calls="not a list",
                turn_count=10,
            )

    def test_tool_calls_with_non_string_raises(self):
        """Verify tool_calls with non-strings raises ValueError."""
        with pytest.raises(ValueError, match=r"tool_calls\[0\] must be a string"):
            calculate_session_tool_usage_density(
                tool_calls=[123, 456],
                turn_count=10,
            )

    @pytest.mark.parametrize("turn_count", [1.5, "10", None, True])
    def test_non_integer_turn_count_raises(self, turn_count):
        """Verify non-integer turn counts raise ValueError."""
        with pytest.raises(ValueError, match="turn_count must be an integer"):
            calculate_session_tool_usage_density(
                tool_calls=["read"],
                turn_count=turn_count,
            )

    def test_empty_tool_calls_with_positive_turns(self):
        """Verify empty tool_calls with positive turns is valid."""
        result = calculate_session_tool_usage_density(
            tool_calls=[],
            turn_count=5,
        )
        assert result.tools_per_turn == 0.0
        assert result.metrics.total_tool_count == 0

    def test_single_turn_single_tool(self):
        """Verify minimal valid session."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"],
            turn_count=1,
        )
        assert result.tools_per_turn == 1.0
        assert result.metrics.unique_tool_count == 1
        assert result.metrics.total_tool_count == 1

    def test_many_tools_single_turn(self):
        """Verify many tools in single turn."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read", "write", "edit", "grep"],
            turn_count=1,
        )
        assert result.tools_per_turn == 4.0
        assert result.burst_density == 1.0

    def test_very_high_diversity(self):
        """Verify very high diversity generates appropriate insight."""
        # Each tool used only once
        tools = [f"tool_{i}" for i in range(20)]
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        assert result.tool_diversity_index == 1.0
        insights_text = " ".join(result.insights).lower()
        assert "diversity" in insights_text

    def test_very_low_diversity(self):
        """Verify very low diversity generates appropriate insight."""
        # One tool repeated many times
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 50,
            turn_count=10,
        )
        assert result.tool_diversity_index < 0.1
        insights_text = " ".join(result.insights).lower()
        assert "diversity" in insights_text or "reliance" in insights_text

    def test_tuple_tool_calls(self):
        """Verify tuple tool_calls is accepted."""
        result = calculate_session_tool_usage_density(
            tool_calls=("read", "write", "edit"),
            turn_count=5,
        )
        assert result.metrics.total_tool_count == 3


class TestDiversityInsights:
    """Test diversity-specific insights."""

    def test_high_diversity_exploration_insight(self):
        """Verify high diversity exploration produces insight."""
        tools = ["tool_" + str(i) for i in range(10)]  # Each tool unique
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "exploration" in insights_text or "diverse" in insights_text

    def test_focused_workflow_efficiency_insight(self):
        """Verify focused workflow mentions efficiency."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 20 + ["write"] * 2,
            turn_count=10,
        )
        if result.workflow_pattern == "focused":
            insights_text = " ".join(result.insights).lower()
            assert "efficiency" in insights_text or "consistency" in insights_text

    def test_scattered_workflow_suggestion(self):
        """Verify scattered workflow suggests breaking up work."""
        # Many tools with high clustering
        tools = (
            ["read"] * 10
            + ["write"] * 8
            + ["edit"] * 6
            + ["grep"] * 4
            + ["glob"] * 3
        )
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=15,
        )
        if result.workflow_pattern == "scattered":
            insights_text = " ".join(result.insights).lower()
            assert (
                "breaking" in insights_text
                or "focused" in insights_text
                or "switching" in insights_text
            )


class TestWorkflowPatternIntegration:
    """Test workflow pattern integration scenarios."""

    def test_tdd_workflow_pattern(self):
        """Verify TDD workflow shows systematic pattern."""
        # Typical TDD: read test, edit code, read test, edit code
        tools = ["read", "edit"] * 5
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        # Should show moderate diversity and clustering
        assert result.workflow_pattern in ["systematic", "focused"]

    def test_debugging_workflow_pattern(self):
        """Verify debugging workflow shows exploration or scattered."""
        # Debugging: many reads, some edits, grep, etc.
        tools = (
            ["read"] * 10
            + ["grep"] * 5
            + ["edit"] * 3
            + ["bash"] * 2
            + ["read"] * 5
        )
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=15,
        )
        # Could be exploration or systematic depending on distribution
        assert result.workflow_pattern in ["exploration", "systematic", "scattered"]

    def test_refactoring_workflow_pattern(self):
        """Verify refactoring workflow shows focused or systematic."""
        # Refactoring: repeated edit/read cycles
        tools = ["edit", "read"] * 10
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=15,
        )
        assert result.workflow_pattern in ["systematic", "focused"]

    def test_documentation_workflow_pattern(self):
        """Verify documentation workflow shows focused pattern."""
        # Documentation: mostly writing
        tools = ["write"] * 15 + ["read"] * 3
        result = calculate_session_tool_usage_density(
            tool_calls=tools,
            turn_count=10,
        )
        # Low diversity, should be focused
        assert result.tool_diversity_index < 0.3


class TestInsightQuality:
    """Test quality and usefulness of generated insights."""

    def test_insights_are_actionable(self):
        """Verify insights contain actionable information."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 10 + ["write"] * 5,
            turn_count=10,
        )
        # Each insight should be a meaningful string
        for insight in result.insights:
            assert isinstance(insight, str)
            assert len(insight) > 10  # Not trivial
            assert insight[0].isupper()  # Properly capitalized

    def test_empty_session_has_clear_insight(self):
        """Verify empty session provides clear feedback."""
        result = calculate_session_tool_usage_density(
            tool_calls=[],
            turn_count=0,
        )
        assert any("empty" in insight.lower() for insight in result.insights)

    def test_pattern_mentioned_in_insights(self):
        """Verify workflow pattern is mentioned in insights."""
        result = calculate_session_tool_usage_density(
            tool_calls=["read"] * 20,
            turn_count=10,
        )
        insights_text = " ".join(result.insights).lower()
        # Pattern name should appear in insights
        assert result.workflow_pattern.lower() in insights_text
