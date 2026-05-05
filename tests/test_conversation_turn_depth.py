"""Tests for conversation turn depth analysis."""

import pytest

from synthesis.conversation_turn_depth import (
    ConversationTurn,
    TurnDepthStats,
    ContextSwitch,
    DeepEngagementPeriod,
    analyze_conversation_turn_depth,
    _calculate_turn_depth_stats,
    _build_nesting_histogram,
    _detect_context_switches,
    _identify_deep_engagement_periods,
    DEPTH_SHALLOW,
    DEPTH_MODERATE,
    DEPTH_DEEP,
    DEEP_ENGAGEMENT_THRESHOLD,
    MIN_DEEP_ENGAGEMENT_TURNS,
)


class TestConversationTurn:
    """Test ConversationTurn dataclass."""

    def test_create_turn_with_parent(self):
        """Verify turn can be created with parent reference."""
        turn = ConversationTurn(
            turn_number=5,
            depth_level=2,
            parent_turn=3,
        )
        assert turn.turn_number == 5
        assert turn.depth_level == 2
        assert turn.parent_turn == 3

    def test_create_turn_without_parent(self):
        """Verify turn can be created without parent (root level)."""
        turn = ConversationTurn(
            turn_number=0,
            depth_level=0,
            parent_turn=None,
        )
        assert turn.parent_turn is None
        assert turn.depth_level == 0

    def test_turn_frozen(self):
        """Verify turn is immutable."""
        turn = ConversationTurn(
            turn_number=1,
            depth_level=0,
            parent_turn=None,
        )
        with pytest.raises(AttributeError):
            turn.turn_number = 2


class TestCalculateTurnDepthStats:
    """Test turn depth statistics calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero stats."""
        stats = _calculate_turn_depth_stats([])
        assert stats.avg_depth == 0.0
        assert stats.max_depth == 0
        assert stats.min_depth == 0
        assert stats.depth_variance == 0.0

    def test_single_turn_depth_zero(self):
        """Verify single shallow turn statistics."""
        turns = [ConversationTurn(0, 0, None)]
        stats = _calculate_turn_depth_stats(turns)
        assert stats.avg_depth == 0.0
        assert stats.max_depth == 0
        assert stats.min_depth == 0
        assert stats.depth_variance == 0.0

    def test_uniform_depth(self):
        """Verify uniform depth has zero variance."""
        turns = [
            ConversationTurn(0, 2, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 2, 1),
        ]
        stats = _calculate_turn_depth_stats(turns)
        assert stats.avg_depth == 2.0
        assert stats.max_depth == 2
        assert stats.min_depth == 2
        assert stats.depth_variance == 0.0

    def test_varying_depths(self):
        """Verify varying depths calculation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 3, 1),
            ConversationTurn(3, 2, 2),
        ]
        stats = _calculate_turn_depth_stats(turns)
        # Average: (0 + 1 + 3 + 2) / 4 = 1.5
        assert stats.avg_depth == 1.5
        assert stats.max_depth == 3
        assert stats.min_depth == 0
        # Variance: ((0-1.5)^2 + (1-1.5)^2 + (3-1.5)^2 + (2-1.5)^2) / 4
        # = (2.25 + 0.25 + 2.25 + 0.25) / 4 = 5 / 4 = 1.25
        assert stats.depth_variance == 1.25

    def test_deep_nesting(self):
        """Verify calculation for deeply nested conversation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 3, 2),
            ConversationTurn(4, 4, 3),
            ConversationTurn(5, 5, 4),
        ]
        stats = _calculate_turn_depth_stats(turns)
        assert stats.avg_depth == 2.5
        assert stats.max_depth == 5
        assert stats.min_depth == 0


class TestBuildNestingHistogram:
    """Test nesting histogram construction."""

    def test_empty_turns(self):
        """Verify empty turns returns empty histogram."""
        histogram = _build_nesting_histogram([])
        assert histogram == {}

    def test_single_depth_level(self):
        """Verify histogram for single depth level."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 0, None),
            ConversationTurn(2, 0, None),
        ]
        histogram = _build_nesting_histogram(turns)
        assert histogram == {0: 3}

    def test_multiple_depth_levels(self):
        """Verify histogram for multiple depth levels."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 1, 0),
            ConversationTurn(3, 2, 1),
            ConversationTurn(4, 2, 1),
            ConversationTurn(5, 2, 1),
            ConversationTurn(6, 3, 2),
        ]
        histogram = _build_nesting_histogram(turns)
        assert histogram == {0: 1, 1: 2, 2: 3, 3: 1}

    def test_sparse_depth_levels(self):
        """Verify histogram with gaps in depth levels."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 5, 1),
        ]
        histogram = _build_nesting_histogram(turns)
        assert histogram == {0: 1, 2: 1, 5: 1}


class TestDetectContextSwitches:
    """Test context switch detection."""

    def test_empty_turns(self):
        """Verify empty turns returns no switches."""
        switches = _detect_context_switches([])
        assert switches == []

    def test_single_turn(self):
        """Verify single turn returns no switches."""
        turns = [ConversationTurn(0, 0, None)]
        switches = _detect_context_switches(turns)
        assert switches == []

    def test_no_depth_changes(self):
        """Verify no switches when depth is constant."""
        turns = [
            ConversationTurn(0, 2, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 2, 1),
        ]
        switches = _detect_context_switches(turns)
        assert switches == []

    def test_single_depth_increase(self):
        """Verify detection of depth increase."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
        ]
        switches = _detect_context_switches(turns)
        assert len(switches) == 1
        assert switches[0].from_turn == 0
        assert switches[0].to_turn == 1
        assert switches[0].from_depth == 0
        assert switches[0].to_depth == 1
        assert switches[0].depth_delta == 1

    def test_single_depth_decrease(self):
        """Verify detection of depth decrease."""
        turns = [
            ConversationTurn(0, 3, None),
            ConversationTurn(1, 1, 0),
        ]
        switches = _detect_context_switches(turns)
        assert len(switches) == 1
        assert switches[0].depth_delta == -2

    def test_multiple_switches(self):
        """Verify detection of multiple switches."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),  # Switch: 0 -> 1
            ConversationTurn(2, 2, 1),  # Switch: 1 -> 2
            ConversationTurn(3, 2, 1),  # No switch
            ConversationTurn(4, 0, None),  # Switch: 2 -> 0
        ]
        switches = _detect_context_switches(turns)
        assert len(switches) == 3
        assert switches[0].depth_delta == 1
        assert switches[1].depth_delta == 1
        assert switches[2].depth_delta == -2

    def test_alternating_depths(self):
        """Verify detection with alternating depths."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 0, None),
            ConversationTurn(3, 2, 2),
            ConversationTurn(4, 0, None),
        ]
        switches = _detect_context_switches(turns)
        assert len(switches) == 4


class TestIdentifyDeepEngagementPeriods:
    """Test deep engagement period identification."""

    def test_empty_turns(self):
        """Verify empty turns returns no periods."""
        periods = _identify_deep_engagement_periods([])
        assert periods == []

    def test_all_shallow_turns(self):
        """Verify no periods when all turns are shallow."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 0, None),
            ConversationTurn(3, 1, 2),
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert periods == []

    def test_short_deep_period(self):
        """Verify short deep period is ignored (< MIN_DEEP_ENGAGEMENT_TURNS)."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),  # Deep but only 1 turn
            ConversationTurn(2, 0, None),
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert periods == []

    def test_single_deep_period_minimum_length(self):
        """Verify deep period at minimum length is detected."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 2, 2),  # 3 turns at depth >= 2
            ConversationTurn(4, 0, None),
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert len(periods) == 1
        assert periods[0].start_turn == 1
        assert periods[0].end_turn == 3
        assert periods[0].duration_turns == 3
        assert periods[0].avg_depth == 2.0
        assert periods[0].max_depth == 2

    def test_single_deep_period_longer(self):
        """Verify longer deep period is detected."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 3, 1),
            ConversationTurn(3, 4, 2),
            ConversationTurn(4, 3, 3),
            ConversationTurn(5, 2, 4),  # 5 turns at depth >= 2
            ConversationTurn(6, 0, None),
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert len(periods) == 1
        assert periods[0].duration_turns == 5
        assert periods[0].avg_depth == 2.8  # (2+3+4+3+2)/5
        assert periods[0].max_depth == 4

    def test_multiple_deep_periods(self):
        """Verify multiple deep periods are detected."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 2, 2),  # First period: turns 1-3
            ConversationTurn(4, 0, None),
            ConversationTurn(5, 1, 4),
            ConversationTurn(6, 3, 5),
            ConversationTurn(7, 3, 6),
            ConversationTurn(8, 3, 7),  # Second period: turns 6-8
            ConversationTurn(9, 0, None),
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert len(periods) == 2
        assert periods[0].start_turn == 1
        assert periods[0].duration_turns == 3
        assert periods[1].start_turn == 6
        assert periods[1].duration_turns == 3

    def test_deep_period_to_end_of_conversation(self):
        """Verify deep period extending to end of conversation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 3, 1),
            ConversationTurn(3, 2, 2),  # Period extends to end
        ]
        periods = _identify_deep_engagement_periods(turns)
        assert len(periods) == 1
        assert periods[0].end_turn == 3


class TestAnalyzeConversationTurnDepth:
    """Test complete conversation turn depth analysis."""

    def test_empty_conversation(self):
        """Verify analysis of empty conversation."""
        result = analyze_conversation_turn_depth([])
        assert result["turn_depth_stats"]["avg_depth"] == 0.0
        assert result["turn_depth_stats"]["max_depth"] == 0
        assert result["nesting_histogram"] == {}
        assert result["context_switches"] == []
        assert result["deep_engagement_duration"] == []

    def test_single_turn_conversation(self):
        """Verify analysis of single-turn conversation."""
        turns = [ConversationTurn(0, 0, None)]
        result = analyze_conversation_turn_depth(turns)
        assert result["turn_depth_stats"]["avg_depth"] == 0.0
        assert result["turn_depth_stats"]["max_depth"] == 0
        assert result["turn_depth_stats"]["min_depth"] == 0
        assert result["nesting_histogram"] == {0: 1}
        assert result["context_switches"] == []
        assert result["deep_engagement_duration"] == []

    def test_shallow_conversation(self):
        """Verify analysis of shallow conversation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 0, None),
            ConversationTurn(3, 1, 2),
        ]
        result = analyze_conversation_turn_depth(turns)
        assert result["turn_depth_stats"]["avg_depth"] == 0.5
        assert result["turn_depth_stats"]["max_depth"] == 1
        assert result["nesting_histogram"] == {0: 2, 1: 2}
        assert len(result["context_switches"]) == 3
        assert result["deep_engagement_duration"] == []

    def test_deep_nested_conversation(self):
        """Verify analysis of deep nested conversation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 3, 2),
            ConversationTurn(4, 4, 3),
            ConversationTurn(5, 5, 4),
        ]
        result = analyze_conversation_turn_depth(turns)
        assert result["turn_depth_stats"]["avg_depth"] == 2.5
        assert result["turn_depth_stats"]["max_depth"] == 5
        assert len(result["context_switches"]) == 5  # Each depth increases
        assert len(result["deep_engagement_duration"]) == 1  # Sustained deep from turn 2-5

    def test_complex_conversation_with_multiple_periods(self):
        """Verify analysis of complex conversation."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 1, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 3, 2),
            ConversationTurn(4, 2, 3),  # First deep period: turns 2-4
            ConversationTurn(5, 0, None),
            ConversationTurn(6, 1, 5),
            ConversationTurn(7, 0, None),
            ConversationTurn(8, 2, 7),
            ConversationTurn(9, 3, 8),
            ConversationTurn(10, 2, 9),  # Second deep period: turns 8-10
        ]
        result = analyze_conversation_turn_depth(turns)
        assert result["turn_depth_stats"]["avg_depth"] == 1.45  # Rounded
        assert result["turn_depth_stats"]["max_depth"] == 3
        assert len(result["deep_engagement_duration"]) == 2

    def test_invalid_turns_not_sequence(self):
        """Verify error on non-sequence input."""
        with pytest.raises(ValueError, match="turns must be a sequence"):
            analyze_conversation_turn_depth("not a sequence")

    def test_invalid_turns_wrong_type(self):
        """Verify error on wrong element type."""
        with pytest.raises(ValueError, match="must contain ConversationTurn instances"):
            analyze_conversation_turn_depth([{"turn": 0}])

    def test_invalid_negative_turn_number(self):
        """Verify error on negative turn number."""
        turns = [ConversationTurn(-1, 0, None)]
        with pytest.raises(ValueError, match="turn_number must be non-negative"):
            analyze_conversation_turn_depth(turns)

    def test_invalid_negative_depth(self):
        """Verify error on negative depth level."""
        turns = [ConversationTurn(0, -1, None)]
        with pytest.raises(ValueError, match="depth_level must be non-negative"):
            analyze_conversation_turn_depth(turns)

    def test_invalid_negative_parent(self):
        """Verify error on negative parent turn."""
        turns = [ConversationTurn(1, 1, -1)]
        with pytest.raises(ValueError, match="parent_turn must be non-negative or None"):
            analyze_conversation_turn_depth(turns)

    def test_invalid_parent_after_current(self):
        """Verify error when parent_turn >= turn_number."""
        turns = [ConversationTurn(1, 1, 1)]
        with pytest.raises(ValueError, match="parent_turn must be less than turn_number"):
            analyze_conversation_turn_depth(turns)

    def test_result_structure(self):
        """Verify result structure contains all required fields."""
        turns = [
            ConversationTurn(0, 0, None),
            ConversationTurn(1, 2, 0),
            ConversationTurn(2, 2, 1),
            ConversationTurn(3, 2, 2),
        ]
        result = analyze_conversation_turn_depth(turns)

        # Check top-level keys
        assert "turn_depth_stats" in result
        assert "nesting_histogram" in result
        assert "context_switches" in result
        assert "deep_engagement_duration" in result

        # Check turn_depth_stats structure
        stats = result["turn_depth_stats"]
        assert "avg_depth" in stats
        assert "max_depth" in stats
        assert "min_depth" in stats
        assert "depth_variance" in stats

        # Check context_switches structure
        if result["context_switches"]:
            switch = result["context_switches"][0]
            assert "from_turn" in switch
            assert "to_turn" in switch
            assert "from_depth" in switch
            assert "to_depth" in switch
            assert "depth_delta" in switch

        # Check deep_engagement_duration structure
        if result["deep_engagement_duration"]:
            period = result["deep_engagement_duration"][0]
            assert "start_turn" in period
            assert "end_turn" in period
            assert "duration_turns" in period
            assert "avg_depth" in period
            assert "max_depth" in period
