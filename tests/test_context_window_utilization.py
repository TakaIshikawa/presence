"""Tests for context window utilization tracking."""

import pytest

from engagement.context_window_utilization import (
    ConversationTurn,
    ContextWindowMetrics,
    ContextWindowUtilization,
    analyze_context_window_utilization,
    _calculate_avg_utilization,
    _calculate_peak_utilization,
    _calculate_current_utilization,
    _detect_pruning_events,
    _detect_summarization_events,
    _classify_utilization_tier,
    DEFAULT_MAX_CONTEXT_TOKENS,
    TIER_LOW,
    TIER_MODERATE,
    TIER_HIGH,
    TIER_CRITICAL,
    THRESHOLD_MODERATE,
    THRESHOLD_HIGH,
    THRESHOLD_CRITICAL,
    PRUNING_THRESHOLD,
    SUMMARIZATION_THRESHOLD,
)


class TestConversationTurn:
    """Test ConversationTurn dataclass."""

    def test_create_turn_basic(self):
        """Verify turn can be created with basic fields."""
        turn = ConversationTurn(turn_number=5, context_tokens=1000)
        assert turn.turn_number == 5
        assert turn.context_tokens == 1000
        assert turn.added_tokens is None

    def test_create_turn_with_added_tokens(self):
        """Verify turn can be created with added_tokens."""
        turn = ConversationTurn(
            turn_number=10,
            context_tokens=5000,
            added_tokens=500,
        )
        assert turn.turn_number == 10
        assert turn.context_tokens == 5000
        assert turn.added_tokens == 500

    def test_turn_frozen(self):
        """Verify turn is immutable."""
        turn = ConversationTurn(turn_number=1, context_tokens=100)
        with pytest.raises(AttributeError):
            turn.turn_number = 2


class TestCalculateAvgUtilization:
    """Test average utilization calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_avg_utilization([], 200000) == 0.0

    def test_single_turn(self):
        """Verify single turn calculation."""
        turns = [ConversationTurn(0, 100000)]
        result = _calculate_avg_utilization(turns, 200000)
        assert result == 50.0  # 100k/200k = 50%

    def test_multiple_turns(self):
        """Verify average across multiple turns."""
        turns = [
            ConversationTurn(0, 40000),  # 20%
            ConversationTurn(1, 80000),  # 40%
            ConversationTurn(2, 120000),  # 60%
        ]
        result = _calculate_avg_utilization(turns, 200000)
        assert result == 40.0  # (20 + 40 + 60) / 3 = 40%

    def test_full_utilization(self):
        """Verify 100% utilization calculation."""
        turns = [ConversationTurn(0, 200000)]
        result = _calculate_avg_utilization(turns, 200000)
        assert result == 100.0

    def test_zero_tokens(self):
        """Verify handling of zero tokens."""
        turns = [ConversationTurn(0, 0)]
        result = _calculate_avg_utilization(turns, 200000)
        assert result == 0.0


class TestCalculatePeakUtilization:
    """Test peak utilization calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_peak_utilization([], 200000) == 0.0

    def test_single_turn(self):
        """Verify single turn is peak."""
        turns = [ConversationTurn(0, 150000)]
        result = _calculate_peak_utilization(turns, 200000)
        assert result == 75.0

    def test_peak_in_middle(self):
        """Verify peak detection in middle of session."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 180000),  # Peak
            ConversationTurn(2, 100000),
        ]
        result = _calculate_peak_utilization(turns, 200000)
        assert result == 90.0

    def test_peak_at_start(self):
        """Verify peak at start of session."""
        turns = [
            ConversationTurn(0, 190000),  # Peak
            ConversationTurn(1, 100000),
            ConversationTurn(2, 50000),
        ]
        result = _calculate_peak_utilization(turns, 200000)
        assert result == 95.0

    def test_peak_at_end(self):
        """Verify peak at end of session."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 100000),
            ConversationTurn(2, 199000),  # Peak
        ]
        result = _calculate_peak_utilization(turns, 200000)
        assert result == 99.5


class TestCalculateCurrentUtilization:
    """Test current utilization calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_current_utilization([], 200000) == 0.0

    def test_single_turn(self):
        """Verify single turn is current."""
        turns = [ConversationTurn(0, 80000)]
        result = _calculate_current_utilization(turns, 200000)
        assert result == 40.0

    def test_last_turn_is_current(self):
        """Verify last turn is used for current."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 100000),
            ConversationTurn(2, 150000),  # Current
        ]
        result = _calculate_current_utilization(turns, 200000)
        assert result == 75.0


class TestDetectPruningEvents:
    """Test pruning event detection."""

    def test_empty_turns(self):
        """Verify empty turns returns zero events."""
        assert _detect_pruning_events([], 200000) == 0

    def test_single_turn(self):
        """Verify single turn has no events."""
        turns = [ConversationTurn(0, 190000)]
        assert _detect_pruning_events(turns, 200000) == 0

    def test_no_pruning_low_utilization(self):
        """Verify no pruning when utilization is low."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 60000),
            ConversationTurn(2, 40000),  # Drop but not pruning
        ]
        assert _detect_pruning_events(turns, 200000) == 0

    def test_pruning_detected(self):
        """Verify pruning detection when approaching max then dropping."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, 100000),
            ConversationTurn(1, int(max_tokens * 0.96)),  # 96% - over threshold
            ConversationTurn(2, 100000),  # Sharp drop to 50%
        ]
        assert _detect_pruning_events(turns, max_tokens) == 1

    def test_multiple_pruning_events(self):
        """Verify detection of multiple pruning events."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, 100000),
            ConversationTurn(1, int(max_tokens * 0.96)),  # First peak
            ConversationTurn(2, 100000),  # First prune
            ConversationTurn(3, int(max_tokens * 0.97)),  # Second peak
            ConversationTurn(4, 80000),  # Second prune
        ]
        assert _detect_pruning_events(turns, max_tokens) == 2

    def test_gradual_decrease_not_pruning(self):
        """Verify gradual decrease doesn't trigger pruning."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.96)),
            ConversationTurn(1, int(max_tokens * 0.85)),  # Only 11% drop
        ]
        # 11% drop is less than 20% threshold
        assert _detect_pruning_events(turns, max_tokens) == 0

    def test_pruning_threshold_exact(self):
        """Verify pruning at exact threshold."""
        max_tokens = 200000
        threshold = int(max_tokens * PRUNING_THRESHOLD)
        turns = [
            ConversationTurn(0, threshold),  # Exactly at threshold
            ConversationTurn(1, int(threshold * 0.75)),  # 25% drop
        ]
        assert _detect_pruning_events(turns, max_tokens) == 1


class TestDetectSummarizationEvents:
    """Test summarization event detection."""

    def test_empty_turns(self):
        """Verify empty turns returns zero events."""
        assert _detect_summarization_events([], 200000) == 0

    def test_single_turn(self):
        """Verify single turn has no events."""
        turns = [ConversationTurn(0, 170000)]
        assert _detect_summarization_events(turns, 200000) == 0

    def test_no_summarization_low_utilization(self):
        """Verify no summarization when utilization is low."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 40000),
        ]
        assert _detect_summarization_events(turns, 200000) == 0

    def test_summarization_detected(self):
        """Verify summarization in high utilization zone."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, 100000),
            ConversationTurn(1, int(max_tokens * 0.85)),  # 85% - in zone
            ConversationTurn(2, int(max_tokens * 0.85 * 0.85)),  # 15% reduction
        ]
        assert _detect_summarization_events(turns, max_tokens) == 1

    def test_multiple_summarization_events(self):
        """Verify detection of multiple summarization events."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.82)),
            ConversationTurn(1, int(max_tokens * 0.82 * 0.85)),  # First summarization
            ConversationTurn(2, int(max_tokens * 0.83)),
            ConversationTurn(3, int(max_tokens * 0.83 * 0.85)),  # Second summarization
        ]
        assert _detect_summarization_events(turns, max_tokens) == 2

    def test_too_high_not_summarization(self):
        """Verify very high utilization triggers pruning, not summarization."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.96)),  # Too high for summarization
            ConversationTurn(1, int(max_tokens * 0.96 * 0.85)),
        ]
        assert _detect_summarization_events(turns, max_tokens) == 0

    def test_too_sharp_not_summarization(self):
        """Verify sharp drops aren't summarization."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.85)),
            ConversationTurn(1, int(max_tokens * 0.85 * 0.75)),  # 25% drop - too sharp
        ]
        assert _detect_summarization_events(turns, max_tokens) == 0

    def test_summarization_threshold_range(self):
        """Verify summarization only in 80-95% range."""
        max_tokens = 200000
        # Just at lower bound
        turns_low = [
            ConversationTurn(0, int(max_tokens * SUMMARIZATION_THRESHOLD)),
            ConversationTurn(1, int(max_tokens * SUMMARIZATION_THRESHOLD * 0.85)),
        ]
        assert _detect_summarization_events(turns_low, max_tokens) == 1

        # Just below upper bound
        turns_high = [
            ConversationTurn(0, int(max_tokens * 0.94)),
            ConversationTurn(1, int(max_tokens * 0.94 * 0.85)),
        ]
        assert _detect_summarization_events(turns_high, max_tokens) == 1


class TestClassifyUtilizationTier:
    """Test utilization tier classification."""

    def test_low_tier(self):
        """Verify low tier classification."""
        assert _classify_utilization_tier(0.0) == TIER_LOW
        assert _classify_utilization_tier(20.0) == TIER_LOW
        assert _classify_utilization_tier(39.9) == TIER_LOW

    def test_moderate_tier(self):
        """Verify moderate tier classification."""
        assert _classify_utilization_tier(40.0) == TIER_MODERATE
        assert _classify_utilization_tier(50.0) == TIER_MODERATE
        assert _classify_utilization_tier(69.9) == TIER_MODERATE

    def test_high_tier(self):
        """Verify high tier classification."""
        assert _classify_utilization_tier(70.0) == TIER_HIGH
        assert _classify_utilization_tier(80.0) == TIER_HIGH
        assert _classify_utilization_tier(89.9) == TIER_HIGH

    def test_critical_tier(self):
        """Verify critical tier classification."""
        assert _classify_utilization_tier(90.0) == TIER_CRITICAL
        assert _classify_utilization_tier(95.0) == TIER_CRITICAL
        assert _classify_utilization_tier(100.0) == TIER_CRITICAL


class TestAnalyzeContextWindowUtilization:
    """Test complete context window utilization analysis."""

    def test_empty_session(self):
        """Verify empty session returns valid result."""
        result = analyze_context_window_utilization([])
        assert result.metrics.total_turns == 0
        assert result.metrics.avg_utilization == 0.0
        assert result.utilization_tier == TIER_LOW
        assert "Empty session" in result.insights[0]

    def test_low_utilization_session(self):
        """Verify low utilization session."""
        turns = [
            ConversationTurn(i, 20000 + i * 1000)
            for i in range(10)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.utilization_tier == TIER_LOW
        assert result.metrics.total_turns == 10

    def test_moderate_utilization_session(self):
        """Verify moderate utilization session."""
        turns = [
            ConversationTurn(i, 100000 + i * 1000)
            for i in range(10)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.utilization_tier == TIER_MODERATE

    def test_high_utilization_session(self):
        """Verify high utilization session."""
        turns = [
            ConversationTurn(i, 150000 + i * 1000)
            for i in range(10)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.utilization_tier == TIER_HIGH

    def test_critical_utilization_session(self):
        """Verify critical utilization session."""
        turns = [
            ConversationTurn(i, 180000 + i * 100)
            for i in range(10)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.utilization_tier == TIER_CRITICAL

    def test_session_with_pruning(self):
        """Verify pruning event detection in full analysis."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, 100000),
            ConversationTurn(1, int(max_tokens * 0.96)),
            ConversationTurn(2, 100000),
        ]
        result = analyze_context_window_utilization(turns, max_tokens)
        assert result.metrics.pruning_events == 1
        assert "pruning" in " ".join(result.insights).lower()

    def test_session_with_summarization(self):
        """Verify summarization event detection in full analysis."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.85)),
            ConversationTurn(1, int(max_tokens * 0.85 * 0.85)),
        ]
        result = analyze_context_window_utilization(turns, max_tokens)
        assert result.metrics.summarization_events == 1
        assert "summarization" in " ".join(result.insights).lower()

    def test_metrics_preserved(self):
        """Verify metrics are preserved in result."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 100000),
            ConversationTurn(2, 150000),
        ]
        result = analyze_context_window_utilization(turns)
        assert isinstance(result.metrics, ContextWindowMetrics)
        assert result.metrics.total_turns == 3
        assert result.metrics.current_utilization > 0

    def test_insights_generated(self):
        """Verify insights are always generated."""
        turns = [ConversationTurn(0, 50000)]
        result = analyze_context_window_utilization(turns)
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        turns = [ConversationTurn(0, 100000)]
        result = analyze_context_window_utilization(turns)
        with pytest.raises(AttributeError):
            result.utilization_tier = TIER_CRITICAL

    def test_custom_max_tokens(self):
        """Verify custom max context tokens."""
        turns = [ConversationTurn(0, 50000)]
        result = analyze_context_window_utilization(turns, max_context_tokens=100000)
        assert result.max_context_tokens == 100000
        assert result.metrics.current_utilization == 50.0

    def test_metrics_values_rounded(self):
        """Verify metrics are rounded appropriately."""
        turns = [
            ConversationTurn(0, 33333),  # Creates fractional percentages
            ConversationTurn(1, 66666),
        ]
        result = analyze_context_window_utilization(turns)
        # Check rounding to 2 decimal places
        assert result.metrics.avg_utilization == round(result.metrics.avg_utilization, 2)
        assert result.metrics.peak_utilization == round(result.metrics.peak_utilization, 2)


class TestValidation:
    """Test input validation."""

    def test_invalid_turns_type_raises(self):
        """Verify invalid turns type raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_context_window_utilization("not a list")  # type: ignore

    def test_invalid_turn_instance_raises(self):
        """Verify invalid turn instance raises ValueError."""
        with pytest.raises(ValueError, match="ConversationTurn instances"):
            analyze_context_window_utilization([{"not": "a turn"}])  # type: ignore

    def test_negative_turn_number_raises(self):
        """Verify negative turn number raises ValueError."""
        turn = ConversationTurn(-1, 1000)
        with pytest.raises(ValueError, match="turn_number must be non-negative"):
            analyze_context_window_utilization([turn])

    def test_negative_context_tokens_raises(self):
        """Verify negative context tokens raises ValueError."""
        turn = ConversationTurn(0, -1000)
        with pytest.raises(ValueError, match="context_tokens must be non-negative"):
            analyze_context_window_utilization([turn])

    def test_negative_added_tokens_raises(self):
        """Verify negative added tokens raises ValueError."""
        turn = ConversationTurn(0, 1000, added_tokens=-100)
        with pytest.raises(ValueError, match="added_tokens must be non-negative"):
            analyze_context_window_utilization([turn])

    def test_zero_max_context_raises(self):
        """Verify zero max context raises ValueError."""
        turns = [ConversationTurn(0, 100)]
        with pytest.raises(ValueError, match="max_context_tokens must be positive"):
            analyze_context_window_utilization(turns, max_context_tokens=0)

    def test_negative_max_context_raises(self):
        """Verify negative max context raises ValueError."""
        turns = [ConversationTurn(0, 100)]
        with pytest.raises(ValueError, match="max_context_tokens must be positive"):
            analyze_context_window_utilization(turns, max_context_tokens=-1000)


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_critical_utilization_insight(self):
        """Verify critical utilization generates warning."""
        turns = [ConversationTurn(0, 185000)]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "critical" in insights_text or "pruning" in insights_text

    def test_high_utilization_insight(self):
        """Verify high utilization generates suggestion."""
        turns = [ConversationTurn(0, 160000)]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "high" in insights_text or "summarization" in insights_text

    def test_low_utilization_insight(self):
        """Verify low utilization generates positive feedback."""
        turns = [ConversationTurn(0, 30000)]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "low" in insights_text or "efficient" in insights_text

    def test_peak_utilization_insight(self):
        """Verify peak utilization mentioned in insights."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 190000),  # Peak
            ConversationTurn(2, 100000),
        ]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "peak" in insights_text

    def test_frequent_pruning_insight(self):
        """Verify frequent pruning generates special warning."""
        max_tokens = 200000
        turns = [
            ConversationTurn(0, int(max_tokens * 0.96)),
            ConversationTurn(1, 100000),
            ConversationTurn(2, int(max_tokens * 0.96)),
            ConversationTurn(3, 100000),
            ConversationTurn(4, int(max_tokens * 0.96)),
            ConversationTurn(5, 100000),
        ]
        result = analyze_context_window_utilization(turns, max_tokens)
        assert result.metrics.pruning_events >= 3
        insights_text = " ".join(result.insights).lower()
        assert "frequent" in insights_text or "shorter" in insights_text

    def test_long_session_no_pruning_insight(self):
        """Verify long efficient session gets recognition."""
        turns = [
            ConversationTurn(i, 80000 + i * 500)
            for i in range(51)
        ]
        result = analyze_context_window_utilization(turns, 200000)
        if result.metrics.pruning_events == 0:
            insights_text = " ".join(result.insights).lower()
            assert "long" in insights_text or "excellent" in insights_text

    def test_very_low_avg_utilization_insight(self):
        """Verify very low average utilization mentioned."""
        turns = [
            ConversationTurn(i, 10000 + i * 100)
            for i in range(15)
        ]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "low" in insights_text or "underutilized" in insights_text

    def test_high_avg_utilization_insight(self):
        """Verify high average utilization mentioned."""
        turns = [
            ConversationTurn(i, 150000 + i * 100)
            for i in range(10)
        ]
        result = analyze_context_window_utilization(turns, 200000)
        insights_text = " ".join(result.insights).lower()
        assert "high" in insights_text or "intensive" in insights_text


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_max_capacity_reached(self):
        """Verify handling when exactly at max capacity."""
        turns = [ConversationTurn(0, 200000)]
        result = analyze_context_window_utilization(turns, 200000)
        assert result.metrics.current_utilization == 100.0
        assert result.utilization_tier == TIER_CRITICAL

    def test_exceeding_capacity(self):
        """Verify handling when exceeding capacity."""
        turns = [ConversationTurn(0, 250000)]
        result = analyze_context_window_utilization(turns, 200000)
        assert result.metrics.current_utilization == 125.0
        assert result.utilization_tier == TIER_CRITICAL

    def test_all_zero_tokens(self):
        """Verify handling of all-zero token counts."""
        turns = [
            ConversationTurn(i, 0)
            for i in range(5)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.metrics.avg_utilization == 0.0
        assert result.metrics.peak_utilization == 0.0

    def test_single_turn_session(self):
        """Verify single turn produces valid analysis."""
        turns = [ConversationTurn(0, 100000)]
        result = analyze_context_window_utilization(turns)
        assert result.metrics.total_turns == 1
        assert result.metrics.pruning_events == 0
        assert result.metrics.summarization_events == 0

    def test_large_session(self):
        """Verify handling of very large session."""
        turns = [
            ConversationTurn(i, 50000 + (i * 1000) % 150000)
            for i in range(200)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.metrics.total_turns == 200
        assert isinstance(result, ContextWindowUtilization)

    def test_fluctuating_utilization(self):
        """Verify handling of highly variable utilization."""
        turns = [
            ConversationTurn(0, 50000),
            ConversationTurn(1, 150000),
            ConversationTurn(2, 30000),
            ConversationTurn(3, 180000),
            ConversationTurn(4, 20000),
        ]
        result = analyze_context_window_utilization(turns)
        assert result.metrics.peak_utilization > result.metrics.avg_utilization

    def test_stable_utilization(self):
        """Verify stable utilization analysis."""
        turns = [
            ConversationTurn(i, 100000)
            for i in range(20)
        ]
        result = analyze_context_window_utilization(turns)
        assert result.metrics.avg_utilization == result.metrics.current_utilization
        insights_text = " ".join(result.insights).lower()
        assert "stable" in insights_text or "no" in insights_text

    def test_default_max_context_tokens(self):
        """Verify default max context tokens used."""
        turns = [ConversationTurn(0, 100000)]
        result = analyze_context_window_utilization(turns)
        assert result.max_context_tokens == DEFAULT_MAX_CONTEXT_TOKENS


class TestContextWindowMetricsDataclass:
    """Test ContextWindowMetrics dataclass properties."""

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        metrics = ContextWindowMetrics(
            avg_utilization=50.0,
            peak_utilization=80.0,
            current_utilization=60.0,
            pruning_events=1,
            summarization_events=2,
            total_turns=10,
        )
        with pytest.raises(AttributeError):
            metrics.avg_utilization = 70.0


class TestContextWindowUtilizationDataclass:
    """Test ContextWindowUtilization dataclass properties."""

    def test_result_frozen(self):
        """Verify result is immutable."""
        metrics = ContextWindowMetrics(
            avg_utilization=50.0,
            peak_utilization=80.0,
            current_utilization=60.0,
            pruning_events=0,
            summarization_events=0,
            total_turns=5,
        )
        result = ContextWindowUtilization(
            metrics=metrics,
            utilization_tier=TIER_MODERATE,
            max_context_tokens=200000,
            insights=["Test insight"],
        )
        with pytest.raises(AttributeError):
            result.utilization_tier = TIER_HIGH
