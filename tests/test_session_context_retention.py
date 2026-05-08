"""Tests for session context retention analysis."""

import pytest

from synthesis.session_context_retention import (
    SessionTurn,
    ContextRetentionMetrics,
    SessionContextRetention,
    analyze_session_context_retention,
    _calculate_context_decay_rate,
    _calculate_avg_reference_chain_length,
    _calculate_topic_drift_score,
    _calculate_reactivation_rate,
    _calculate_coherence_score,
    _classify_quality_tier,
    TIER_COHERENT,
    TIER_MODERATE,
    TIER_FRAGMENTED,
    TIER_DISCONNECTED,
    THRESHOLD_MODERATE,
    THRESHOLD_FRAGMENTED,
    THRESHOLD_DISCONNECTED,
)


class TestSessionTurn:
    """Test SessionTurn dataclass."""

    def test_create_turn_with_reference(self):
        """Verify turn can be created with reference."""
        turn = SessionTurn(
            turn_number=5,
            references_turn=3,
            topic_keywords=["testing", "validation"],
        )
        assert turn.turn_number == 5
        assert turn.references_turn == 3
        assert turn.topic_keywords == ["testing", "validation"]

    def test_create_turn_without_reference(self):
        """Verify turn can be created without reference."""
        turn = SessionTurn(
            turn_number=1,
            references_turn=None,
            topic_keywords=["introduction"],
        )
        assert turn.references_turn is None

    def test_turn_frozen(self):
        """Verify turn is immutable."""
        turn = SessionTurn(
            turn_number=1,
            references_turn=None,
            topic_keywords=[],
        )
        with pytest.raises(AttributeError):
            turn.turn_number = 2


class TestCalculateContextDecayRate:
    """Test context decay rate calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_context_decay_rate([]) == 0.0

    def test_single_turn(self):
        """Verify single turn returns zero decay."""
        turns = [SessionTurn(0, None, [])]
        assert _calculate_context_decay_rate(turns) == 0.0

    def test_perfect_context_retention(self):
        """Verify no decay when all turns reference recent context."""
        # Each turn references the previous turn
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, 0, []),
            SessionTurn(2, 1, []),
            SessionTurn(3, 2, []),
        ]
        assert _calculate_context_decay_rate(turns) == 0.0

    def test_complete_decay(self):
        """Verify 100% decay when no turns reference recent context."""
        # No references at all
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, []),
            SessionTurn(2, None, []),
        ]
        assert _calculate_context_decay_rate(turns) == 1.0

    def test_partial_decay(self):
        """Verify partial decay calculation."""
        # 50% decay - half the turns reference recent context
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, 0, []),  # References recent
            SessionTurn(2, None, []),  # No reference
            SessionTurn(3, 2, []),  # References recent
            SessionTurn(4, None, []),  # No reference
        ]
        # 2 out of 4 turns after first have decay
        assert _calculate_context_decay_rate(turns) == 0.5

    def test_references_within_window(self):
        """Verify only references within 3-turn window count."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, []),
            SessionTurn(2, None, []),
            SessionTurn(3, None, []),
            SessionTurn(4, 0, []),  # References turn 0 (4 turns back - counts as decay)
        ]
        # Last turn references beyond 3-turn window
        assert _calculate_context_decay_rate(turns) == 1.0


class TestCalculateAvgReferenceChainLength:
    """Test average reference chain length calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_avg_reference_chain_length([]) == 0.0

    def test_no_references(self):
        """Verify no references returns zero."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, []),
        ]
        assert _calculate_avg_reference_chain_length(turns) == 0.0

    def test_single_reference(self):
        """Verify single reference calculation."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(5, 0, []),  # Chain length = 5
        ]
        assert _calculate_avg_reference_chain_length(turns) == 5.0

    def test_multiple_references(self):
        """Verify average of multiple references."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(2, 0, []),  # Chain length = 2
            SessionTurn(4, 0, []),  # Chain length = 4
            SessionTurn(6, 0, []),  # Chain length = 6
        ]
        # Average = (2 + 4 + 6) / 3 = 4.0
        assert _calculate_avg_reference_chain_length(turns) == 4.0

    def test_mixed_references(self):
        """Verify calculation with some turns without references."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, 0, []),  # Chain length = 1
            SessionTurn(2, None, []),  # No reference
            SessionTurn(3, 0, []),  # Chain length = 3
        ]
        # Average = (1 + 3) / 2 = 2.0
        assert _calculate_avg_reference_chain_length(turns) == 2.0


class TestCalculateTopicDriftScore:
    """Test topic drift score calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero drift."""
        assert _calculate_topic_drift_score([]) == 0.0

    def test_single_turn(self):
        """Verify single turn returns zero drift."""
        turns = [SessionTurn(0, None, ["topic1"])]
        assert _calculate_topic_drift_score(turns) == 0.0

    def test_no_drift(self):
        """Verify identical topics returns zero drift."""
        turns = [
            SessionTurn(0, None, ["topic1", "topic2"]),
            SessionTurn(1, None, ["topic1", "topic2"]),
            SessionTurn(2, None, ["topic1", "topic2"]),
        ]
        assert _calculate_topic_drift_score(turns) == 0.0

    def test_complete_drift(self):
        """Verify completely different topics returns max drift."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, None, ["topic2"]),
            SessionTurn(2, None, ["topic3"]),
        ]
        # Each turn has completely different topics
        assert _calculate_topic_drift_score(turns) == 1.0

    def test_partial_drift(self):
        """Verify partial topic overlap."""
        turns = [
            SessionTurn(0, None, ["topic1", "topic2"]),
            SessionTurn(1, None, ["topic1", "topic3"]),  # 1 overlap, 1 new
        ]
        # Jaccard distance: 1 - (1 / 3) = 0.666...
        result = _calculate_topic_drift_score(turns)
        assert 0.6 < result < 0.7

    def test_empty_initial_topics(self):
        """Verify handling of empty initial topics."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, ["topic1"]),
        ]
        assert _calculate_topic_drift_score(turns) == 0.0

    def test_empty_subsequent_topics(self):
        """Verify empty topics in later turns treated as complete drift."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, None, []),  # No topics
        ]
        assert _calculate_topic_drift_score(turns) == 1.0


class TestCalculateReactivationRate:
    """Test context reactivation rate calculation."""

    def test_empty_turns(self):
        """Verify empty turns returns zero."""
        assert _calculate_reactivation_rate([]) == 0.0

    def test_no_references(self):
        """Verify no references returns zero rate."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, []),
        ]
        assert _calculate_reactivation_rate(turns) == 0.0

    def test_all_references(self):
        """Verify all turns with references returns 100%."""
        turns = [
            SessionTurn(1, 0, []),
            SessionTurn(2, 1, []),
            SessionTurn(3, 2, []),
        ]
        assert _calculate_reactivation_rate(turns) == 1.0

    def test_partial_references(self):
        """Verify partial references calculation."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, 0, []),  # Has reference
            SessionTurn(2, None, []),  # No reference
            SessionTurn(3, 0, []),  # Has reference
        ]
        # 2 out of 4 have references = 0.5
        assert _calculate_reactivation_rate(turns) == 0.5


class TestCalculateCoherenceScore:
    """Test composite coherence score calculation."""

    def test_perfect_coherence(self):
        """Verify perfect metrics give high score."""
        metrics = ContextRetentionMetrics(
            decay_rate=0.0,  # No decay (35 points)
            avg_reference_chain_length=10.0,  # Max chain (25 points)
            topic_drift_score=0.0,  # No drift (25 points)
            reactivation_rate=1.0,  # Full reactivation (15 points)
        )
        score = _calculate_coherence_score(metrics)
        assert score == 100.0

    def test_worst_coherence(self):
        """Verify worst metrics give low score."""
        metrics = ContextRetentionMetrics(
            decay_rate=1.0,  # Complete decay (0 points)
            avg_reference_chain_length=0.0,  # No chains (0 points)
            topic_drift_score=1.0,  # Complete drift (0 points)
            reactivation_rate=0.0,  # No reactivation (0 points)
        )
        score = _calculate_coherence_score(metrics)
        assert score == 0.0

    def test_moderate_coherence(self):
        """Verify moderate metrics give middle score."""
        metrics = ContextRetentionMetrics(
            decay_rate=0.5,
            avg_reference_chain_length=5.0,
            topic_drift_score=0.5,
            reactivation_rate=0.5,
        )
        score = _calculate_coherence_score(metrics)
        assert 40 < score < 60

    def test_score_bounded(self):
        """Verify score is always between 0 and 100."""
        # Try extreme values
        metrics = ContextRetentionMetrics(
            decay_rate=2.0,  # Beyond normal range
            avg_reference_chain_length=100.0,
            topic_drift_score=-0.5,
            reactivation_rate=5.0,
        )
        score = _calculate_coherence_score(metrics)
        assert 0.0 <= score <= 100.0


class TestClassifyQualityTier:
    """Test quality tier classification."""

    def test_coherent_tier(self):
        """Verify scores >=60 are coherent."""
        assert _classify_quality_tier(60.0) == TIER_COHERENT
        assert _classify_quality_tier(80.0) == TIER_COHERENT
        assert _classify_quality_tier(100.0) == TIER_COHERENT

    def test_moderate_tier(self):
        """Verify scores 40-59 are moderate."""
        assert _classify_quality_tier(40.0) == TIER_MODERATE
        assert _classify_quality_tier(50.0) == TIER_MODERATE
        assert _classify_quality_tier(59.9) == TIER_MODERATE

    def test_fragmented_tier(self):
        """Verify scores 20-39 are fragmented."""
        assert _classify_quality_tier(20.0) == TIER_FRAGMENTED
        assert _classify_quality_tier(30.0) == TIER_FRAGMENTED
        assert _classify_quality_tier(39.9) == TIER_FRAGMENTED

    def test_disconnected_tier(self):
        """Verify scores <20 are disconnected."""
        assert _classify_quality_tier(0.0) == TIER_DISCONNECTED
        assert _classify_quality_tier(10.0) == TIER_DISCONNECTED
        assert _classify_quality_tier(19.9) == TIER_DISCONNECTED


class TestAnalyzeSessionContextRetention:
    """Test complete session context retention analysis."""

    def test_empty_session(self):
        """Verify empty session returns disconnected tier."""
        result = analyze_session_context_retention([])
        assert result.coherence_score == 0.0
        assert result.quality_tier == TIER_DISCONNECTED
        assert "Empty session" in result.insights[0]

    def test_invalid_turns_type_raises(self):
        """Verify invalid turns type raises ValueError."""
        with pytest.raises(ValueError, match="must be a sequence"):
            analyze_session_context_retention("not a list")  # type: ignore

    def test_invalid_turn_instance_raises(self):
        """Verify invalid turn instance raises ValueError."""
        with pytest.raises(ValueError, match="SessionTurn instances"):
            analyze_session_context_retention([{"not": "a turn"}])  # type: ignore

    def test_negative_turn_number_raises(self):
        """Verify negative turn number raises ValueError."""
        turn = SessionTurn(-1, None, [])
        with pytest.raises(ValueError, match="turn_number must be non-negative"):
            analyze_session_context_retention([turn])

    def test_negative_references_turn_raises(self):
        """Verify negative references_turn raises ValueError."""
        turn = SessionTurn(1, -1, [])
        with pytest.raises(ValueError, match="references_turn must be non-negative"):
            analyze_session_context_retention([turn])

    def test_duplicate_turn_numbers_raise(self):
        """Verify duplicate turn numbers are rejected."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(0, None, []),
        ]
        with pytest.raises(ValueError, match="turn_number values must be unique"):
            analyze_session_context_retention(turns)

    def test_unordered_turns_raise(self):
        """Verify turns must be ordered by turn_number."""
        turns = [
            SessionTurn(1, None, []),
            SessionTurn(0, None, []),
        ]
        with pytest.raises(ValueError, match="ordered by turn_number"):
            analyze_session_context_retention(turns)

    def test_self_reference_raises(self):
        """Verify turns cannot reference themselves."""
        turns = [SessionTurn(0, 0, [])]
        with pytest.raises(ValueError, match="earlier turn"):
            analyze_session_context_retention(turns)

    def test_future_reference_raises(self):
        """Verify turns cannot reference future turns."""
        turns = [
            SessionTurn(0, 1, []),
            SessionTurn(1, None, []),
        ]
        with pytest.raises(ValueError, match="future turn"):
            analyze_session_context_retention(turns)

    def test_missing_reference_raises(self):
        """Verify references must point to a turn present in the session."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(2, 1, []),
        ]
        with pytest.raises(ValueError, match="existing turn"):
            analyze_session_context_retention(turns)

    def test_coherent_session(self):
        """Verify coherent session gets high score."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, 0, ["topic1"]),
            SessionTurn(2, 1, ["topic1"]),
            SessionTurn(3, 2, ["topic1"]),
        ]
        result = analyze_session_context_retention(turns)
        assert result.quality_tier == TIER_COHERENT
        assert result.coherence_score >= THRESHOLD_MODERATE

    def test_fragmented_session(self):
        """Verify fragmented session gets low score."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, None, ["topic2"]),
            SessionTurn(2, None, ["topic3"]),
            SessionTurn(3, None, ["topic4"]),
        ]
        result = analyze_session_context_retention(turns)
        assert result.quality_tier in [TIER_FRAGMENTED, TIER_DISCONNECTED]
        assert result.coherence_score < THRESHOLD_FRAGMENTED

    def test_metrics_preserved(self):
        """Verify metrics are preserved in result."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, 0, ["topic1"]),
        ]
        result = analyze_session_context_retention(turns)
        assert isinstance(result.metrics, ContextRetentionMetrics)
        assert result.metrics.decay_rate >= 0.0
        assert result.metrics.reactivation_rate >= 0.0

    def test_insights_generated(self):
        """Verify insights are always generated."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
        ]
        result = analyze_session_context_retention(turns)
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        turns = [SessionTurn(0, None, [])]
        result = analyze_session_context_retention(turns)
        with pytest.raises(AttributeError):
            result.coherence_score = 99.0


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_coherent_tier_insight(self):
        """Verify coherent tier generates positive insight."""
        turns = [
            SessionTurn(i, i - 1 if i > 0 else None, ["topic1"])
            for i in range(5)
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "coherent" in insights_text or "strong" in insights_text

    def test_high_decay_insight(self):
        """Verify high decay generates warning."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(1, None, []),
            SessionTurn(2, None, []),
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "decay" in insights_text

    def test_low_decay_insight(self):
        """Verify low decay generates positive insight."""
        turns = [
            SessionTurn(i, i - 1 if i > 0 else None, [])
            for i in range(5)
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "decay" in insights_text or "memory" in insights_text

    def test_short_reference_chains_insight(self):
        """Verify short chains generate warning."""
        # All references to immediate previous turn
        turns = [
            SessionTurn(i, i - 1 if i > 0 else None, [])
            for i in range(10)
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "short" in insights_text or "reference" in insights_text

    def test_good_reference_depth_insight(self):
        """Verify good reference depth generates positive insight."""
        turns = [
            SessionTurn(0, None, []),
            SessionTurn(5, 0, []),  # Long chain
            SessionTurn(10, 0, []),  # Another long chain
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "reference" in insights_text or "depth" in insights_text

    def test_high_topic_drift_insight(self):
        """Verify high topic drift generates warning."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(1, None, ["topic2"]),
            SessionTurn(2, None, ["topic3"]),
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "drift" in insights_text or "diverged" in insights_text

    def test_low_topic_drift_insight(self):
        """Verify low topic drift generates positive insight."""
        turns = [
            SessionTurn(i, None, ["topic1"])
            for i in range(5)
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "drift" in insights_text or "focused" in insights_text

    def test_low_reactivation_insight(self):
        """Verify low reactivation generates warning."""
        turns = [
            SessionTurn(i, None, [])
            for i in range(10)
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "reactivation" in insights_text or "reference" in insights_text

    def test_high_reactivation_insight(self):
        """Verify high reactivation generates positive insight."""
        turns = [
            SessionTurn(0, None, []),
            *[SessionTurn(i, 0, []) for i in range(1, 10)],
        ]
        result = analyze_session_context_retention(turns)
        insights_text = " ".join(result.insights).lower()
        assert "reactivation" in insights_text or "reference" in insights_text

    def test_long_coherent_session_insight(self):
        """Verify long coherent session gets special recognition."""
        turns = [
            SessionTurn(i, i - 1 if i > 0 else None, ["topic1"])
            for i in range(25)
        ]
        result = analyze_session_context_retention(turns)
        if result.quality_tier == TIER_COHERENT:
            insights_text = " ".join(result.insights).lower()
            assert "impressive" in insights_text or "long" in insights_text

    def test_long_fragmented_session_insight(self):
        """Verify long fragmented session gets warning."""
        turns = [
            SessionTurn(i, None, [f"topic{i}"])
            for i in range(25)
        ]
        result = analyze_session_context_retention(turns)
        if result.quality_tier in [TIER_FRAGMENTED, TIER_DISCONNECTED]:
            insights_text = " ".join(result.insights).lower()
            assert "long" in insights_text or "breaking" in insights_text


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_turn_session(self):
        """Verify single turn produces valid analysis."""
        turns = [SessionTurn(0, None, ["topic1"])]
        result = analyze_session_context_retention(turns)
        assert 0.0 <= result.coherence_score <= 100.0
        assert result.quality_tier in [TIER_COHERENT, TIER_MODERATE, TIER_FRAGMENTED, TIER_DISCONNECTED]

    def test_large_session(self):
        """Verify handling of large session."""
        turns = [
            SessionTurn(i, i - 1 if i > 0 else None, [f"topic{i % 5}"])
            for i in range(100)
        ]
        result = analyze_session_context_retention(turns)
        assert isinstance(result, SessionContextRetention)

    def test_metrics_values_rounded(self):
        """Verify metrics are rounded appropriately."""
        turns = [
            SessionTurn(0, None, ["topic1"]),
            SessionTurn(3, 0, ["topic1"]),
        ]
        result = analyze_session_context_retention(turns)
        # Check rounding
        assert result.metrics.decay_rate == round(result.metrics.decay_rate, 3)
        assert result.coherence_score == round(result.coherence_score, 2)


class TestContextRetentionMetricsDataclass:
    """Test ContextRetentionMetrics dataclass properties."""

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        metrics = ContextRetentionMetrics(
            decay_rate=0.1,
            avg_reference_chain_length=3.5,
            topic_drift_score=0.2,
            reactivation_rate=0.8,
        )
        with pytest.raises(AttributeError):
            metrics.decay_rate = 0.5
