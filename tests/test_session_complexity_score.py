"""Tests for session complexity score calculation."""

import pytest

from synthesis.session_complexity_score import (
    SessionCharacteristics,
    SessionComplexityScore,
    calculate_session_complexity_score,
    categorize_complexity_tier,
    TIER_SIMPLE,
    TIER_MODERATE,
    TIER_COMPLEX,
    TIER_HIGH_RISK,
    THRESHOLD_MODERATE,
    THRESHOLD_COMPLEX,
    THRESHOLD_HIGH_RISK,
    WEIGHT_DURATION,
    WEIGHT_TOOL_CALLS,
    WEIGHT_CONTEXT_SWITCHES,
    WEIGHT_ERROR_RATE,
)


class TestSessionCharacteristics:
    """Test SessionCharacteristics dataclass."""

    def test_create_characteristics(self):
        """Verify characteristics can be created with all fields."""
        chars = SessionCharacteristics(
            duration_minutes=30.0,
            tool_call_count=10,
            context_switch_count=5,
            error_count=2,
            total_operations=20,
        )
        assert chars.duration_minutes == 30.0
        assert chars.tool_call_count == 10
        assert chars.context_switch_count == 5
        assert chars.error_count == 2
        assert chars.total_operations == 20

    def test_characteristics_frozen(self):
        """Verify characteristics are immutable."""
        chars = SessionCharacteristics(
            duration_minutes=30.0,
            tool_call_count=10,
            context_switch_count=5,
            error_count=2,
            total_operations=20,
        )
        with pytest.raises(AttributeError):
            chars.duration_minutes = 60.0


class TestWeightConstants:
    """Test weight constants sum to 1.0 for proper normalization."""

    def test_weights_sum_to_one(self):
        """Verify component weights sum to 1.0."""
        total = (
            WEIGHT_DURATION
            + WEIGHT_TOOL_CALLS
            + WEIGHT_CONTEXT_SWITCHES
            + WEIGHT_ERROR_RATE
        )
        assert total == pytest.approx(1.0, abs=0.001)

    def test_duration_weight(self):
        assert WEIGHT_DURATION == 0.25

    def test_tool_calls_weight(self):
        assert WEIGHT_TOOL_CALLS == 0.30

    def test_context_switches_weight(self):
        assert WEIGHT_CONTEXT_SWITCHES == 0.25

    def test_error_rate_weight(self):
        assert WEIGHT_ERROR_RATE == 0.20


class TestCategorizeComplexityTier:
    """Test complexity tier categorization."""

    def test_zero_score_is_simple(self):
        assert categorize_complexity_tier(0.0) == TIER_SIMPLE

    def test_threshold_moderate_minus_one(self):
        assert categorize_complexity_tier(THRESHOLD_MODERATE - 1) == TIER_SIMPLE

    def test_threshold_moderate_exact(self):
        assert categorize_complexity_tier(THRESHOLD_MODERATE) == TIER_MODERATE

    def test_threshold_complex_minus_one(self):
        assert categorize_complexity_tier(THRESHOLD_COMPLEX - 1) == TIER_MODERATE

    def test_threshold_complex_exact(self):
        assert categorize_complexity_tier(THRESHOLD_COMPLEX) == TIER_COMPLEX

    def test_threshold_high_risk_minus_one(self):
        assert categorize_complexity_tier(THRESHOLD_HIGH_RISK - 1) == TIER_COMPLEX

    def test_threshold_high_risk_exact(self):
        assert categorize_complexity_tier(THRESHOLD_HIGH_RISK) == TIER_HIGH_RISK

    def test_maximum_score(self):
        assert categorize_complexity_tier(100.0) == TIER_HIGH_RISK

    def test_mid_range_simple(self):
        assert categorize_complexity_tier(15.0) == TIER_SIMPLE

    def test_mid_range_moderate(self):
        assert categorize_complexity_tier(40.0) == TIER_MODERATE

    def test_mid_range_complex(self):
        assert categorize_complexity_tier(65.0) == TIER_COMPLEX

    def test_mid_range_high_risk(self):
        assert categorize_complexity_tier(90.0) == TIER_HIGH_RISK


class TestCalculateSessionComplexityScore:
    """Test session complexity score calculation."""

    def test_minimal_session_is_simple(self):
        """Verify minimal session produces simple tier."""
        result = calculate_session_complexity_score(
            duration_minutes=5.0,
            tool_call_count=2,
            context_switch_count=1,
            error_count=0,
            total_operations=2,
        )
        assert result.tier == TIER_SIMPLE
        assert result.score < THRESHOLD_MODERATE

    def test_zero_values_session(self):
        """Verify session with all zeros produces zero score."""
        result = calculate_session_complexity_score(
            duration_minutes=0.0,
            tool_call_count=0,
            context_switch_count=0,
            error_count=0,
            total_operations=0,
        )
        assert result.score == 0.0
        assert result.tier == TIER_SIMPLE

    def test_moderate_session(self):
        """Verify typical development session produces moderate tier."""
        result = calculate_session_complexity_score(
            duration_minutes=45.0,
            tool_call_count=30,
            context_switch_count=15,
            error_count=3,
            total_operations=30,
        )
        assert THRESHOLD_MODERATE <= result.score < THRESHOLD_COMPLEX
        assert result.tier == TIER_MODERATE

    def test_complex_session(self):
        """Verify complex session produces complex tier."""
        result = calculate_session_complexity_score(
            duration_minutes=90.0,
            tool_call_count=60,
            context_switch_count=30,
            error_count=10,
            total_operations=60,
        )
        assert THRESHOLD_COMPLEX <= result.score < THRESHOLD_HIGH_RISK
        assert result.tier == TIER_COMPLEX

    def test_high_risk_session(self):
        """Verify very complex session produces high-risk tier."""
        result = calculate_session_complexity_score(
            duration_minutes=120.0,
            tool_call_count=100,
            context_switch_count=50,
            error_count=25,
            total_operations=50,
        )
        assert result.score >= THRESHOLD_HIGH_RISK
        assert result.tier == TIER_HIGH_RISK

    def test_score_normalized_to_100(self):
        """Verify score is capped at 100 even with extreme values."""
        result = calculate_session_complexity_score(
            duration_minutes=500.0,  # Way over max
            tool_call_count=500,  # Way over max
            context_switch_count=200,  # Way over max
            error_count=100,
            total_operations=100,
        )
        assert result.score <= 100.0

    def test_score_non_negative(self):
        """Verify score is never negative."""
        result = calculate_session_complexity_score(
            duration_minutes=0.0,
            tool_call_count=0,
            context_switch_count=0,
            error_count=0,
            total_operations=1,
        )
        assert result.score >= 0.0

    def test_characteristics_preserved(self):
        """Verify input characteristics are preserved in result."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=15,
            context_switch_count=8,
            error_count=2,
            total_operations=15,
        )
        assert result.characteristics.duration_minutes == 30.0
        assert result.characteristics.tool_call_count == 15
        assert result.characteristics.context_switch_count == 8
        assert result.characteristics.error_count == 2
        assert result.characteristics.total_operations == 15

    def test_component_scores_included(self):
        """Verify component scores are included in result."""
        result = calculate_session_complexity_score(
            duration_minutes=60.0,
            tool_call_count=50,
            context_switch_count=25,
            error_count=5,
            total_operations=50,
        )
        assert "duration" in result.component_scores
        assert "tool_calls" in result.component_scores
        assert "context_switches" in result.component_scores
        assert "error_rate" in result.component_scores

    def test_component_scores_are_normalized(self):
        """Verify component scores are in 0-100 range."""
        result = calculate_session_complexity_score(
            duration_minutes=60.0,
            tool_call_count=50,
            context_switch_count=25,
            error_count=5,
            total_operations=50,
        )
        for score in result.component_scores.values():
            assert 0.0 <= score <= 100.0

    def test_insights_generated(self):
        """Verify insights are generated."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=15,
            context_switch_count=8,
            error_count=2,
            total_operations=15,
        )
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_score_rounded_to_two_decimals(self):
        """Verify score is rounded to 2 decimal places."""
        result = calculate_session_complexity_score(
            duration_minutes=33.333,
            tool_call_count=17,
            context_switch_count=9,
            error_count=3,
            total_operations=17,
        )
        # Check that score has at most 2 decimal places
        assert result.score == round(result.score, 2)


class TestComplexityScoreEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_no_operations_with_errors(self):
        """Verify error rate is 0 when no operations occurred."""
        result = calculate_session_complexity_score(
            duration_minutes=10.0,
            tool_call_count=5,
            context_switch_count=2,
            error_count=5,  # Errors but no operations
            total_operations=0,
        )
        assert result.component_scores["error_rate"] == 0.0

    def test_high_error_rate(self):
        """Verify high error rate increases complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=20,
            context_switch_count=10,
            error_count=10,  # 50% error rate
            total_operations=20,
        )
        assert result.component_scores["error_rate"] > 50.0

    def test_very_short_session(self):
        """Verify very short session produces low complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=1.0,
            tool_call_count=1,
            context_switch_count=0,
            error_count=0,
            total_operations=1,
        )
        assert result.score < 10.0

    def test_very_long_session_capped(self):
        """Verify very long session duration is capped at max normalization."""
        result = calculate_session_complexity_score(
            duration_minutes=500.0,  # Far exceeds max
            tool_call_count=0,
            context_switch_count=0,
            error_count=0,
            total_operations=1,
        )
        # Duration component should be capped at 100
        assert result.component_scores["duration"] == 100.0

    def test_many_tool_calls_capped(self):
        """Verify very high tool call count is capped."""
        result = calculate_session_complexity_score(
            duration_minutes=10.0,
            tool_call_count=500,  # Far exceeds max
            context_switch_count=0,
            error_count=0,
            total_operations=1,
        )
        assert result.component_scores["tool_calls"] == 100.0

    def test_many_context_switches_capped(self):
        """Verify very high context switches are capped."""
        result = calculate_session_complexity_score(
            duration_minutes=10.0,
            tool_call_count=5,
            context_switch_count=200,  # Far exceeds max
            error_count=0,
            total_operations=1,
        )
        assert result.component_scores["context_switches"] == 100.0

    def test_perfect_error_rate_capped(self):
        """Verify 100% error rate is capped properly."""
        result = calculate_session_complexity_score(
            duration_minutes=10.0,
            tool_call_count=5,
            context_switch_count=2,
            error_count=20,  # 100% error rate
            total_operations=20,
        )
        # Error rate should be normalized (100% error / 50% max = 2.0, capped at 1.0 = 100)
        assert result.component_scores["error_rate"] == 100.0


class TestComplexityInsights:
    """Test insight generation for workflow optimization."""

    def test_simple_tier_insight(self):
        """Verify simple tier produces appropriate insight."""
        result = calculate_session_complexity_score(
            duration_minutes=5.0,
            tool_call_count=3,
            context_switch_count=1,
            error_count=0,
            total_operations=3,
        )
        insights_text = " ".join(result.insights).lower()
        assert any(
            word in insights_text
            for word in ["low complexity", "simple", "quick"]
        )

    def test_moderate_tier_insight(self):
        """Verify moderate tier produces appropriate insight."""
        result = calculate_session_complexity_score(
            duration_minutes=40.0,
            tool_call_count=30,
            context_switch_count=12,
            error_count=3,
            total_operations=30,
        )
        insights_text = " ".join(result.insights).lower()
        assert "standard" in insights_text or "moderate" in insights_text

    def test_complex_tier_insight(self):
        """Verify complex tier produces appropriate insight."""
        result = calculate_session_complexity_score(
            duration_minutes=85.0,
            tool_call_count=65,
            context_switch_count=32,
            error_count=10,
            total_operations=65,
        )
        insights_text = " ".join(result.insights).lower()
        assert any(
            phrase in insights_text
            for phrase in ["breaking", "smaller", "complex"]
        )

    def test_high_risk_tier_insight(self):
        """Verify high-risk tier produces appropriate insight."""
        result = calculate_session_complexity_score(
            duration_minutes=120.0,
            tool_call_count=95,
            context_switch_count=48,
            error_count=20,
            total_operations=50,
        )
        insights_text = " ".join(result.insights).lower()
        assert any(
            phrase in insights_text
            for phrase in ["high complexity", "careful", "monitoring", "planning"]
        )

    def test_high_duration_insight(self):
        """Verify high duration produces specific insight."""
        result = calculate_session_complexity_score(
            duration_minutes=110.0,  # High duration
            tool_call_count=10,
            context_switch_count=5,
            error_count=0,
            total_operations=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "duration" in insights_text or "break" in insights_text

    def test_high_tool_usage_insight(self):
        """Verify high tool usage produces specific insight."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=90,  # High tool calls
            context_switch_count=5,
            error_count=0,
            total_operations=90,
        )
        insights_text = " ".join(result.insights).lower()
        assert "tool" in insights_text or "exploratory" in insights_text

    def test_high_context_switches_insight(self):
        """Verify high context switches produces specific insight."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=20,
            context_switch_count=45,  # High context switches
            error_count=0,
            total_operations=20,
        )
        insights_text = " ".join(result.insights).lower()
        assert "context" in insights_text or "focus" in insights_text

    def test_high_error_rate_insight(self):
        """Verify high error rate produces specific insight."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=20,
            context_switch_count=5,
            error_count=12,  # 60% error rate
            total_operations=20,
        )
        insights_text = " ".join(result.insights).lower()
        assert "error" in insights_text or "dependencies" in insights_text

    def test_resource_allocation_insight_present(self):
        """Verify all tiers include resource allocation insight."""
        for duration, expected_tier in [
            (5.0, TIER_SIMPLE),
            (45.0, TIER_MODERATE),
            (85.0, TIER_COMPLEX),
            (120.0, TIER_HIGH_RISK),
        ]:
            result = calculate_session_complexity_score(
                duration_minutes=duration,
                tool_call_count=int(duration),
                context_switch_count=int(duration / 2),
                error_count=int(duration / 10),
                total_operations=int(duration),
            )
            insights_text = " ".join(result.insights).lower()
            assert "resource allocation" in insights_text


class TestSessionComplexityScoreDataclass:
    """Test SessionComplexityScore dataclass properties."""

    def test_session_complexity_score_frozen(self):
        """Verify result is immutable."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=15,
            context_switch_count=8,
            error_count=2,
            total_operations=15,
        )
        with pytest.raises(AttributeError):
            result.score = 99.0

    def test_session_complexity_score_has_all_fields(self):
        """Verify result has all required fields."""
        result = calculate_session_complexity_score(
            duration_minutes=30.0,
            tool_call_count=15,
            context_switch_count=8,
            error_count=2,
            total_operations=15,
        )
        assert hasattr(result, "score")
        assert hasattr(result, "tier")
        assert hasattr(result, "characteristics")
        assert hasattr(result, "component_scores")
        assert hasattr(result, "insights")


class TestIntegrationWithTDDWorkflow:
    """Test integration scenarios with TDD workflow metrics."""

    def test_tdd_red_phase_session(self):
        """Verify TDD red phase session (writing failing tests) produces low complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=10.0,
            tool_call_count=8,  # Few reads/writes
            context_switch_count=2,  # Test file + source
            error_count=1,  # Expected test failure
            total_operations=8,
        )
        assert result.tier in [TIER_SIMPLE, TIER_MODERATE]

    def test_tdd_green_phase_session(self):
        """Verify TDD green phase session (making tests pass) produces low to moderate complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=25.0,
            tool_call_count=20,  # Multiple edits
            context_switch_count=8,  # Back and forth
            error_count=3,  # Some trial and error
            total_operations=20,
        )
        assert result.tier in [TIER_SIMPLE, TIER_MODERATE, TIER_COMPLEX]

    def test_tdd_refactor_phase_session(self):
        """Verify TDD refactor phase produces variable complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=40.0,
            tool_call_count=35,
            context_switch_count=15,
            error_count=2,  # Fewer errors, tests help
            total_operations=35,
        )
        assert result.tier in [TIER_MODERATE, TIER_COMPLEX]

    def test_incomplete_session_high_errors(self):
        """Verify incomplete session with many errors produces high complexity."""
        result = calculate_session_complexity_score(
            duration_minutes=60.0,
            tool_call_count=40,
            context_switch_count=20,
            error_count=20,  # 50% error rate indicates struggles
            total_operations=40,
        )
        assert result.tier in [TIER_COMPLEX, TIER_HIGH_RISK]
        assert result.component_scores["error_rate"] >= 50.0

    def test_anomalous_session_pattern(self):
        """Verify detection of anomalous patterns (many context switches, few operations)."""
        result = calculate_session_complexity_score(
            duration_minutes=90.0,  # Long duration
            tool_call_count=15,  # Few operations
            context_switch_count=40,  # Many switches
            error_count=0,
            total_operations=15,
        )
        # Should flag high context switching
        insights_text = " ".join(result.insights).lower()
        assert "context" in insights_text
