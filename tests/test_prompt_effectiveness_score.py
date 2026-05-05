"""Tests for prompt effectiveness score calculation."""

import pytest

from synthesis.prompt_effectiveness_score import (
    PromptMetrics,
    PromptEffectivenessScore,
    calculate_prompt_effectiveness_score,
    _calculate_first_response_score,
    _calculate_clarification_score,
    _calculate_revision_score,
    _calculate_completion_score,
    _categorize_quality_tier,
    TIER_POOR,
    TIER_AVERAGE,
    TIER_GOOD,
    TIER_EXCELLENT,
    THRESHOLD_AVERAGE,
    THRESHOLD_GOOD,
    THRESHOLD_EXCELLENT,
    WEIGHT_FIRST_RESPONSE_SUCCESS,
    WEIGHT_CLARIFICATION_RATE,
    WEIGHT_REVISION_CYCLES,
    WEIGHT_COMPLETION_RATE,
    MAX_CLARIFICATIONS,
    MAX_REVISIONS,
)


class TestPromptMetrics:
    """Test PromptMetrics dataclass."""

    def test_create_metrics(self):
        """Verify metrics can be created with all fields."""
        metrics = PromptMetrics(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=3,
            revision_cycles=5,
            tasks_completed=8,
            tasks_attempted=10,
        )
        assert metrics.total_prompts == 10
        assert metrics.first_response_successes == 7
        assert metrics.clarification_requests == 3
        assert metrics.revision_cycles == 5
        assert metrics.tasks_completed == 8
        assert metrics.tasks_attempted == 10

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        metrics = PromptMetrics(
            total_prompts=5,
            first_response_successes=3,
            clarification_requests=1,
            revision_cycles=2,
            tasks_completed=4,
            tasks_attempted=5,
        )
        with pytest.raises(AttributeError):
            metrics.total_prompts = 10


class TestWeightConstants:
    """Test weight constants sum to 1.0."""

    def test_weights_sum_to_one(self):
        """Verify component weights sum to 1.0."""
        total = (
            WEIGHT_FIRST_RESPONSE_SUCCESS
            + WEIGHT_CLARIFICATION_RATE
            + WEIGHT_REVISION_CYCLES
            + WEIGHT_COMPLETION_RATE
        )
        assert total == pytest.approx(1.0, abs=0.001)


class TestCalculateFirstResponseScore:
    """Test first-response success score calculation."""

    def test_zero_prompts(self):
        """Verify zero prompts returns zero score."""
        assert _calculate_first_response_score(0, 0) == 0.0

    def test_all_successful(self):
        """Verify 100% success rate."""
        assert _calculate_first_response_score(10, 10) == 1.0

    def test_no_successes(self):
        """Verify 0% success rate."""
        assert _calculate_first_response_score(0, 10) == 0.0

    def test_partial_success(self):
        """Verify partial success rate."""
        assert _calculate_first_response_score(7, 10) == 0.7

    def test_single_success(self):
        """Verify single success calculation."""
        assert _calculate_first_response_score(1, 5) == 0.2


class TestCalculateClarificationScore:
    """Test clarification rate score calculation."""

    def test_zero_prompts(self):
        """Verify zero prompts returns zero score."""
        assert _calculate_clarification_score(0, 0) == 0.0

    def test_no_clarifications(self):
        """Verify no clarifications gives perfect score."""
        assert _calculate_clarification_score(0, 10) == 1.0

    def test_low_clarifications(self):
        """Verify low clarification rate gives high score."""
        # 0.5 per prompt / 3.0 max = 0.167, inverted = 0.833
        result = _calculate_clarification_score(5, 10)
        assert 0.8 < result < 0.9

    def test_moderate_clarifications(self):
        """Verify moderate clarification rate."""
        # 1.5 per prompt / 3.0 max = 0.5, inverted = 0.5
        result = _calculate_clarification_score(15, 10)
        assert 0.4 < result < 0.6

    def test_high_clarifications(self):
        """Verify high clarification rate gives low score."""
        # 3.0 per prompt / 3.0 max = 1.0, inverted = 0.0
        assert _calculate_clarification_score(30, 10) == 0.0

    def test_excessive_clarifications_capped(self):
        """Verify excessive clarifications are capped at 0."""
        # More than max should still be 0
        assert _calculate_clarification_score(50, 10) == 0.0


class TestCalculateRevisionScore:
    """Test revision cycles score calculation."""

    def test_zero_prompts(self):
        """Verify zero prompts returns zero score."""
        assert _calculate_revision_score(0, 0) == 0.0

    def test_no_revisions(self):
        """Verify no revisions gives perfect score."""
        assert _calculate_revision_score(0, 10) == 1.0

    def test_low_revisions(self):
        """Verify low revision rate gives high score."""
        # 1.0 per prompt / 5.0 max = 0.2, inverted = 0.8
        result = _calculate_revision_score(10, 10)
        assert 0.7 < result < 0.9

    def test_moderate_revisions(self):
        """Verify moderate revision rate."""
        # 2.5 per prompt / 5.0 max = 0.5, inverted = 0.5
        result = _calculate_revision_score(25, 10)
        assert 0.4 < result < 0.6

    def test_high_revisions(self):
        """Verify high revision rate gives low score."""
        # 5.0 per prompt / 5.0 max = 1.0, inverted = 0.0
        assert _calculate_revision_score(50, 10) == 0.0

    def test_excessive_revisions_capped(self):
        """Verify excessive revisions are capped at 0."""
        assert _calculate_revision_score(100, 10) == 0.0


class TestCalculateCompletionScore:
    """Test task completion score calculation."""

    def test_zero_tasks(self):
        """Verify zero tasks returns zero score."""
        assert _calculate_completion_score(0, 0) == 0.0

    def test_all_completed(self):
        """Verify 100% completion rate."""
        assert _calculate_completion_score(10, 10) == 1.0

    def test_no_completions(self):
        """Verify 0% completion rate."""
        assert _calculate_completion_score(0, 10) == 0.0

    def test_partial_completion(self):
        """Verify partial completion rate."""
        assert _calculate_completion_score(6, 10) == 0.6

    def test_single_completion(self):
        """Verify single completion calculation."""
        assert _calculate_completion_score(1, 5) == 0.2


class TestCategorizeQualityTier:
    """Test quality tier categorization."""

    def test_poor_tier(self):
        """Verify scores < 50 are poor."""
        assert _categorize_quality_tier(0.0) == TIER_POOR
        assert _categorize_quality_tier(30.0) == TIER_POOR
        assert _categorize_quality_tier(49.9) == TIER_POOR

    def test_average_tier(self):
        """Verify scores 50-69 are average."""
        assert _categorize_quality_tier(50.0) == TIER_AVERAGE
        assert _categorize_quality_tier(60.0) == TIER_AVERAGE
        assert _categorize_quality_tier(69.9) == TIER_AVERAGE

    def test_good_tier(self):
        """Verify scores 70-84 are good."""
        assert _categorize_quality_tier(70.0) == TIER_GOOD
        assert _categorize_quality_tier(75.0) == TIER_GOOD
        assert _categorize_quality_tier(84.9) == TIER_GOOD

    def test_excellent_tier(self):
        """Verify scores >= 85 are excellent."""
        assert _categorize_quality_tier(85.0) == TIER_EXCELLENT
        assert _categorize_quality_tier(95.0) == TIER_EXCELLENT
        assert _categorize_quality_tier(100.0) == TIER_EXCELLENT


class TestCalculatePromptEffectivenessScore:
    """Test complete prompt effectiveness score calculation."""

    def test_excellent_prompts(self):
        """Verify excellent prompt quality gets high score."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=9,  # 90%
            clarification_requests=1,  # 0.1 per prompt
            revision_cycles=2,  # 0.2 per prompt
            tasks_completed=10,  # 100%
            tasks_attempted=10,
        )
        assert result.tier == TIER_EXCELLENT
        assert result.score >= THRESHOLD_EXCELLENT

    def test_poor_prompts(self):
        """Verify poor prompt quality gets low score."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=1,  # 10%
            clarification_requests=25,  # 2.5 per prompt
            revision_cycles=40,  # 4.0 per prompt
            tasks_completed=3,  # 30%
            tasks_attempted=10,
        )
        assert result.tier in [TIER_POOR, TIER_AVERAGE]
        assert result.score < THRESHOLD_AVERAGE

    def test_average_prompts(self):
        """Verify average prompt quality gets middle score."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=5,  # 50%
            clarification_requests=10,  # 1.0 per prompt
            revision_cycles=20,  # 2.0 per prompt
            tasks_completed=6,  # 60%
            tasks_attempted=10,
        )
        assert result.tier in [TIER_AVERAGE, TIER_GOOD]

    def test_good_prompts(self):
        """Verify good prompt quality."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=7,  # 70%
            clarification_requests=5,  # 0.5 per prompt
            revision_cycles=10,  # 1.0 per prompt
            tasks_completed=8,  # 80%
            tasks_attempted=10,
        )
        assert result.tier in [TIER_GOOD, TIER_EXCELLENT]

    def test_zero_prompts(self):
        """Verify zero prompts returns poor tier."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=0,
            first_response_successes=0,
            clarification_requests=0,
            revision_cycles=0,
            tasks_completed=0,
            tasks_attempted=0,
        )
        assert result.score == 0.0
        assert result.tier == TIER_POOR
        assert "insufficient data" in result.insights[0].lower()

    def test_perfect_scores(self):
        """Verify perfect metrics give perfect score."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=10,
            clarification_requests=0,
            revision_cycles=0,
            tasks_completed=10,
            tasks_attempted=10,
        )
        assert result.score == 100.0
        assert result.tier == TIER_EXCELLENT

    def test_metrics_preserved(self):
        """Verify input metrics are preserved in result."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=15,
            first_response_successes=10,
            clarification_requests=5,
            revision_cycles=8,
            tasks_completed=12,
            tasks_attempted=15,
        )
        assert result.metrics.total_prompts == 15
        assert result.metrics.first_response_successes == 10
        assert result.metrics.clarification_requests == 5
        assert result.metrics.revision_cycles == 8
        assert result.metrics.tasks_completed == 12
        assert result.metrics.tasks_attempted == 15

    def test_component_scores_included(self):
        """Verify component scores are in result."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=5,
            revision_cycles=8,
            tasks_completed=8,
            tasks_attempted=10,
        )
        assert "first_response_success" in result.component_scores
        assert "clarification_rate" in result.component_scores
        assert "revision_cycles" in result.component_scores
        assert "completion_rate" in result.component_scores

    def test_insights_generated(self):
        """Verify insights are generated."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=5,
            clarification_requests=10,
            revision_cycles=15,
            tasks_completed=7,
            tasks_attempted=10,
        )
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=5,
            first_response_successes=3,
            clarification_requests=2,
            revision_cycles=3,
            tasks_completed=4,
            tasks_attempted=5,
        )
        with pytest.raises(AttributeError):
            result.score = 99.0

    def test_score_bounded(self):
        """Verify score is always between 0 and 100."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=10,
            clarification_requests=0,
            revision_cycles=0,
            tasks_completed=10,
            tasks_attempted=10,
        )
        assert 0.0 <= result.score <= 100.0


class TestValidation:
    """Test input validation."""

    def test_negative_total_prompts_raises(self):
        """Verify negative total_prompts raises ValueError."""
        with pytest.raises(ValueError, match="total_prompts must be non-negative"):
            calculate_prompt_effectiveness_score(-1, 0, 0, 0, 0, 0)

    def test_negative_successes_raises(self):
        """Verify negative successes raises ValueError."""
        with pytest.raises(ValueError, match="first_response_successes must be non-negative"):
            calculate_prompt_effectiveness_score(10, -1, 0, 0, 0, 0)

    def test_negative_clarifications_raises(self):
        """Verify negative clarifications raises ValueError."""
        with pytest.raises(ValueError, match="clarification_requests must be non-negative"):
            calculate_prompt_effectiveness_score(10, 5, -1, 0, 0, 0)

    def test_negative_revisions_raises(self):
        """Verify negative revisions raises ValueError."""
        with pytest.raises(ValueError, match="revision_cycles must be non-negative"):
            calculate_prompt_effectiveness_score(10, 5, 2, -1, 0, 0)

    def test_negative_completed_raises(self):
        """Verify negative completed raises ValueError."""
        with pytest.raises(ValueError, match="tasks_completed must be non-negative"):
            calculate_prompt_effectiveness_score(10, 5, 2, 3, -1, 10)

    def test_negative_attempted_raises(self):
        """Verify negative attempted raises ValueError."""
        with pytest.raises(ValueError, match="tasks_attempted must be non-negative"):
            calculate_prompt_effectiveness_score(10, 5, 2, 3, 5, -1)

    def test_successes_exceed_prompts_raises(self):
        """Verify successes > prompts raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed total_prompts"):
            calculate_prompt_effectiveness_score(10, 15, 0, 0, 0, 0)

    def test_completed_exceed_attempted_raises(self):
        """Verify completed > attempted raises ValueError."""
        with pytest.raises(ValueError, match="cannot exceed tasks_attempted"):
            calculate_prompt_effectiveness_score(10, 5, 0, 0, 10, 5)


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_excellent_tier_insight(self):
        """Verify excellent tier generates positive feedback."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=9,
            clarification_requests=1,
            revision_cycles=2,
            tasks_completed=10,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "excellent" in insights_text

    def test_poor_tier_insight(self):
        """Verify poor tier generates improvement suggestions."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=1,
            clarification_requests=30,
            revision_cycles=40,
            tasks_completed=2,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "poor" in insights_text or "improvement" in insights_text

    def test_high_first_response_insight(self):
        """Verify high first-response rate generates positive feedback."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=9,
            clarification_requests=2,
            revision_cycles=3,
            tasks_completed=9,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "high" in insights_text or "clear" in insights_text

    def test_low_first_response_insight(self):
        """Verify low first-response rate generates warning."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=2,
            clarification_requests=5,
            revision_cycles=10,
            tasks_completed=6,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "low" in insights_text or "vague" in insights_text

    def test_high_clarification_insight(self):
        """Verify high clarification rate generates warning."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=5,
            clarification_requests=25,  # 2.5 per prompt
            revision_cycles=10,
            tasks_completed=7,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "clarification" in insights_text or "details" in insights_text

    def test_low_clarification_insight(self):
        """Verify low clarification rate generates positive feedback."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=3,  # 0.3 per prompt
            revision_cycles=5,
            tasks_completed=8,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "clarification" in insights_text or "well-structured" in insights_text

    def test_high_revision_insight(self):
        """Verify high revision rate generates warning."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=3,
            clarification_requests=10,
            revision_cycles=35,  # 3.5 per prompt
            tasks_completed=6,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "revision" in insights_text

    def test_low_revision_insight(self):
        """Verify low revision rate generates positive feedback."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=5,
            revision_cycles=8,  # 0.8 per prompt
            tasks_completed=9,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "revision" in insights_text or "alignment" in insights_text

    def test_high_completion_insight(self):
        """Verify high completion rate generates positive feedback."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=5,
            revision_cycles=8,
            tasks_completed=9,  # 90%
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "completion" in insights_text or "achievement" in insights_text

    def test_low_completion_insight(self):
        """Verify low completion rate generates warning."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=5,
            clarification_requests=10,
            revision_cycles=15,
            tasks_completed=4,  # 40%
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        assert "completion" in insights_text or "incomplete" in insights_text

    def test_improvement_suggestions_for_poor(self):
        """Verify poor quality generates specific suggestions."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=2,
            clarification_requests=20,
            revision_cycles=30,
            tasks_completed=3,
            tasks_attempted=10,
        )
        insights_text = " ".join(result.insights).lower()
        # Should suggest improvements
        assert "suggest" in insights_text or "improve" in insights_text


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_single_prompt_successful(self):
        """Verify single successful prompt."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=1,
            first_response_successes=1,
            clarification_requests=0,
            revision_cycles=0,
            tasks_completed=1,
            tasks_attempted=1,
        )
        assert result.score == 100.0

    def test_single_prompt_failed(self):
        """Verify single failed prompt."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=1,
            first_response_successes=0,
            clarification_requests=3,
            revision_cycles=5,
            tasks_completed=0,
            tasks_attempted=1,
        )
        assert result.score < 50.0

    def test_no_tasks_attempted(self):
        """Verify handling when no tasks attempted."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=5,
            clarification_requests=10,
            revision_cycles=15,
            tasks_completed=0,
            tasks_attempted=0,
        )
        # Completion score should be 0 but other scores still factor in
        assert result.component_scores["completion_rate"] == 0.0

    def test_all_metrics_at_max(self):
        """Verify behavior when all metrics at maximum thresholds."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=10,
            first_response_successes=10,
            clarification_requests=30,  # 3.0 per prompt (max)
            revision_cycles=50,  # 5.0 per prompt (max)
            tasks_completed=10,
            tasks_attempted=10,
        )
        # Mixed: perfect successes/completion but max clarifications/revisions
        assert 0 <= result.score <= 100

    def test_large_numbers(self):
        """Verify handling of large numbers."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=1000,
            first_response_successes=800,
            clarification_requests=500,
            revision_cycles=1000,
            tasks_completed=900,
            tasks_attempted=1000,
        )
        assert isinstance(result, PromptEffectivenessScore)

    def test_metrics_values_rounded(self):
        """Verify metrics are rounded appropriately."""
        result = calculate_prompt_effectiveness_score(
            total_prompts=3,
            first_response_successes=1,  # 33.333...%
            clarification_requests=1,
            revision_cycles=1,
            tasks_completed=2,  # 66.666...%
            tasks_attempted=3,
        )
        # Check rounding to 2 decimal places
        assert result.score == round(result.score, 2)
        for score in result.component_scores.values():
            assert score == round(score, 2)


class TestPromptEffectivenessScoreDataclass:
    """Test PromptEffectivenessScore dataclass properties."""

    def test_result_frozen(self):
        """Verify result is immutable."""
        metrics = PromptMetrics(
            total_prompts=10,
            first_response_successes=7,
            clarification_requests=5,
            revision_cycles=8,
            tasks_completed=8,
            tasks_attempted=10,
        )
        result = PromptEffectivenessScore(
            score=75.0,
            tier=TIER_GOOD,
            metrics=metrics,
            component_scores={},
            insights=["Test"],
        )
        with pytest.raises(AttributeError):
            result.score = 80.0
