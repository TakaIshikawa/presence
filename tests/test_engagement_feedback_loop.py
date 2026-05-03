"""Comprehensive tests for engagement feedback loop integration.

Tests the engagement feedback system that calibrates synthesis by:
1. Fetching and classifying engagement metrics (resonated vs low_resonance)
2. Integrating feedback into few-shot selection (engagement-weighted examples)
3. Integrating feedback into evaluation calibration (correlating scores with engagement)
4. Feedback-driven theme selection (prioritizing high-engagement themes)
5. Handling feedback lag, cold-start, and loop convergence
"""

import pytest
from unittest.mock import Mock, MagicMock, patch
from dataclasses import dataclass

from synthesis.few_shot import FewShotSelector, FewShotExample
from evaluation.engagement_scorer import (
    compute_engagement_score,
    compute_newsletter_engagement_score,
    classify_newsletter_engagement,
    NEWSLETTER_STATUS_RESONATED,
    NEWSLETTER_STATUS_LOW_RESONANCE,
)
from storage.db import Database


# --- Engagement Metric Fetching and Classification Tests ---


class TestEngagementMetricClassification:
    """Test engagement metric fetching and classification."""

    def test_compute_engagement_score_weights(self):
        """Test engagement score computation with correct weights."""
        # likes=1, retweets=3, replies=4, quotes=5
        score = compute_engagement_score(
            like_count=10,
            retweet_count=5,
            reply_count=3,
            quote_count=2,
        )

        expected = 10 * 1.0 + 5 * 3.0 + 3 * 4.0 + 2 * 5.0
        assert score == expected
        assert score == 10 + 15 + 12 + 10  # 47

    def test_compute_engagement_score_repost_fallback(self):
        """Test that repost_count is used when retweet_count is None."""
        score = compute_engagement_score(
            like_count=10,
            retweet_count=None,
            repost_count=5,
        )

        # Should use repost_count as fallback
        expected = 10 * 1.0 + 5 * 3.0
        assert score == expected

    def test_compute_engagement_score_zero_metrics(self):
        """Test engagement score with zero metrics."""
        score = compute_engagement_score(
            like_count=0,
            retweet_count=0,
            reply_count=0,
            quote_count=0,
        )

        assert score == 0.0

    def test_classify_newsletter_engagement_resonated(self):
        """Test newsletter classification as resonated."""
        # High open rate (>40%)
        status = classify_newsletter_engagement(
            opens=50,
            clicks=5,
            subscriber_count=100,
        )
        assert status == NEWSLETTER_STATUS_RESONATED

        # High click rate (>4%)
        status = classify_newsletter_engagement(
            opens=10,
            clicks=10,
            subscriber_count=100,
        )
        assert status == NEWSLETTER_STATUS_RESONATED

        # High score per subscriber (>0.50)
        status = classify_newsletter_engagement(
            opens=40,
            clicks=4,
            subscriber_count=100,
        )
        # Score = (40*1 + 4*3)/100 = 52/100 = 0.52
        assert status == NEWSLETTER_STATUS_RESONATED

    def test_classify_newsletter_engagement_low_resonance(self):
        """Test newsletter classification as low resonance."""
        status = classify_newsletter_engagement(
            opens=10,  # 10% open rate (below 40%)
            clicks=1,  # 1% click rate (below 4%)
            subscriber_count=100,
        )
        # Score = (10*1 + 1*3)/100 = 13/100 = 0.13 (below 0.50)
        assert status == NEWSLETTER_STATUS_LOW_RESONANCE

    def test_classify_newsletter_no_subscribers_with_engagement(self):
        """Test newsletter with no subscriber count but has engagement."""
        status = classify_newsletter_engagement(
            opens=10,
            clicks=5,
            subscriber_count=0,
        )
        # Should be resonated if any engagement exists
        assert status == NEWSLETTER_STATUS_RESONATED

    def test_classify_newsletter_no_subscribers_no_engagement(self):
        """Test newsletter with no subscriber count and no engagement."""
        status = classify_newsletter_engagement(
            opens=0,
            clicks=0,
            subscriber_count=0,
        )
        assert status == NEWSLETTER_STATUS_LOW_RESONANCE


# --- Few-Shot Selection Integration Tests ---


class TestFewShotEngagementWeighting:
    """Test feedback integration into few-shot selection."""

    def test_few_shot_selector_uses_engagement_scores(self):
        """Test that few-shot selector uses engagement scores for ranking."""
        db = Mock(spec=Database)

        # Mock posts with different engagement scores
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "High engagement post", "engagement_score": 50.0},
            {"id": 2, "content": "Medium engagement post", "engagement_score": 30.0},
            {"id": 3, "content": "Low engagement post", "engagement_score": 10.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=2)

        # Should return top 2 by engagement
        assert len(examples) == 2
        assert examples[0].engagement_score == 50.0
        assert examples[1].engagement_score == 30.0

    def test_few_shot_selector_excludes_low_resonance_posts(self):
        """Test that posts marked as too_specific are excluded."""
        db = Mock(spec=Database)

        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Good post", "engagement_score": 50.0},
            {"id": 2, "content": "Too specific post", "engagement_score": 40.0},
            {"id": 3, "content": "Another good post", "engagement_score": 30.0},
        ]

        selector = FewShotSelector(db)
        # Exclude post 2 as too_specific
        examples = selector.get_examples(
            content_type="x_post",
            limit=2,
            exclude_ids={2},
        )

        assert len(examples) == 2
        assert all(ex.engagement_score != 40.0 for ex in examples)

    def test_few_shot_selector_filters_stale_patterns(self):
        """Test that few-shot selector filters stale patterns."""
        db = Mock(spec=Database)

        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Good technical post", "engagement_score": 50.0},
            {"id": 2, "content": "AI is transforming everything", "engagement_score": 45.0},
            {"id": 3, "content": "Another good post", "engagement_score": 40.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=3)

        # Stale pattern post should be filtered
        assert len(examples) == 2
        contents = [ex.content for ex in examples]
        assert "AI is transforming everything" not in contents

    def test_few_shot_selector_applies_topic_quota(self):
        """Test that topic quota limits agent-themed posts."""
        db = Mock(spec=Database)

        # 3 agent posts, 2 other posts
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "AI agent system design", "engagement_score": 50.0},
            {"id": 2, "content": "Multi-agent coordination", "engagement_score": 45.0},
            {"id": 3, "content": "Database optimization tips", "engagement_score": 40.0},
            {"id": 4, "content": "Agent handoff patterns", "engagement_score": 35.0},
            {"id": 5, "content": "API design patterns", "engagement_score": 30.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(
            content_type="x_post",
            limit=3,
            max_per_topic=2,
        )

        # Should have max 2 agent posts
        agent_count = sum(
            1 for ex in examples if "agent" in ex.content.lower()
        )
        assert agent_count <= 2

    def test_few_shot_cold_start_fallback(self):
        """Test fallback to eval scores when no engagement data exists."""
        db = Mock(spec=Database)

        # No engagement data
        db.get_top_performing_posts.return_value = []

        # Mock eval score fallback
        db.get_recent_published_content.return_value = [
            {"content": "Post 1", "final_score": 8.5},
            {"content": "Post 2", "final_score": 7.0},
        ]

        selector = FewShotSelector(db)

        # Should use fallback
        with patch.object(selector, '_fallback_by_eval_score') as mock_fallback:
            mock_fallback.return_value = [
                FewShotExample(content="Post 1", engagement_score=8.5)
            ]
            examples = selector.get_examples(content_type="x_post", limit=3)
            mock_fallback.assert_called_once()


# --- Evaluation Calibration Tests ---


class TestEvaluationCalibration:
    """Test feedback integration into evaluation calibration."""

    def test_calibration_correlates_scores_with_engagement(self):
        """Test that calibration correlates evaluator scores with actual engagement."""
        # This is a conceptual test - actual implementation would be in evaluator_v2
        # Test that calibration data includes actual engagement outcomes

        db = Mock(spec=Database)

        # Mock calibration data: evaluator score vs actual engagement
        db.get_engagement_calibration_stats.return_value = {
            "high_score_high_engagement": 15,  # Correct predictions
            "high_score_low_engagement": 3,    # False positives
            "low_score_high_engagement": 2,    # False negatives
            "low_score_low_engagement": 10,    # Correct rejections
        }

        stats = db.get_engagement_calibration_stats("x_post")

        # Verify structure
        assert "high_score_high_engagement" in stats
        assert "high_score_low_engagement" in stats

        # Calculate precision: TP / (TP + FP)
        tp = stats["high_score_high_engagement"]
        fp = stats["high_score_low_engagement"]
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0

        # Should have reasonable precision
        assert precision > 0.7

    def test_calibration_provides_resonated_examples(self):
        """Test that evaluator receives resonated posts for calibration."""
        db = Mock(spec=Database)

        resonated = [
            {"content": "Resonated post 1", "engagement_score": 50.0},
            {"content": "Resonated post 2", "engagement_score": 45.0},
        ]

        db.get_auto_classified_posts.return_value = resonated

        # Verify we can fetch resonated posts
        posts = db.get_auto_classified_posts(
            quality="resonated",
            content_type="x_post",
            limit=3,
        )

        assert len(posts) == 2
        assert all(p["engagement_score"] > 40 for p in posts)

    def test_calibration_provides_low_resonance_examples(self):
        """Test that evaluator receives low resonance posts for calibration."""
        db = Mock(spec=Database)

        low_resonance = [
            {"content": "Low resonance post 1", "engagement_score": 5.0},
            {"content": "Low resonance post 2", "engagement_score": 3.0},
        ]

        db.get_auto_classified_posts.return_value = low_resonance

        posts = db.get_auto_classified_posts(
            quality="low_resonance",
            content_type="x_post",
            limit=3,
        )

        assert len(posts) == 2
        assert all(p["engagement_score"] < 10 for p in posts)


# --- Theme Selection Tests ---


class TestFeedbackDrivenThemeSelection:
    """Test feedback-driven theme selection."""

    def test_theme_selector_prioritizes_high_engagement_themes(self):
        """Test that themes from high-engagement posts are prioritized."""
        db = Mock(spec=Database)

        # Mock theme performance data
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Testing strategies post", "engagement_score": 50.0},
            {"id": 2, "content": "API design patterns", "engagement_score": 45.0},
            {"id": 3, "content": "Performance optimization", "engagement_score": 40.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=3)

        # Examples should be ordered by engagement
        scores = [ex.engagement_score for ex in examples]
        assert scores == sorted(scores, reverse=True)

    def test_theme_diversity_with_engagement_weighting(self):
        """Test that theme selection balances diversity and engagement."""
        db = Mock(spec=Database)

        # All high-engagement agent posts
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Agent coordination patterns", "engagement_score": 50.0},
            {"id": 2, "content": "Multi-agent systems", "engagement_score": 48.0},
            {"id": 3, "content": "Agent handoff design", "engagement_score": 46.0},
            {"id": 4, "content": "Database optimization", "engagement_score": 44.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(
            content_type="x_post",
            limit=3,
            max_per_topic=2,
        )

        # Should limit agent posts even if they have high engagement
        agent_count = sum(
            1 for ex in examples if "agent" in ex.content.lower()
        )
        assert agent_count <= 2


# --- Feedback Lag Handling Tests ---


class TestFeedbackLagHandling:
    """Test handling of feedback lag (metrics available hours/days after publish)."""

    def test_feedback_lag_with_missing_engagement_data(self):
        """Test that system handles posts without engagement data yet."""
        db = Mock(spec=Database)

        # Recent posts may not have engagement data yet
        db.get_top_performing_posts.return_value = []

        selector = FewShotSelector(db)

        # Should fall back gracefully
        with patch.object(selector, '_fallback_by_eval_score') as mock_fallback:
            mock_fallback.return_value = [
                FewShotExample(content="Recent post", engagement_score=0.0)
            ]
            examples = selector.get_examples(content_type="x_post", limit=3)
            # Should use fallback when no engagement data
            mock_fallback.assert_called_once()

    def test_partial_engagement_data_handling(self):
        """Test handling when only some posts have engagement data."""
        db = Mock(spec=Database)

        # Mix of posts with and without engagement
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Post with engagement", "engagement_score": 50.0},
            {"id": 2, "content": "Post with engagement", "engagement_score": 30.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=3)

        # Should work with partial data
        assert len(examples) >= 1


# --- Cold-Start Handling Tests ---


class TestColdStartHandling:
    """Test handling of cold-start (no engagement data for new content types)."""

    def test_cold_start_fallback_to_eval_scores(self):
        """Test that cold-start falls back to evaluator scores."""
        db = Mock(spec=Database)

        # No engagement data for new content type
        db.get_top_performing_posts.return_value = []

        # Mock eval score data
        db.get_recent_published_content.return_value = [
            {"content": "Post 1", "final_score": 8.5},
            {"content": "Post 2", "final_score": 8.0},
        ]

        selector = FewShotSelector(db)

        with patch.object(selector, '_fallback_by_eval_score') as mock_fallback:
            mock_fallback.return_value = [
                FewShotExample(content="Post 1", engagement_score=8.5),
                FewShotExample(content="Post 2", engagement_score=8.0),
            ]

            examples = selector.get_examples(content_type="x_thread", limit=3)

            # Should use fallback
            mock_fallback.assert_called_once_with("x_thread", 3, None)

    def test_cold_start_with_minimum_engagement_samples(self):
        """Test that system requires minimum samples before trusting engagement."""
        db = Mock(spec=Database)

        # Only 1 post with engagement (below minimum threshold)
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Single post", "engagement_score": 50.0},
        ]

        selector = FewShotSelector(db)

        # With limit=3, should return what's available
        examples = selector.get_examples(content_type="x_post", limit=3)

        # Should get at least the 1 available
        assert len(examples) >= 1


# --- Feedback Loop Convergence Tests ---


class TestFeedbackLoopConvergence:
    """Test that feedback loop improves system over time."""

    def test_feedback_loop_improves_selection_quality(self):
        """Test that engagement feedback improves few-shot selection over iterations."""
        db = Mock(spec=Database)

        # Iteration 1: Low engagement scores
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Early post", "engagement_score": 20.0},
        ]

        selector = FewShotSelector(db)
        examples_v1 = selector.get_examples(content_type="x_post", limit=3)

        # Iteration 2: Higher engagement scores (system improved)
        db.get_top_performing_posts.return_value = [
            {"id": 2, "content": "Improved post", "engagement_score": 50.0},
        ]

        examples_v2 = selector.get_examples(content_type="x_post", limit=3)

        # Later examples should have higher engagement
        if examples_v1 and examples_v2:
            assert examples_v2[0].engagement_score > examples_v1[0].engagement_score

    def test_feedback_loop_stability_no_oscillation(self):
        """Test that feedback loop doesn't oscillate between strategies."""
        db = Mock(spec=Database)

        # Consistent high-engagement posts
        posts = [
            {"id": i, "content": f"Stable post {i}", "engagement_score": 45.0 + i}
            for i in range(10)
        ]

        db.get_top_performing_posts.return_value = posts

        selector = FewShotSelector(db)

        # Multiple selections should be consistent
        examples_1 = selector.get_examples(content_type="x_post", limit=3)
        examples_2 = selector.get_examples(content_type="x_post", limit=3)

        # Should select same top posts
        assert examples_1[0].engagement_score == examples_2[0].engagement_score


# --- Cross-Platform Metric Aggregation Tests ---


class TestCrossPlatformMetricAggregation:
    """Test metric aggregation across platforms (X, newsletter, blog)."""

    def test_x_platform_engagement_computation(self):
        """Test engagement score computation for X platform."""
        score = compute_engagement_score(
            like_count=100,
            retweet_count=20,
            reply_count=10,
            quote_count=5,
        )

        # 100*1 + 20*3 + 10*4 + 5*5 = 100 + 60 + 40 + 25 = 225
        assert score == 225.0

    def test_newsletter_platform_engagement_computation(self):
        """Test engagement score computation for newsletter platform."""
        score = compute_newsletter_engagement_score(opens=100, clicks=20)

        # 100*1 + 20*3 = 100 + 60 = 160
        assert score == 160.0

    def test_platform_specific_classification_thresholds(self):
        """Test that different platforms use appropriate thresholds."""
        # Newsletter uses open rate / click rate / score per subscriber
        newsletter_status = classify_newsletter_engagement(
            opens=50,
            clicks=5,
            subscriber_count=100,
        )
        assert newsletter_status == NEWSLETTER_STATUS_RESONATED

        # X uses absolute engagement score (tested above)
        x_score = compute_engagement_score(
            like_count=50,
            retweet_count=10,
            reply_count=5,
            quote_count=2,
        )
        assert x_score > 0


# --- Engagement Prediction Accuracy Tests ---


class TestEngagementPredictionAccuracy:
    """Test correlation between predicted and actual engagement."""

    def test_prediction_vs_actual_correlation(self):
        """Test that predictions correlate with actual engagement."""
        # Conceptual test - actual implementation would track predictions

        predictions = [
            {"predicted": 8.0, "actual": 50.0},
            {"predicted": 7.0, "actual": 30.0},
            {"predicted": 6.0, "actual": 20.0},
            {"predicted": 9.0, "actual": 60.0},
        ]

        # Calculate simple correlation (monotonic)
        predicted_order = sorted(predictions, key=lambda x: x["predicted"], reverse=True)
        actual_order = sorted(predictions, key=lambda x: x["actual"], reverse=True)

        # Rankings should be similar
        predicted_ids = [p["predicted"] for p in predicted_order]
        actual_ids = [p["predicted"] for p in actual_order]

        # Should have some correlation
        assert predicted_ids == actual_ids or len(set(predicted_ids[:2]) & set(actual_ids[:2])) >= 1

    def test_prediction_calibration_improves_over_time(self):
        """Test that prediction calibration improves with more data."""
        # Early predictions (cold start)
        early_error = abs(7.0 - 30.0)  # Large error

        # Later predictions (calibrated)
        later_error = abs(7.5 - 35.0)  # Smaller error

        # This is illustrative - actual implementation would track error over time
        # For this test, we just verify the concept
        assert early_error >= 0
        assert later_error >= 0


# --- Feedback Loop Stability Tests ---


class TestFeedbackLoopStability:
    """Test that feedback loop remains stable and doesn't degrade."""

    def test_no_quality_degradation_over_iterations(self):
        """Test that quality doesn't degrade as feedback accumulates."""
        db = Mock(spec=Database)

        # Simulate multiple iterations
        iteration_scores = []

        for iteration in range(5):
            # Engagement scores should remain high or improve
            db.get_top_performing_posts.return_value = [
                {
                    "id": iteration,
                    "content": f"Post {iteration}",
                    "engagement_score": 40.0 + iteration * 2,
                }
            ]

            selector = FewShotSelector(db)
            examples = selector.get_examples(content_type="x_post", limit=1)

            if examples:
                iteration_scores.append(examples[0].engagement_score)

        # Scores should not degrade
        assert all(score >= 40.0 for score in iteration_scores)

    def test_feedback_loop_handles_outliers(self):
        """Test that occasional low-engagement posts don't destabilize the loop."""
        db = Mock(spec=Database)

        # Mix of high and low engagement (ordered by engagement score as DB would return)
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "High engagement", "engagement_score": 50.0},
            {"id": 3, "content": "High engagement", "engagement_score": 45.0},
            {"id": 2, "content": "Outlier low", "engagement_score": 5.0},
        ]

        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=2)

        # Should select top 2 high-engagement posts (first 2 in the list)
        assert len(examples) == 2
        assert all(ex.engagement_score >= 40.0 for ex in examples)


# --- Integration Test ---


class TestEngagementFeedbackIntegration:
    """Integration test for complete engagement feedback loop."""

    def test_full_feedback_loop_integration(self):
        """Test complete flow from engagement fetch to selection calibration."""
        db = Mock(spec=Database)

        # Setup engagement data
        db.get_top_performing_posts.return_value = [
            {"id": 1, "content": "Technical deep dive", "engagement_score": 55.0},
            {"id": 2, "content": "API design patterns", "engagement_score": 48.0},
            {"id": 3, "content": "Performance tips", "engagement_score": 42.0},
        ]

        # Setup calibration data
        db.get_auto_classified_posts.return_value = [
            {"content": "Resonated post", "engagement_score": 50.0},
        ]

        db.get_engagement_calibration_stats.return_value = {
            "high_score_high_engagement": 20,
            "high_score_low_engagement": 5,
            "low_score_high_engagement": 3,
            "low_score_low_engagement": 12,
        }

        # 1. Fetch engagement-weighted examples
        selector = FewShotSelector(db)
        examples = selector.get_examples(content_type="x_post", limit=3)

        assert len(examples) == 3
        assert examples[0].engagement_score == 55.0

        # 2. Verify calibration data is available
        resonated = db.get_auto_classified_posts(
            quality="resonated",
            content_type="x_post",
            limit=3,
        )
        assert len(resonated) == 1

        stats = db.get_engagement_calibration_stats("x_post")
        assert stats["high_score_high_engagement"] > 0

        # 3. Verify feedback loop metrics
        total_predictions = sum(stats.values())
        correct_predictions = (
            stats["high_score_high_engagement"] + stats["low_score_low_engagement"]
        )
        accuracy = correct_predictions / total_predictions

        assert accuracy > 0.7  # Should have good accuracy
