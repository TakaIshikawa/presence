"""Tests for engagement score computation."""

from evaluation.engagement_scorer import (
    compute_engagement_score,
    compute_newsletter_engagement_score,
    classify_newsletter_engagement,
    NEWSLETTER_STATUS_LOW_RESONANCE,
    NEWSLETTER_STATUS_RESONATED,
    WEIGHT_LIKE,
    WEIGHT_RETWEET,
    WEIGHT_REPLY,
    WEIGHT_QUOTE,
)


class TestWeightConstants:
    """Regression guard: verify weight constants match expected values."""

    def test_weight_like(self):
        assert WEIGHT_LIKE == 1.0

    def test_weight_retweet(self):
        assert WEIGHT_RETWEET == 3.0

    def test_weight_reply(self):
        assert WEIGHT_REPLY == 4.0

    def test_weight_quote(self):
        assert WEIGHT_QUOTE == 5.0


class TestComputeEngagementScore:
    def test_zero_engagement(self):
        assert compute_engagement_score(0, 0, 0, 0) == 0.0

    def test_likes_only(self):
        assert compute_engagement_score(10, 0, 0, 0) == 10.0

    def test_retweets_weighted_3x(self):
        assert compute_engagement_score(0, 5, 0, 0) == 15.0

    def test_replies_weighted_4x(self):
        assert compute_engagement_score(0, 0, 3, 0) == 12.0

    def test_quotes_weighted_5x(self):
        assert compute_engagement_score(0, 0, 0, 2) == 10.0

    def test_mixed_counts(self):
        # 10*1 + 5*3 + 3*4 + 2*5 = 10 + 15 + 12 + 10 = 47
        assert compute_engagement_score(10, 5, 3, 2) == 47.0

    def test_single_each(self):
        # 1*1 + 1*3 + 1*4 + 1*5 = 13
        assert compute_engagement_score(1, 1, 1, 1) == 13.0

    def test_returns_float(self):
        result = compute_engagement_score(1, 0, 0, 0)
        assert isinstance(result, float)

    def test_large_values_no_overflow(self):
        """Verify large metric counts produce correct results without overflow."""
        # Use large but realistic values
        large_likes = 1_000_000
        large_retweets = 500_000
        large_replies = 250_000
        large_quotes = 100_000

        expected = (
            large_likes * 1.0
            + large_retweets * 3.0
            + large_replies * 4.0
            + large_quotes * 5.0
        )
        # 1,000,000 + 1,500,000 + 1,000,000 + 500,000 = 4,000,000
        assert expected == 4_000_000.0

        result = compute_engagement_score(
            large_likes, large_retweets, large_replies, large_quotes
        )
        assert result == expected
        assert isinstance(result, float)

    def test_very_large_single_metric(self):
        """Verify very large single metric value."""
        result = compute_engagement_score(10_000_000, 0, 0, 0)
        assert result == 10_000_000.0

    def test_known_computation_example(self):
        """Verify weighted sum with specific known example from requirements."""
        # Example: 10 likes + 5 retweets + 2 replies + 1 quote
        # = 10*1 + 5*3 + 2*4 + 1*5 = 10 + 15 + 8 + 5 = 38.0
        result = compute_engagement_score(10, 5, 2, 1)
        assert result == 38.0


class TestNewsletterEngagement:
    def test_compute_newsletter_score_weights_clicks(self):
        assert compute_newsletter_engagement_score(opens=40, clicks=5) == 55.0

    def test_classifies_high_open_rate_as_resonated(self):
        status = classify_newsletter_engagement(
            opens=45,
            clicks=0,
            subscriber_count=100,
        )
        assert status == NEWSLETTER_STATUS_RESONATED

    def test_classifies_high_click_rate_as_resonated(self):
        status = classify_newsletter_engagement(
            opens=10,
            clicks=4,
            subscriber_count=100,
        )
        assert status == NEWSLETTER_STATUS_RESONATED

    def test_classifies_low_metrics_as_low_resonance(self):
        status = classify_newsletter_engagement(
            opens=10,
            clicks=0,
            subscriber_count=100,
        )
        assert status == NEWSLETTER_STATUS_LOW_RESONANCE

    def test_no_subscriber_denominator_uses_nonzero_engagement(self):
        assert (
            classify_newsletter_engagement(opens=1, clicks=0, subscriber_count=0)
            == NEWSLETTER_STATUS_RESONATED
        )
        assert (
            classify_newsletter_engagement(opens=0, clicks=0, subscriber_count=0)
            == NEWSLETTER_STATUS_LOW_RESONANCE
        )
