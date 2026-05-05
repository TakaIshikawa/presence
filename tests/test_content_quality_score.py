"""Tests for content quality score calculation."""

import pytest
from datetime import datetime, timedelta, timezone

from engagement.content_quality_score import (
    ContentMetrics,
    ContentQualityScore,
    calculate_content_quality_score,
    _calculate_engagement_rate_score,
    _calculate_reply_depth_score,
    _calculate_freshness_score,
    _calculate_consistency_score,
    _categorize_quality_tier,
    TIER_POOR,
    TIER_BELOW_AVERAGE,
    TIER_AVERAGE,
    TIER_GOOD,
    TIER_EXCELLENT,
    THRESHOLD_BELOW_AVERAGE,
    THRESHOLD_AVERAGE,
    THRESHOLD_GOOD,
    THRESHOLD_EXCELLENT,
    WEIGHT_ENGAGEMENT_RATE,
    WEIGHT_REPLY_DEPTH,
    WEIGHT_FRESHNESS,
    WEIGHT_CONSISTENCY,
)


class TestContentMetrics:
    """Test ContentMetrics dataclass."""

    def test_create_metrics(self):
        """Verify metrics can be created with all fields."""
        now = datetime.now(timezone.utc)
        metrics = ContentMetrics(
            views=1000,
            likes=50,
            replies=10,
            shares=5,
            reply_depth_avg=3.5,
            published_at=now,
            engagement_variance=0.15,
        )
        assert metrics.views == 1000
        assert metrics.likes == 50
        assert metrics.replies == 10
        assert metrics.shares == 5
        assert metrics.reply_depth_avg == 3.5
        assert metrics.published_at == now
        assert metrics.engagement_variance == 0.15

    def test_metrics_frozen(self):
        """Verify metrics are immutable."""
        now = datetime.now(timezone.utc)
        metrics = ContentMetrics(
            views=100,
            likes=10,
            replies=2,
            shares=1,
            reply_depth_avg=2.0,
            published_at=now,
        )
        with pytest.raises(AttributeError):
            metrics.views = 200


class TestWeightConstants:
    """Test weight constants sum to 1.0."""

    def test_weights_sum_to_one(self):
        """Verify component weights sum to 1.0."""
        total = (
            WEIGHT_ENGAGEMENT_RATE
            + WEIGHT_REPLY_DEPTH
            + WEIGHT_FRESHNESS
            + WEIGHT_CONSISTENCY
        )
        assert total == pytest.approx(1.0, abs=0.001)


class TestCalculateEngagementRateScore:
    """Test engagement rate score calculation."""

    def test_zero_views(self):
        """Verify zero views returns zero score."""
        assert _calculate_engagement_rate_score(0, 10, 5, 2) == 0.0

    def test_zero_engagement(self):
        """Verify zero engagement returns zero score."""
        assert _calculate_engagement_rate_score(1000, 0, 0, 0) == 0.0

    def test_typical_engagement(self):
        """Verify typical engagement calculates correctly."""
        # 100 views, 5 likes, 2 replies (x2), 1 share (x3)
        # Total: 5 + 4 + 3 = 12
        # Rate: 12/100 = 0.12 -> normalized against 0.10 max = 1.0 (capped)
        result = _calculate_engagement_rate_score(100, 5, 2, 1)
        assert result > 0.0

    def test_high_engagement_capped(self):
        """Verify very high engagement is capped at 1.0."""
        result = _calculate_engagement_rate_score(100, 50, 50, 50)
        assert result == 1.0

    def test_engagement_weights_applied(self):
        """Verify replies and shares are weighted higher than likes."""
        # Same count but different types - use smaller numbers to avoid capping
        likes_only = _calculate_engagement_rate_score(1000, 5, 0, 0)
        replies_only = _calculate_engagement_rate_score(1000, 0, 5, 0)
        shares_only = _calculate_engagement_rate_score(1000, 0, 0, 5)

        # Shares should be weighted highest, then replies, then likes
        assert shares_only > replies_only > likes_only


class TestCalculateReplyDepthScore:
    """Test reply depth score calculation."""

    def test_zero_depth(self):
        """Verify zero depth returns zero score."""
        assert _calculate_reply_depth_score(0.0) == 0.0

    def test_negative_depth(self):
        """Verify negative depth returns zero score."""
        assert _calculate_reply_depth_score(-1.0) == 0.0

    def test_shallow_depth(self):
        """Verify shallow depth returns low score."""
        result = _calculate_reply_depth_score(1.0)
        assert 0.0 < result < 0.2

    def test_moderate_depth(self):
        """Verify moderate depth returns moderate score."""
        result = _calculate_reply_depth_score(5.0)
        assert 0.4 < result < 0.6

    def test_deep_replies(self):
        """Verify deep replies return high score."""
        result = _calculate_reply_depth_score(9.0)
        assert result > 0.8

    def test_max_depth_capped(self):
        """Verify very deep replies are capped at 1.0."""
        assert _calculate_reply_depth_score(20.0) == 1.0


class TestCalculateFreshnessScore:
    """Test freshness score calculation."""

    def test_just_published(self):
        """Verify just-published content gets maximum freshness."""
        now = datetime.now(timezone.utc)
        result = _calculate_freshness_score(now, now)
        assert result == 1.0

    def test_one_day_old(self):
        """Verify one-day-old content has high freshness."""
        now = datetime.now(timezone.utc)
        published = now - timedelta(days=1)
        result = _calculate_freshness_score(published, now)
        assert 0.9 < result < 1.0

    def test_30_days_old(self):
        """Verify 30-day-old content has moderate freshness."""
        now = datetime.now(timezone.utc)
        published = now - timedelta(days=30)
        result = _calculate_freshness_score(published, now)
        # At decay_days, should be e^-1 ≈ 0.368
        assert 0.3 < result < 0.4

    def test_very_old_content(self):
        """Verify very old content has low freshness."""
        now = datetime.now(timezone.utc)
        published = now - timedelta(days=180)
        result = _calculate_freshness_score(published, now)
        assert result < 0.1

    def test_one_week_old(self):
        """Verify one-week-old content still fairly fresh."""
        now = datetime.now(timezone.utc)
        published = now - timedelta(days=7)
        result = _calculate_freshness_score(published, now)
        assert 0.7 < result < 0.9


class TestCalculateConsistencyScore:
    """Test consistency score calculation."""

    def test_no_variance_data(self):
        """Verify missing variance returns neutral score."""
        assert _calculate_consistency_score(None) == 0.5

    def test_zero_variance(self):
        """Verify zero variance returns perfect score."""
        assert _calculate_consistency_score(0.0) == 1.0

    def test_low_variance(self):
        """Verify low variance returns high score."""
        result = _calculate_consistency_score(0.1)
        assert 0.9 < result < 1.0

    def test_moderate_variance(self):
        """Verify moderate variance returns moderate score."""
        result = _calculate_consistency_score(1.0)
        assert 0.4 < result < 0.6

    def test_high_variance(self):
        """Verify high variance returns low score."""
        result = _calculate_consistency_score(10.0)
        assert result < 0.2


class TestCategorizeQualityTier:
    """Test quality tier categorization."""

    def test_poor_tier(self):
        """Verify scores below 30 are poor."""
        assert _categorize_quality_tier(0.0) == TIER_POOR
        assert _categorize_quality_tier(29.9) == TIER_POOR

    def test_below_average_tier(self):
        """Verify scores 30-49 are below average."""
        assert _categorize_quality_tier(30.0) == TIER_BELOW_AVERAGE
        assert _categorize_quality_tier(40.0) == TIER_BELOW_AVERAGE
        assert _categorize_quality_tier(49.9) == TIER_BELOW_AVERAGE

    def test_average_tier(self):
        """Verify scores 50-69 are average."""
        assert _categorize_quality_tier(50.0) == TIER_AVERAGE
        assert _categorize_quality_tier(60.0) == TIER_AVERAGE
        assert _categorize_quality_tier(69.9) == TIER_AVERAGE

    def test_good_tier(self):
        """Verify scores 70-84 are good."""
        assert _categorize_quality_tier(70.0) == TIER_GOOD
        assert _categorize_quality_tier(80.0) == TIER_GOOD
        assert _categorize_quality_tier(84.9) == TIER_GOOD

    def test_excellent_tier(self):
        """Verify scores 85+ are excellent."""
        assert _categorize_quality_tier(85.0) == TIER_EXCELLENT
        assert _categorize_quality_tier(95.0) == TIER_EXCELLENT
        assert _categorize_quality_tier(100.0) == TIER_EXCELLENT


class TestCalculateContentQualityScore:
    """Test complete content quality score calculation."""

    def test_excellent_content(self):
        """Verify excellent content gets high score."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=1000,
            likes=80,
            replies=30,
            shares=20,
            reply_depth_avg=8.0,
            published_at=now - timedelta(hours=1),
            engagement_variance=0.05,
            now=now,
        )
        assert result.tier in [TIER_GOOD, TIER_EXCELLENT]
        assert result.score >= THRESHOLD_GOOD

    def test_poor_content(self):
        """Verify poor content gets low score."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=1000,
            likes=1,
            replies=0,
            shares=0,
            reply_depth_avg=0.0,
            published_at=now - timedelta(days=90),
            engagement_variance=5.0,
            now=now,
        )
        assert result.tier in [TIER_POOR, TIER_BELOW_AVERAGE]
        assert result.score < THRESHOLD_AVERAGE

    def test_average_content(self):
        """Verify average content gets middle score."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=500,
            likes=15,
            replies=5,
            shares=2,
            reply_depth_avg=3.0,
            published_at=now - timedelta(days=10),
            engagement_variance=0.5,
            now=now,
        )
        assert result.tier in [TIER_BELOW_AVERAGE, TIER_AVERAGE, TIER_GOOD]

    def test_zero_views_content(self):
        """Verify content with no views gets low score."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=0,
            likes=0,
            replies=0,
            shares=0,
            reply_depth_avg=0.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        assert result.score < 50

    def test_metrics_preserved(self):
        """Verify input metrics are preserved in result."""
        now = datetime.now(timezone.utc)
        published = now - timedelta(days=5)
        result = calculate_content_quality_score(
            views=500,
            likes=25,
            replies=10,
            shares=5,
            reply_depth_avg=4.5,
            published_at=published,
            engagement_variance=0.3,
            now=now,
        )
        assert result.metrics.views == 500
        assert result.metrics.likes == 25
        assert result.metrics.replies == 10
        assert result.metrics.shares == 5
        assert result.metrics.reply_depth_avg == 4.5
        assert result.metrics.published_at == published
        assert result.metrics.engagement_variance == 0.3

    def test_component_scores_included(self):
        """Verify component scores are in result."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=10,
            replies=5,
            shares=2,
            reply_depth_avg=3.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        assert "engagement_rate" in result.component_scores
        assert "reply_depth" in result.component_scores
        assert "freshness" in result.component_scores
        assert "consistency" in result.component_scores

    def test_insights_generated(self):
        """Verify insights are generated."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=5,
            replies=2,
            shares=1,
            reply_depth_avg=2.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        assert isinstance(result.insights, list)
        assert len(result.insights) > 0

    def test_result_immutable(self):
        """Verify result is immutable."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=5,
            replies=2,
            shares=1,
            reply_depth_avg=2.0,
            published_at=now,
            now=now,
        )
        with pytest.raises(AttributeError):
            result.score = 99.0

    def test_score_bounded(self):
        """Verify score is always between 0 and 100."""
        now = datetime.now(timezone.utc)
        # Test with extreme values
        result = calculate_content_quality_score(
            views=1000000,
            likes=100000,
            replies=50000,
            shares=25000,
            reply_depth_avg=50.0,
            published_at=now,
            engagement_variance=0.0,
            now=now,
        )
        assert 0.0 <= result.score <= 100.0


class TestContentQualityScoreValidation:
    """Test input validation."""

    def test_negative_views_raises(self):
        """Verify negative views raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="views must be non-negative"):
            calculate_content_quality_score(
                views=-1,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=now,
                now=now,
            )

    def test_negative_likes_raises(self):
        """Verify negative likes raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="likes must be non-negative"):
            calculate_content_quality_score(
                views=100,
                likes=-1,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=now,
                now=now,
            )

    def test_negative_replies_raises(self):
        """Verify negative replies raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="replies must be non-negative"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=-1,
                shares=1,
                reply_depth_avg=2.0,
                published_at=now,
                now=now,
            )

    def test_negative_shares_raises(self):
        """Verify negative shares raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="shares must be non-negative"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=-1,
                reply_depth_avg=2.0,
                published_at=now,
                now=now,
            )

    def test_negative_reply_depth_raises(self):
        """Verify negative reply depth raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="reply_depth_avg must be non-negative"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=-1.0,
                published_at=now,
                now=now,
            )

    def test_negative_variance_raises(self):
        """Verify negative variance raises ValueError."""
        now = datetime.now(timezone.utc)
        with pytest.raises(ValueError, match="engagement_variance must be non-negative"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=now,
                engagement_variance=-0.5,
                now=now,
            )

    def test_naive_published_at_raises(self):
        """Verify naive datetime for published_at raises ValueError."""
        now = datetime.now(timezone.utc)
        naive_dt = datetime.now()  # No timezone
        with pytest.raises(ValueError, match="published_at must be timezone-aware"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=naive_dt,
                now=now,
            )

    def test_naive_now_raises(self):
        """Verify naive datetime for now raises ValueError."""
        published = datetime.now(timezone.utc)
        naive_now = datetime.now()  # No timezone
        with pytest.raises(ValueError, match="now must be timezone-aware"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=published,
                now=naive_now,
            )

    def test_future_published_at_raises(self):
        """Verify future published_at raises ValueError."""
        now = datetime.now(timezone.utc)
        future = now + timedelta(days=1)
        with pytest.raises(ValueError, match="published_at cannot be in the future"):
            calculate_content_quality_score(
                views=100,
                likes=5,
                replies=2,
                shares=1,
                reply_depth_avg=2.0,
                published_at=future,
                now=now,
            )


class TestInsightGeneration:
    """Test insight generation quality."""

    def test_low_engagement_insight(self):
        """Verify low engagement generates appropriate insight."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=1000,
            likes=1,
            replies=0,
            shares=0,
            reply_depth_avg=0.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        assert "engagement" in insights_text or "poor" in insights_text

    def test_high_engagement_insight(self):
        """Verify high engagement generates positive insight."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=50,
            replies=20,
            shares=10,
            reply_depth_avg=8.0,
            published_at=now,
            engagement_variance=0.05,
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        assert "high" in insights_text or "good" in insights_text or "excellent" in insights_text

    def test_old_content_insight(self):
        """Verify old content generates aging insight."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=5,
            replies=2,
            shares=1,
            reply_depth_avg=2.0,
            published_at=now - timedelta(days=60),
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        assert "aging" in insights_text or "old" in insights_text or "recency" in insights_text

    def test_no_views_insight(self):
        """Verify no views generates specific insight."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=0,
            likes=0,
            replies=0,
            shares=0,
            reply_depth_avg=0.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        assert "no views" in insights_text or "not reaching" in insights_text

    def test_inconsistent_engagement_insight(self):
        """Verify inconsistent engagement generates variance insight."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=10,
            replies=5,
            shares=2,
            reply_depth_avg=3.0,
            published_at=now - timedelta(days=5),
            engagement_variance=10.0,  # High variance
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        assert "inconsistent" in insights_text or "variance" in insights_text

    def test_tier_mentioned_in_insights(self):
        """Verify tier is mentioned in insights."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=10,
            replies=5,
            shares=2,
            reply_depth_avg=3.0,
            published_at=now - timedelta(days=1),
            now=now,
        )
        insights_text = " ".join(result.insights).lower()
        # Tier or quality level should be mentioned
        assert any(
            word in insights_text
            for word in ["excellent", "good", "average", "below", "poor", "quality"]
        )


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_exactly_at_threshold_values(self):
        """Verify behavior at exact threshold values."""
        now = datetime.now(timezone.utc)
        # This is mostly to ensure no crashes at boundaries
        result = calculate_content_quality_score(
            views=100,
            likes=10,
            replies=10,
            shares=10,
            reply_depth_avg=10.0,
            published_at=now - timedelta(days=30),
            engagement_variance=1.0,
            now=now,
        )
        assert 0 <= result.score <= 100

    def test_all_zeros(self):
        """Verify all-zero metrics produces valid result."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=0,
            likes=0,
            replies=0,
            shares=0,
            reply_depth_avg=0.0,
            published_at=now,
            now=now,
        )
        assert result.score >= 0
        assert result.tier in [TIER_POOR, TIER_BELOW_AVERAGE, TIER_AVERAGE]

    def test_missing_variance_uses_default(self):
        """Verify missing variance uses neutral default."""
        now = datetime.now(timezone.utc)
        result = calculate_content_quality_score(
            views=100,
            likes=10,
            replies=5,
            shares=2,
            reply_depth_avg=3.0,
            published_at=now,
            engagement_variance=None,
            now=now,
        )
        assert result.component_scores["consistency"] == 50.0  # 0.5 * 100
