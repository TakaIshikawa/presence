"""Tests for engagement score computation."""

from evaluation.engagement_scorer import compute_engagement_score


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
