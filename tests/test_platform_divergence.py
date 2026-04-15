"""Tests for platform divergence analysis module."""

import pytest
from datetime import datetime, timezone, timedelta

from src.storage.db import Database
from src.evaluation.platform_divergence import (
    PlatformDivergenceAnalyzer,
    DivergenceReport,
    DivergenceItem,
    PlatformComparison
)


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    db = Database(":memory:")
    db.connect()
    db.init_schema("schema.sql")
    yield db
    db.close()


@pytest.fixture
def populated_db(test_db):
    """Create database with cross-platform engagement data."""
    now = datetime.now(timezone.utc)

    # Insert some cross-posted content with engagement
    content_items = [
        # Post 1: Bluesky wins significantly
        {
            "content": "Testing cross-platform post 1",
            "type": "x_post",
            "x_score": 5.0,
            "bluesky_score": 15.0,
        },
        # Post 2: X wins slightly
        {
            "content": "Testing cross-platform post 2",
            "type": "x_post",
            "x_score": 10.0,
            "bluesky_score": 8.0,
        },
        # Post 3: Bluesky wins significantly (thread)
        {
            "content": "Testing cross-platform thread 1",
            "type": "x_thread",
            "x_score": 3.0,
            "bluesky_score": 12.0,
        },
        # Post 4: Similar performance
        {
            "content": "Testing cross-platform post 4",
            "type": "x_post",
            "x_score": 7.0,
            "bluesky_score": 7.5,
        },
        # Post 5: X wins significantly
        {
            "content": "Testing cross-platform post 5",
            "type": "x_post",
            "x_score": 20.0,
            "bluesky_score": 5.0,
        },
    ]

    for i, item in enumerate(content_items, 1):
        # Insert content
        content_id = test_db.insert_generated_content(
            content_type=item["type"],
            source_commits=[],
            source_messages=[],
            content=item["content"],
            eval_score=7.0,
            eval_feedback="Test"
        )

        # Mark as published
        test_db.mark_published(
            content_id=content_id,
            url=f"https://x.com/test/status/{i}",
            tweet_id=str(i)
        )
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri=f"at://did:plc:test/app.bsky.feed.post/{i}"
        )

        # Update published_at to be within the time range
        test_db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            ((now - timedelta(days=i)).isoformat(), content_id)
        )

        # Insert X engagement
        test_db.insert_engagement(
            content_id=content_id,
            tweet_id=str(i),
            like_count=int(item["x_score"]),
            retweet_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=item["x_score"]
        )

        # Insert Bluesky engagement
        test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=f"at://did:plc:test/app.bsky.feed.post/{i}",
            like_count=int(item["bluesky_score"]),
            repost_count=0,
            reply_count=0,
            quote_count=0,
            engagement_score=item["bluesky_score"]
        )

    test_db.conn.commit()
    return test_db


class TestDatabaseQuery:
    """Test the database query method."""

    def test_get_cross_platform_engagement_empty(self, test_db):
        """Test query with no cross-posted content."""
        result = test_db.get_cross_platform_engagement(days=60)
        assert result == []

    def test_get_cross_platform_engagement_with_data(self, populated_db):
        """Test query returns correct cross-platform data."""
        result = populated_db.get_cross_platform_engagement(days=60)

        assert len(result) == 5
        assert all('content_id' in item for item in result)
        assert all('content_type' in item for item in result)
        assert all('content_preview' in item for item in result)
        assert all('x_score' in item for item in result)
        assert all('bluesky_score' in item for item in result)

        # Check content preview truncation
        for item in result:
            assert len(item['content_preview']) <= 100

    def test_get_cross_platform_engagement_only_x(self, test_db):
        """Test query excludes X-only posts."""
        # Insert X-only post
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="X only post",
            eval_score=7.0,
            eval_feedback="Test"
        )
        test_db.mark_published(content_id, "https://x.com/test/1", "1")
        test_db.insert_engagement(content_id, "1", 10, 0, 0, 0, 10.0)
        test_db.conn.commit()

        result = test_db.get_cross_platform_engagement(days=60)
        assert len(result) == 0  # Should be excluded (no bluesky_uri)

    def test_get_cross_platform_engagement_only_bluesky(self, test_db):
        """Test query excludes Bluesky-only posts."""
        # Insert Bluesky-only post
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Bluesky only post",
            eval_score=7.0,
            eval_feedback="Test"
        )
        test_db.mark_published_bluesky(content_id, "at://did:plc:test/post/1")
        test_db.insert_bluesky_engagement(
            content_id, "at://did:plc:test/post/1", 10, 0, 0, 0, 10.0
        )
        test_db.conn.commit()

        result = test_db.get_cross_platform_engagement(days=60)
        assert len(result) == 0  # Should be excluded (no tweet_id)


class TestPlatformDivergenceAnalyzer:
    """Test the divergence analyzer."""

    def test_analyze_divergence_empty(self, test_db):
        """Test analyzer with no data."""
        analyzer = PlatformDivergenceAnalyzer(test_db)
        report = analyzer.analyze_divergence(days=60)

        assert isinstance(report, DivergenceReport)
        assert report.total_cross_posted == 0
        assert report.avg_x_score == 0.0
        assert report.avg_bluesky_score == 0.0
        assert report.platform_winner == "tie"
        assert report.high_divergence_items == []
        assert report.content_type_breakdown == {}
        assert report.format_insights == []

    def test_analyze_divergence_with_data(self, populated_db):
        """Test analyzer computes correct metrics."""
        analyzer = PlatformDivergenceAnalyzer(populated_db)
        report = analyzer.analyze_divergence(days=60)

        assert report.total_cross_posted == 5

        # Check average scores
        expected_avg_x = (5.0 + 10.0 + 3.0 + 7.0 + 20.0) / 5  # 9.0
        expected_avg_bluesky = (15.0 + 8.0 + 12.0 + 7.5 + 5.0) / 5  # 9.5

        assert report.avg_x_score == pytest.approx(expected_avg_x, rel=0.01)
        assert report.avg_bluesky_score == pytest.approx(expected_avg_bluesky, rel=0.01)

        # Platform winner should be close (Bluesky slightly ahead)
        assert report.platform_winner in ["bluesky", "tie"]

    def test_high_divergence_detection(self, populated_db):
        """Test detection of high-divergence items."""
        analyzer = PlatformDivergenceAnalyzer(populated_db)
        report = analyzer.analyze_divergence(days=60)

        # Should detect at least 3 high-divergence items (ratio > 2.0):
        # Post 1: 15/5 = 3.0x Bluesky
        # Post 3: 12/3 = 4.0x Bluesky
        # Post 5: 20/5 = 4.0x X
        assert len(report.high_divergence_items) >= 3

        # Check structure
        for item in report.high_divergence_items:
            assert isinstance(item, DivergenceItem)
            assert item.divergence_ratio > 2.0
            assert item.winning_platform in ["x", "bluesky"]

        # Items should be sorted by ratio descending
        ratios = [item.divergence_ratio for item in report.high_divergence_items]
        assert ratios == sorted(ratios, reverse=True)

    def test_content_type_breakdown(self, populated_db):
        """Test content type breakdown grouping."""
        analyzer = PlatformDivergenceAnalyzer(populated_db)
        report = analyzer.analyze_divergence(days=60)

        # Should have both x_post and x_thread
        assert "x_post" in report.content_type_breakdown
        assert "x_thread" in report.content_type_breakdown

        # Check x_post breakdown (4 items)
        x_post = report.content_type_breakdown["x_post"]
        assert isinstance(x_post, PlatformComparison)
        assert x_post.content_type == "x_post"
        assert x_post.count == 4

        # Check x_thread breakdown (1 item)
        x_thread = report.content_type_breakdown["x_thread"]
        assert x_thread.content_type == "x_thread"
        assert x_thread.count == 1

    def test_format_insights_generation(self, populated_db):
        """Test format insights are generated."""
        analyzer = PlatformDivergenceAnalyzer(populated_db)
        report = analyzer.analyze_divergence(days=60)

        assert len(report.format_insights) > 0
        assert all(isinstance(insight, str) for insight in report.format_insights)

        # Should contain some insight about overall platform performance
        insight_text = " ".join(report.format_insights).lower()
        assert any(platform in insight_text for platform in ["bluesky", "x", "platforms"])

    def test_generate_adaptation_context_insufficient_data(self, test_db):
        """Test context returns empty with insufficient data."""
        # Add only 2 items (below threshold of 5)
        for i in range(2):
            content_id = test_db.insert_generated_content(
                content_type="x_post",
                source_commits=[],
                source_messages=[],
                content=f"Test post {i}",
                eval_score=7.0,
                eval_feedback="Test"
            )
            test_db.mark_published(content_id, f"https://x.com/test/{i}", str(i))
            test_db.mark_published_bluesky(content_id, f"at://test/post/{i}")
            test_db.insert_engagement(content_id, str(i), 5, 0, 0, 0, 5.0)
            test_db.insert_bluesky_engagement(content_id, f"at://test/post/{i}", 5, 0, 0, 0, 5.0)

        test_db.conn.commit()

        analyzer = PlatformDivergenceAnalyzer(test_db)
        context = analyzer.generate_adaptation_context(days=60)

        assert context == ""

    def test_generate_adaptation_context_sufficient_data(self, populated_db):
        """Test context generation with sufficient data."""
        analyzer = PlatformDivergenceAnalyzer(populated_db)
        context = analyzer.generate_adaptation_context(days=60)

        assert context != ""
        assert "PLATFORM NOTES:" in context
        assert len(context.split("\n")) > 1  # Should have multiple lines

    def test_divergence_ratio_calculation(self, test_db):
        """Test divergence ratio is calculated correctly."""
        # Create a post with clear 3x divergence
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test",
            eval_score=7.0,
            eval_feedback="Test"
        )
        test_db.mark_published(content_id, "https://x.com/test/1", "1")
        test_db.mark_published_bluesky(content_id, "at://test/post/1")
        test_db.insert_engagement(content_id, "1", 5, 0, 0, 0, 5.0)
        test_db.insert_bluesky_engagement(content_id, "at://test/post/1", 15, 0, 0, 0, 15.0)
        test_db.conn.commit()

        analyzer = PlatformDivergenceAnalyzer(test_db)
        report = analyzer.analyze_divergence(days=60)

        assert len(report.high_divergence_items) == 1
        item = report.high_divergence_items[0]
        assert item.divergence_ratio == pytest.approx(3.0, rel=0.1)
        assert item.winning_platform == "bluesky"

    def test_zero_engagement_handling(self, test_db):
        """Test handling of posts with zero engagement."""
        # Create post with zero engagement on both platforms
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test",
            eval_score=7.0,
            eval_feedback="Test"
        )
        test_db.mark_published(content_id, "https://x.com/test/1", "1")
        test_db.mark_published_bluesky(content_id, "at://test/post/1")
        test_db.insert_engagement(content_id, "1", 0, 0, 0, 0, 0.0)
        test_db.insert_bluesky_engagement(content_id, "at://test/post/1", 0, 0, 0, 0, 0.0)
        test_db.conn.commit()

        analyzer = PlatformDivergenceAnalyzer(test_db)
        report = analyzer.analyze_divergence(days=60)

        # Should not crash and should handle gracefully
        assert report.total_cross_posted == 1
        assert len(report.high_divergence_items) == 0  # Zero on both = skip
