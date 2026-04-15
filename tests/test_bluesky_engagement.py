"""Tests for Bluesky engagement tracking system."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timezone, timedelta

from src.output.bluesky_client import BlueskyClient
from src.storage.db import Database
from src.evaluation.pipeline_analytics import PipelineAnalytics, CrossPlatformReport
from src.evaluation.engagement_scorer import compute_engagement_score


@pytest.fixture
def mock_atproto_client():
    """Mock atproto Client for testing."""
    with patch('src.output.bluesky_client.Client') as mock_client:
        yield mock_client


@pytest.fixture
def bluesky_client(mock_atproto_client):
    """Create BlueskyClient with mocked atproto."""
    return BlueskyClient(handle="test.bsky.social", app_password="test-password")


@pytest.fixture
def test_db():
    """Create in-memory test database."""
    db = Database(":memory:")
    db.connect()
    db.init_schema("schema.sql")
    yield db
    db.close()


class TestBlueskyClientMetrics:
    """Test BlueskyClient metrics fetching methods."""

    def test_get_post_metrics_success(self, bluesky_client, mock_atproto_client):
        """Test successful metrics fetch."""
        # Mock the get_post_thread response
        mock_post = Mock()
        mock_post.like_count = 10
        mock_post.repost_count = 5
        mock_post.reply_count = 3
        mock_post.quote_count = 2

        mock_thread = Mock()
        mock_thread.post = mock_post

        mock_response = Mock()
        mock_response.thread = mock_thread

        bluesky_client.client.get_post_thread = Mock(return_value=mock_response)

        # Fetch metrics
        uri = "at://did:plc:test/app.bsky.feed.post/test123"
        metrics = bluesky_client.get_post_metrics(uri)

        assert metrics is not None
        assert metrics['like_count'] == 10
        assert metrics['repost_count'] == 5
        assert metrics['reply_count'] == 3
        assert metrics['quote_count'] == 2

    def test_get_post_metrics_not_found(self, bluesky_client):
        """Test metrics fetch for non-existent post."""
        bluesky_client.client.get_post_thread = Mock(side_effect=Exception("Not found"))

        uri = "at://did:plc:test/app.bsky.feed.post/notfound"
        metrics = bluesky_client.get_post_metrics(uri)

        assert metrics is None

    def test_get_post_metrics_batch(self, bluesky_client, mock_atproto_client):
        """Test batch metrics fetch with rate limiting."""
        # Mock two posts
        def mock_get_thread(uri):
            mock_post = Mock()
            if "post1" in uri:
                mock_post.like_count = 10
                mock_post.repost_count = 5
                mock_post.reply_count = 3
                mock_post.quote_count = 2
            else:
                mock_post.like_count = 20
                mock_post.repost_count = 10
                mock_post.reply_count = 6
                mock_post.quote_count = 4

            mock_thread = Mock()
            mock_thread.post = mock_post
            mock_response = Mock()
            mock_response.thread = mock_thread
            return mock_response

        bluesky_client.client.get_post_thread = Mock(side_effect=mock_get_thread)

        uris = [
            "at://did:plc:test/app.bsky.feed.post/post1",
            "at://did:plc:test/app.bsky.feed.post/post2",
        ]

        with patch('time.sleep') as mock_sleep:
            metrics_list = bluesky_client.get_post_metrics_batch(uris)

            # Verify rate limiting was applied
            assert mock_sleep.call_count == 1

        assert len(metrics_list) == 2
        assert metrics_list[0]['like_count'] == 10
        assert metrics_list[1]['like_count'] == 20


class TestDatabaseBlueskyEngagement:
    """Test Database methods for Bluesky engagement."""

    def test_insert_bluesky_engagement(self, test_db):
        """Test inserting Bluesky engagement metrics."""
        # Create a test content item
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test post",
            eval_score=8.0,
            eval_feedback="Good"
        )

        # Mark as published with Bluesky URI
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri="at://did:plc:test/app.bsky.feed.post/test123"
        )

        # Insert engagement metrics
        score = compute_engagement_score(
            like_count=10,
            repost_count=5,
            reply_count=3,
            quote_count=2
        )

        engagement_id = test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri="at://did:plc:test/app.bsky.feed.post/test123",
            like_count=10,
            repost_count=5,
            reply_count=3,
            quote_count=2,
            engagement_score=score
        )

        assert engagement_id > 0

        # Verify the metrics were stored
        metrics = test_db.get_bluesky_engagement(content_id)
        assert len(metrics) == 1
        assert metrics[0]['like_count'] == 10
        assert metrics[0]['repost_count'] == 5
        assert metrics[0]['reply_count'] == 3
        assert metrics[0]['quote_count'] == 2
        assert metrics[0]['engagement_score'] == score

    def test_get_content_needing_bluesky_engagement(self, test_db):
        """Test querying content that needs Bluesky engagement fetch."""
        # Create content with Bluesky URI
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test post",
            eval_score=8.0,
            eval_feedback="Good"
        )

        test_db.mark_published(content_id, "https://x.com/test/status/123", "123")
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri="at://did:plc:test/app.bsky.feed.post/test123"
        )

        # Should need engagement fetch (no metrics yet)
        content = test_db.get_content_needing_bluesky_engagement(max_age_days=7)
        assert len(content) == 1
        assert content[0]['id'] == content_id

        # Insert engagement
        test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri="at://did:plc:test/app.bsky.feed.post/test123",
            like_count=10,
            repost_count=5,
            reply_count=3,
            quote_count=2,
            engagement_score=35.0
        )

        # Should not need fetch (recently fetched)
        content = test_db.get_content_needing_bluesky_engagement(max_age_days=7)
        assert len(content) == 0

    def test_get_combined_engagement(self, test_db):
        """Test unified engagement view combining X and Bluesky."""
        # Create content
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test post",
            eval_score=8.0,
            eval_feedback="Good"
        )

        test_db.mark_published(content_id, "https://x.com/test/status/123", "123")
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri="at://did:plc:test/app.bsky.feed.post/test123"
        )

        # Insert X engagement
        x_score = compute_engagement_score(15, 8, 4, 3)
        test_db.insert_engagement(
            content_id=content_id,
            tweet_id="123",
            like_count=15,
            retweet_count=8,
            reply_count=4,
            quote_count=3,
            engagement_score=x_score
        )

        # Insert Bluesky engagement
        bsky_score = compute_engagement_score(10, 5, 3, 2)
        test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri="at://did:plc:test/app.bsky.feed.post/test123",
            like_count=10,
            repost_count=5,
            reply_count=3,
            quote_count=2,
            engagement_score=bsky_score
        )

        # Get combined engagement
        combined = test_db.get_combined_engagement(content_id)

        assert combined['content_id'] == content_id
        assert combined['x_engagement'] is not None
        assert combined['x_engagement']['like_count'] == 15
        assert combined['bluesky_engagement'] is not None
        assert combined['bluesky_engagement']['like_count'] == 10
        assert combined['combined_score'] == x_score + bsky_score


class TestCrossPlatformAnalytics:
    """Test cross-platform comparison analytics."""

    def test_cross_platform_comparison(self, test_db):
        """Test cross-platform engagement comparison."""
        analytics = PipelineAnalytics(test_db)

        # Create content posted to both platforms
        for i in range(3):
            content_id = test_db.insert_generated_content(
                content_type="x_post",
                source_commits=[],
                source_messages=[],
                content=f"Test post {i}",
                eval_score=8.0,
                eval_feedback="Good"
            )

            test_db.mark_published(
                content_id,
                f"https://x.com/test/status/{i}",
                str(i)
            )
            test_db.mark_published_bluesky(
                content_id,
                f"at://did:plc:test/app.bsky.feed.post/test{i}"
            )

            # Insert X engagement (higher on X)
            x_score = compute_engagement_score(20 + i*5, 10, 5, 3)
            test_db.insert_engagement(
                content_id=content_id,
                tweet_id=str(i),
                like_count=20 + i*5,
                retweet_count=10,
                reply_count=5,
                quote_count=3,
                engagement_score=x_score
            )

            # Insert Bluesky engagement (lower on Bluesky)
            bsky_score = compute_engagement_score(10 + i*3, 5, 2, 1)
            test_db.insert_bluesky_engagement(
                content_id=content_id,
                bluesky_uri=f"at://did:plc:test/app.bsky.feed.post/test{i}",
                like_count=10 + i*3,
                repost_count=5,
                reply_count=2,
                quote_count=1,
                engagement_score=bsky_score
            )

        # Get cross-platform comparison
        report = analytics.cross_platform_comparison(days=30)

        assert isinstance(report, CrossPlatformReport)
        assert report.both_count == 3
        assert report.x_only_count == 0
        assert report.bluesky_only_count == 0
        assert report.avg_x_score > report.avg_bluesky_score
        assert report.correlation is not None  # Should have correlation with 3+ samples

    def test_cross_platform_x_only(self, test_db):
        """Test cross-platform comparison with X-only content."""
        analytics = PipelineAnalytics(test_db)

        # Create X-only content
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="X only post",
            eval_score=8.0,
            eval_feedback="Good"
        )

        test_db.mark_published(content_id, "https://x.com/test/status/1", "1")

        test_db.insert_engagement(
            content_id=content_id,
            tweet_id="1",
            like_count=20,
            retweet_count=10,
            reply_count=5,
            quote_count=3,
            engagement_score=75.0
        )

        report = analytics.cross_platform_comparison(days=30)

        assert report.x_only_count == 1
        assert report.both_count == 0
        assert report.bluesky_only_count == 0


class TestFetchEngagementIntegration:
    """Test fetch_engagement.py script integration."""

    def test_bluesky_fetch_flow(self, test_db):
        """Test the Bluesky engagement fetch flow."""
        # Create content with Bluesky URI
        content_id = test_db.insert_generated_content(
            content_type="x_post",
            source_commits=[],
            source_messages=[],
            content="Test post",
            eval_score=8.0,
            eval_feedback="Good"
        )

        test_db.mark_published(content_id, "https://x.com/test/status/123", "123")
        test_db.mark_published_bluesky(
            content_id=content_id,
            uri="at://did:plc:test/app.bsky.feed.post/test123"
        )

        # Simulate fetch
        posts = test_db.get_content_needing_bluesky_engagement(max_age_days=7)
        assert len(posts) == 1

        # Simulate inserting fetched metrics
        metrics = {
            'like_count': 10,
            'repost_count': 5,
            'reply_count': 3,
            'quote_count': 2
        }

        score = compute_engagement_score(
            metrics['like_count'],
            metrics['repost_count'],
            metrics['reply_count'],
            metrics['quote_count']
        )

        test_db.insert_bluesky_engagement(
            content_id=content_id,
            bluesky_uri=posts[0]['bluesky_uri'],
            like_count=metrics['like_count'],
            repost_count=metrics['repost_count'],
            reply_count=metrics['reply_count'],
            quote_count=metrics['quote_count'],
            engagement_score=score
        )

        # Verify metrics were stored
        engagement = test_db.get_bluesky_engagement(content_id)
        assert len(engagement) == 1
        assert engagement[0]['engagement_score'] == score
