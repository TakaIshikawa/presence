"""Tests for fetch_engagement.py — backfill logic and main orchestration."""

import sys
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
import tweepy

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_engagement import (
    backfill_tweet_ids,
    fetch_bluesky_profile_metrics,
    get_bearer_token,
)


# --- helpers ---


def _seed_published_post(db, tweet_id=None, url=None):
    """Insert a published post and return its ID."""
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha1"],
        source_messages=["uuid1"],
        content="Test post about AI",
        eval_score=8.0,
        eval_feedback="Good",
    )
    if url or tweet_id:
        db.mark_published(content_id, url or "", tweet_id=tweet_id)
    return content_id


def _make_config():
    config = MagicMock()
    config.paths.database = ":memory:"
    config.x.api_key = "key"
    config.x.api_secret = "secret"
    config.x.access_token = "at"
    config.x.access_token_secret = "ats"
    config.bluesky.enabled = False
    config.bluesky.handle = "test.bsky.social"
    config.bluesky.app_password = "app-password"
    config.synthesis.eval_threshold = 0.7
    return config


def _mock_script_context(config, db):
    """Create a mock context manager that yields (config, db)."""
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


# --- TestBackfillTweetIds ---


class TestBackfillTweetIds:
    def test_backfills_from_status_url(self, db):
        _seed_published_post(
            db, url="https://x.com/user/status/12345"
        )
        count = backfill_tweet_ids(db)
        assert count == 1
        row = db.conn.execute(
            "SELECT tweet_id FROM generated_content WHERE tweet_id IS NOT NULL"
        ).fetchone()
        assert row["tweet_id"] == "12345"

    def test_skips_post_with_existing_tweet_id(self, db):
        _seed_published_post(
            db, tweet_id="existing_id", url="https://x.com/user/status/99999"
        )
        count = backfill_tweet_ids(db)
        assert count == 0

    def test_skips_non_status_url(self, db):
        _seed_published_post(db, url="https://example.com/article")
        count = backfill_tweet_ids(db)
        assert count == 0

    def test_multiple_posts(self, db):
        _seed_published_post(db, url="https://x.com/user/status/111")
        _seed_published_post(db, url="https://x.com/user/status/222")
        _seed_published_post(db, tweet_id="333", url="https://x.com/user/status/333")
        count = backfill_tweet_ids(db)
        assert count == 2


# --- TestMain ---


def _make_tweet_data(tweet_id, likes=5, rts=2, replies=1, quotes=0):
    """Build a mock tweet object matching tweepy response format."""
    tweet = MagicMock()
    tweet.id = int(tweet_id)
    tweet.public_metrics = {
        "like_count": likes,
        "retweet_count": rts,
        "reply_count": replies,
        "quote_count": quotes,
    }
    return tweet


class TestMain:
    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_happy_path(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
            {"id": 2, "tweet_id": "200", "content": "Post B"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(
            data=[_make_tweet_data("100"), _make_tweet_data("200")]
        )

        from fetch_engagement import main
        main()

        assert mock_db.insert_engagement.call_count == 2
        mock_db.auto_classify_posts.assert_called_once_with(min_age_hours=48)

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_no_posts_need_metrics(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = []
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        from fetch_engagement import main
        main()

        # No tweets to fetch → get_tweets never called
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.assert_not_called()
        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_api_error_skips_batch(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        mock_client.get_tweets.side_effect = tweepy.TweepyException("API timeout")

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_tweepy_exception_logs_and_continues(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer, caplog):
        """Test that TweepyException during batch fetch logs error and continues to next batch."""
        import logging
        caplog.set_level(logging.ERROR)

        config = _make_config()
        mock_db = MagicMock()
        # Create 150 posts to trigger 2 batches (100 + 50)
        posts = [{"id": i, "tweet_id": str(1000 + i), "content": f"Post {i}"} for i in range(150)]
        mock_db.get_posts_needing_metrics.return_value = posts
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        # First batch fails with TweepyException, second batch succeeds
        second_batch_tweets = [_make_tweet_data(str(1100 + i)) for i in range(50)]
        mock_client.get_tweets.side_effect = [
            tweepy.TweepyException("Rate limit exceeded"),
            MagicMock(data=second_batch_tweets)
        ]

        from fetch_engagement import main
        main()

        # Verify error was logged
        assert "API error fetching batch 1" in caplog.text
        assert "Rate limit exceeded" in caplog.text

        # Verify second batch was still attempted and processed
        assert mock_client.get_tweets.call_count == 2
        # Only second batch (50 tweets) should be recorded
        assert mock_db.insert_engagement.call_count == 50

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_no_data_in_response(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(data=None)

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score", return_value=0.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_missing_public_metrics(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        tweet = MagicMock()
        tweet.id = 100
        tweet.public_metrics = None
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(data=[tweet])

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_called_once()
        call_kwargs = mock_db.insert_engagement.call_args
        assert call_kwargs[1]["like_count"] == 0
        assert call_kwargs[1]["retweet_count"] == 0

    @patch("fetch_engagement.compute_engagement_score", return_value=5.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_backfill_runs_before_fetch(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = []
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        from fetch_engagement import main
        main()

        # backfill_tweet_ids is called on the db instance before get_posts_needing_metrics
        # We verify by checking that get_posts_needing_metrics was called
        mock_db.get_posts_needing_metrics.assert_called_once()

    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_batching_with_more_than_batch_size_posts(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        """Test that posts > BATCH_SIZE (100) are fetched in multiple batches."""
        config = _make_config()
        mock_db = MagicMock()

        # Create 150 posts (should trigger 2 batches: 100 + 50)
        posts = [{"id": i, "tweet_id": str(1000 + i), "content": f"Post {i}"} for i in range(150)]
        mock_db.get_posts_needing_metrics.return_value = posts
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        # Mock responses for both batches
        first_batch_tweets = [_make_tweet_data(str(1000 + i)) for i in range(100)]
        second_batch_tweets = [_make_tweet_data(str(1100 + i)) for i in range(50)]
        mock_client.get_tweets.side_effect = [
            MagicMock(data=first_batch_tweets),
            MagicMock(data=second_batch_tweets),
        ]

        from fetch_engagement import main
        main()

        # Verify get_tweets was called exactly twice
        assert mock_client.get_tweets.call_count == 2

        # Verify first call had 100 tweet IDs
        first_call_ids = mock_client.get_tweets.call_args_list[0][1]["ids"]
        assert len(first_call_ids) == 100

        # Verify second call had 50 tweet IDs
        second_call_ids = mock_client.get_tweets.call_args_list[1][1]["ids"]
        assert len(second_call_ids) == 50

        # Verify all 150 engagements were recorded
        assert mock_db.insert_engagement.call_count == 150

    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_partial_batch_failure(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        """Test that first batch succeeds but second batch fails — first batch data is still recorded."""
        config = _make_config()
        mock_db = MagicMock()

        # Create 150 posts
        posts = [{"id": i, "tweet_id": str(1000 + i), "content": f"Post {i}"} for i in range(150)]
        mock_db.get_posts_needing_metrics.return_value = posts
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        # First batch succeeds, second batch fails
        first_batch_tweets = [_make_tweet_data(str(1000 + i)) for i in range(100)]
        mock_client.get_tweets.side_effect = [
            MagicMock(data=first_batch_tweets),
            tweepy.TweepyException("API rate limit exceeded"),
        ]

        from fetch_engagement import main
        main()

        # Verify get_tweets was called twice (second call failed)
        assert mock_client.get_tweets.call_count == 2

        # Verify only first batch (100 engagements) were recorded
        assert mock_db.insert_engagement.call_count == 100

    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_auto_classify_output_formatting(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer, caplog):
        """Test that auto_classify_posts output is correctly formatted and printed."""
        import logging
        caplog.set_level(logging.INFO)

        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 2, "low_resonance": 1}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(data=[_make_tweet_data("100")])

        from fetch_engagement import main
        main()

        assert "Auto-classified: 2 resonated, 1 low_resonance" in caplog.text

    @patch("fetch_engagement.compute_engagement_score", return_value=10.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.script_context")
    def test_tweet_id_not_in_mapping_skipped(self, mock_ctx, mock_bearer, MockTweepy, mock_scorer):
        """Test that response tweets not in tweet_id_to_post mapping are silently skipped."""
        config = _make_config()
        mock_db = MagicMock()
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}
        mock_ctx.return_value = _mock_script_context(config, mock_db)()

        mock_client = MockTweepy.return_value
        # API returns our tweet plus an extra tweet not in our mapping
        mock_client.get_tweets.return_value = MagicMock(
            data=[_make_tweet_data("100"), _make_tweet_data("999")]
        )

        from fetch_engagement import main
        main()

        # Only the tweet in our mapping (100) should be recorded
        assert mock_db.insert_engagement.call_count == 1
        call_kwargs = mock_db.insert_engagement.call_args[1]
        assert call_kwargs["tweet_id"] == "100"


# --- TestFetchBlueskyProfileMetrics ---


class TestFetchBlueskyProfileMetrics:
    @patch("output.bluesky_client.BlueskyClient")
    def test_stores_bluesky_profile_snapshot(self, MockBlueskyClient, db):
        config = _make_config()
        config.bluesky.enabled = True
        MockBlueskyClient.return_value.get_profile_metrics.return_value = {
            "follower_count": 42,
            "following_count": 12,
            "tweet_count": 88,
            "listed_count": None,
        }

        stored = fetch_bluesky_profile_metrics(config, db)

        assert stored is True
        latest = db.get_latest_profile_metrics("bluesky")
        assert latest["follower_count"] == 42
        assert latest["following_count"] == 12
        assert latest["tweet_count"] == 88
        assert latest["listed_count"] is None
        MockBlueskyClient.assert_called_once_with(
            handle="test.bsky.social",
            app_password="app-password",
        )

    @patch("output.bluesky_client.BlueskyClient")
    def test_skips_when_bluesky_disabled(self, MockBlueskyClient, db):
        config = _make_config()
        config.bluesky.enabled = False

        stored = fetch_bluesky_profile_metrics(config, db)

        assert stored is False
        assert db.get_latest_profile_metrics("bluesky") is None
        MockBlueskyClient.assert_not_called()

    @patch("output.bluesky_client.BlueskyClient")
    def test_skips_when_profile_metrics_unavailable(self, MockBlueskyClient, db):
        config = _make_config()
        config.bluesky.enabled = True
        MockBlueskyClient.return_value.get_profile_metrics.return_value = None

        stored = fetch_bluesky_profile_metrics(config, db)

        assert stored is False
        assert db.get_latest_profile_metrics("bluesky") is None


# --- TestGetBearerToken ---


class TestGetBearerToken:
    @patch("requests.post")
    def test_successful_token_exchange(self, mock_post):
        """Test successful OAuth 2.0 bearer token exchange."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "bearer-xyz"}
        mock_post.return_value = mock_response

        token = get_bearer_token("test_key", "test_secret")

        assert token == "bearer-xyz"
        mock_post.assert_called_once()

        # Verify the request details
        call_args = mock_post.call_args
        assert call_args[0][0] == "https://api.twitter.com/oauth2/token"

        headers = call_args[1]["headers"]
        assert "Authorization" in headers
        assert headers["Authorization"].startswith("Basic ")
        assert headers["Content-Type"] == "application/x-www-form-urlencoded;charset=UTF-8"

        assert call_args[1]["data"] == "grant_type=client_credentials"
        mock_response.raise_for_status.assert_called_once()

    @patch("requests.post")
    def test_http_error_propagates(self, mock_post):
        """Test that HTTP error from raise_for_status() propagates."""
        import requests

        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        mock_post.return_value = mock_response

        with pytest.raises(requests.HTTPError, match="401 Unauthorized"):
            get_bearer_token("bad_key", "bad_secret")

    @patch("requests.post")
    def test_network_failure_propagates(self, mock_post):
        """Test that network connection errors propagate."""
        import requests

        mock_post.side_effect = requests.ConnectionError("Network unreachable")

        with pytest.raises(requests.ConnectionError, match="Network unreachable"):
            get_bearer_token("test_key", "test_secret")

    @patch("requests.post")
    def test_authorization_header_base64_encoding(self, mock_post):
        """Test that Authorization header contains correctly Base64-encoded credentials."""
        import base64

        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "bearer-xyz"}
        mock_post.return_value = mock_response

        api_key = "my_api_key"
        api_secret = "my_api_secret"
        get_bearer_token(api_key, api_secret)

        call_args = mock_post.call_args
        auth_header = call_args[1]["headers"]["Authorization"]

        # Extract the Base64 part after "Basic "
        base64_part = auth_header.split("Basic ")[1]
        decoded = base64.b64decode(base64_part).decode()

        assert decoded == f"{api_key}:{api_secret}"
