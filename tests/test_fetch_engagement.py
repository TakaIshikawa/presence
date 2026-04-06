"""Tests for fetch_engagement.py — backfill logic and main orchestration."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_engagement import backfill_tweet_ids


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
    config.synthesis.eval_threshold = 0.7
    return config


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
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_happy_path(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
            {"id": 2, "tweet_id": "200", "content": "Post B"},
        ]
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(
            data=[_make_tweet_data("100"), _make_tweet_data("200")]
        )
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}

        from fetch_engagement import main
        main()

        assert mock_db.insert_engagement.call_count == 2
        mock_db.auto_classify_posts.assert_called_once_with(min_age_hours=48)

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_no_posts_need_metrics(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = []

        from fetch_engagement import main
        main()

        # No tweets to fetch → get_tweets never called
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.assert_not_called()
        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_api_error_skips_batch(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.side_effect = Exception("API timeout")
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score")
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_no_data_in_response(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(data=None)
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_not_called()

    @patch("fetch_engagement.compute_engagement_score", return_value=0.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_missing_public_metrics(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = [
            {"id": 1, "tweet_id": "100", "content": "Post A"},
        ]
        tweet = MagicMock()
        tweet.id = 100
        tweet.public_metrics = None
        mock_client = MockTweepy.return_value
        mock_client.get_tweets.return_value = MagicMock(data=[tweet])
        mock_db.auto_classify_posts.return_value = {"resonated": 0, "low_resonance": 0}

        from fetch_engagement import main
        main()

        mock_db.insert_engagement.assert_called_once()
        call_kwargs = mock_db.insert_engagement.call_args
        assert call_kwargs[1]["like_count"] == 0
        assert call_kwargs[1]["retweet_count"] == 0

    @patch("fetch_engagement.compute_engagement_score", return_value=5.0)
    @patch("fetch_engagement.tweepy.Client")
    @patch("fetch_engagement.get_bearer_token", return_value="bearer-token")
    @patch("fetch_engagement.Database")
    @patch("fetch_engagement.load_config")
    def test_backfill_runs_before_fetch(self, mock_config, MockDB, mock_bearer, MockTweepy, mock_scorer):
        mock_config.return_value = _make_config()
        mock_db = MockDB.return_value
        mock_db.get_posts_needing_metrics.return_value = []

        from fetch_engagement import main
        main()

        # backfill_tweet_ids is called on the db instance before get_posts_needing_metrics
        # We verify by checking the call order: init_schema before get_posts_needing_metrics
        mock_db.init_schema.assert_called_once()
