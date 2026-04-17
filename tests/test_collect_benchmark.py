"""Tests for collect_benchmark.py — fetch_following and fetch_account_tweets."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock, Mock

import pytest
import tweepy
import requests

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from collect_benchmark import fetch_following, fetch_account_tweets, get_bearer_token


# --- helpers ---


def _make_mock_response(status_code, json_data=None):
    """Create a mock requests.Response object."""
    mock_resp = Mock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = json_data or {}
    mock_resp.raise_for_status = Mock()
    if status_code >= 400:
        mock_resp.raise_for_status.side_effect = requests.HTTPError(response=mock_resp)
    return mock_resp


def _make_user(user_id="123", username="testuser", name="Test User",
               bio="I build things", followers=5000, following=500, tweets=1000):
    user = MagicMock()
    user.id = int(user_id)
    user.username = username
    user.name = name
    user.description = bio
    user.public_metrics = {
        "followers_count": followers,
        "following_count": following,
        "tweet_count": tweets,
    }
    return user


def _make_tweet(tweet_id="999", text="Hello world", likes=10, rts=2,
                replies=1, quotes=0, created_at="2026-04-06T12:00:00Z"):
    tweet = MagicMock()
    tweet.id = int(tweet_id)
    tweet.text = text
    tweet.public_metrics = {
        "like_count": likes,
        "retweet_count": rts,
        "reply_count": replies,
        "quote_count": quotes,
    }
    tweet.created_at = created_at
    return tweet


def _make_response(data=None, next_token=None):
    resp = MagicMock()
    resp.data = data
    resp.meta = {"next_token": next_token} if next_token else {}
    return resp


# --- TestFetchFollowing ---


class TestFetchFollowing:
    def test_single_page(self):
        client = MagicMock(spec=tweepy.Client)
        client.get_users_following.return_value = _make_response(
            data=[_make_user(), _make_user(user_id="456", username="user2")]
        )

        result = fetch_following(client, "my_id")

        assert len(result) == 2
        assert result[0]["username"] == "testuser"
        assert result[0]["follower_count"] == 5000
        assert result[1]["username"] == "user2"

    def test_pagination(self):
        client = MagicMock(spec=tweepy.Client)
        # First page: 1 user + next_token
        client.get_users_following.side_effect = [
            _make_response(data=[_make_user()], next_token="page2"),
            _make_response(data=[_make_user(user_id="456", username="user2")]),
        ]

        result = fetch_following(client, "my_id")

        assert len(result) == 2
        assert client.get_users_following.call_count == 2

    @patch("collect_benchmark.time.sleep")
    def test_rate_limit_retries(self, mock_sleep):
        client = MagicMock(spec=tweepy.Client)
        client.get_users_following.side_effect = [
            tweepy.TooManyRequests(MagicMock(status_code=429)),
            _make_response(data=[_make_user()]),
        ]

        result = fetch_following(client, "my_id")

        assert len(result) == 1
        mock_sleep.assert_called_once()

    def test_no_data(self):
        client = MagicMock(spec=tweepy.Client)
        client.get_users_following.return_value = _make_response(data=None)

        result = fetch_following(client, "my_id")

        assert result == []


# --- TestGetBearerToken ---


class TestGetBearerToken:
    @patch("collect_benchmark.requests.post")
    def test_success(self, mock_post):
        """Successful bearer token retrieval."""
        mock_post.return_value = _make_mock_response(
            200, {"access_token": "test_bearer_token"}
        )

        token = get_bearer_token("api_key", "api_secret")

        assert token == "test_bearer_token"
        mock_post.assert_called_once()
        # Verify timeout was set
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["timeout"] == 30

    @patch("collect_benchmark.requests.post")
    def test_connection_error(self, mock_post):
        """Network connectivity failure."""
        mock_post.side_effect = requests.ConnectionError("Network unreachable")

        with pytest.raises(RuntimeError) as exc_info:
            get_bearer_token("api_key", "api_secret")

        assert "Failed to connect to Twitter OAuth endpoint" in str(exc_info.value)
        assert "network connectivity" in str(exc_info.value)

    @patch("collect_benchmark.requests.post")
    def test_timeout_error(self, mock_post):
        """Request timeout."""
        mock_post.side_effect = requests.Timeout("Request timed out")

        with pytest.raises(RuntimeError) as exc_info:
            get_bearer_token("api_key", "api_secret")

        assert "timed out after 30 seconds" in str(exc_info.value)

    @patch("collect_benchmark.requests.post")
    def test_http_401_error(self, mock_post):
        """Unauthorized - bad credentials."""
        mock_post.return_value = _make_mock_response(401)

        with pytest.raises(RuntimeError) as exc_info:
            get_bearer_token("api_key", "api_secret")

        assert "Authentication failed (HTTP 401)" in str(exc_info.value)
        assert "API key and secret are correct" in str(exc_info.value)

    @patch("collect_benchmark.requests.post")
    def test_http_403_error(self, mock_post):
        """Forbidden - permission issue."""
        mock_post.return_value = _make_mock_response(403)

        with pytest.raises(RuntimeError) as exc_info:
            get_bearer_token("api_key", "api_secret")

        assert "Authentication failed (HTTP 403)" in str(exc_info.value)
        assert "permissions" in str(exc_info.value)

    @patch("collect_benchmark.requests.post")
    def test_http_500_error(self, mock_post):
        """Server error."""
        mock_post.return_value = _make_mock_response(500)

        with pytest.raises(RuntimeError) as exc_info:
            get_bearer_token("api_key", "api_secret")

        assert "HTTP error 500" in str(exc_info.value)
        assert "temporarily unavailable" in str(exc_info.value)


# --- TestFetchAccountTweets ---


class TestFetchAccountTweets:
    def test_returns_tweets_with_metrics(self):
        client = MagicMock(spec=tweepy.Client)
        client.get_users_tweets.return_value = _make_response(
            data=[_make_tweet(likes=20, rts=5)]
        )

        result = fetch_account_tweets(client, "user_123", max_tweets=10)

        assert len(result) == 1
        assert result[0]["tweet_id"] == "999"
        assert result[0]["like_count"] == 20
        assert result[0]["retweet_count"] == 5
        assert result[0]["engagement_score"] > 0

    def test_pagination_up_to_max_tweets(self):
        client = MagicMock(spec=tweepy.Client)
        # First page: 5 tweets + next_token, second page: 3 tweets
        tweets_page1 = [_make_tweet(tweet_id=str(i)) for i in range(5)]
        tweets_page2 = [_make_tweet(tweet_id=str(i + 5)) for i in range(3)]
        client.get_users_tweets.side_effect = [
            _make_response(data=tweets_page1, next_token="page2"),
            _make_response(data=tweets_page2),
        ]

        result = fetch_account_tweets(client, "user_123", max_tweets=10)

        assert len(result) == 8

    def test_missing_public_metrics(self):
        client = MagicMock(spec=tweepy.Client)
        tweet = _make_tweet()
        tweet.public_metrics = None
        client.get_users_tweets.return_value = _make_response(data=[tweet])

        result = fetch_account_tweets(client, "user_123", max_tweets=10)

        assert len(result) == 1
        assert result[0]["like_count"] == 0
        assert result[0]["retweet_count"] == 0

    def test_api_error_returns_partial(self):
        client = MagicMock(spec=tweepy.Client)
        client.get_users_tweets.side_effect = [
            _make_response(data=[_make_tweet()], next_token="page2"),
            tweepy.TweepyException("API error"),
        ]

        result = fetch_account_tweets(client, "user_123", max_tweets=20)

        assert len(result) == 1  # First page only
