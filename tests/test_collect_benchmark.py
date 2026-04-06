"""Tests for collect_benchmark.py — fetch_following and fetch_account_tweets."""

import sys
import time
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest
import tweepy

# Add scripts/ and src/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from collect_benchmark import fetch_following, fetch_account_tweets


# --- helpers ---


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
