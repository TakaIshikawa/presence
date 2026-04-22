"""Tests for logging in XClient exception handlers."""

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.x_client import XClient


def make_x_client():
    """Create an XClient with mocked tweepy.Client."""
    with patch("output.x_client.tweepy.Client") as mock_cls:
        mock_tweepy = MagicMock()
        mock_cls.return_value = mock_tweepy
        client = XClient(
            api_key="key",
            api_secret="secret",
            access_token="token",
            access_token_secret="token_secret",
        )
        return client, mock_tweepy


class TestGetUserIdLogging:
    """Tests for get_user_id() exception logging."""

    def test_logs_debug_on_tweepy_exception(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_user.side_effect = tweepy.TweepyException("User not found")

        with patch("output.x_client.logger") as mock_logger:
            result = client.get_user_id("nonexistent")

            # Verify the method returns None
            assert result is None

            # Verify debug logging was called
            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "Failed to resolve username 'nonexistent'" in call_args
            assert "User not found" in call_args

    def test_includes_username_in_log_message(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_user.side_effect = tweepy.TweepyException("API error")

        with patch("output.x_client.logger") as mock_logger:
            client.get_user_id("test_user_123")

            call_args = mock_logger.debug.call_args[0][0]
            assert "test_user_123" in call_args


class TestGetUserTweetsLogging:
    """Tests for get_user_tweets() exception logging."""

    def test_logs_debug_on_tweepy_exception(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_users_tweets.side_effect = tweepy.TweepyException("Rate limit exceeded")

        with patch("output.x_client.logger") as mock_logger:
            result = client.get_user_tweets("user456", count=10)

            # Verify the method returns empty list
            assert result == []

            # Verify debug logging was called
            mock_logger.debug.assert_called_once()
            call_args = mock_logger.debug.call_args[0][0]
            assert "Failed to fetch timeline for user user456" in call_args
            assert "Rate limit exceeded" in call_args

    def test_includes_user_id_in_log_message(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_users_tweets.side_effect = tweepy.TweepyException("Forbidden")

        with patch("output.x_client.logger") as mock_logger:
            client.get_user_tweets("user789")

            call_args = mock_logger.debug.call_args[0][0]
            assert "user789" in call_args


class TestThreadContextParsing:
    def test_get_user_tweets_includes_reply_and_quote_refs(self):
        client, mock_tweepy = make_x_client()
        tweet = SimpleNamespace(
            id=100,
            text="Replying with a quote",
            created_at=None,
            public_metrics={},
            reply_settings="everyone",
            author_id=200,
            conversation_id=300,
            referenced_tweets=[
                SimpleNamespace(type="replied_to", id=250),
                SimpleNamespace(type="quoted", id=400),
            ],
        )
        mock_tweepy.get_users_tweets.return_value = SimpleNamespace(data=[tweet])

        result = client.get_user_tweets("user123", count=5)

        assert result[0]["conversation_id"] == "300"
        assert result[0]["parent_tweet_id"] == "250"
        assert result[0]["quoted_tweet_id"] == "400"

    def test_context_falls_back_when_parent_fetch_fails(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_tweet.side_effect = tweepy.TweepyException("Forbidden")
        mock_tweepy.search_recent_tweets.return_value = SimpleNamespace(data=None)

        context = client.get_conversation_context(
            tweet_id="100",
            conversation_id="300",
            parent_tweet_id="250",
        )

        assert context == {}
