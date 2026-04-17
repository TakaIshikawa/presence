"""Tests for XClient.search_tweets() method."""

from unittest.mock import MagicMock, patch

from output.x_client import XClient


class TestSearchTweets:
    def _make_client(self):
        with patch("output.x_client.tweepy.Client") as MockClient:
            client = XClient("k", "s", "t", "ts")
            client._mock_tweepy = MockClient.return_value
            return client

    def test_returns_list_on_success(self):
        client = self._make_client()

        mock_tweet = MagicMock()
        mock_tweet.id = 123
        mock_tweet.text = "Found tweet"
        mock_tweet.created_at = None
        mock_tweet.public_metrics = {"like_count": 5}
        mock_tweet.reply_settings = "everyone"
        mock_tweet.author_id = 999

        mock_user = MagicMock()
        mock_user.id = 999
        mock_user.username = "founduser"

        mock_response = MagicMock()
        mock_response.data = [mock_tweet]
        mock_response.includes = {"users": [mock_user]}

        client._mock_tweepy.search_recent_tweets.return_value = mock_response

        results = client.search_tweets("AI agents", max_results=10)

        assert len(results) == 1
        assert results[0]["id"] == "123"
        assert results[0]["text"] == "Found tweet"
        assert results[0]["author_id"] == "999"
        assert results[0]["author_username"] == "founduser"
        assert results[0]["reply_settings"] == "everyone"

    def test_returns_empty_on_no_data(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.data = None
        client._mock_tweepy.search_recent_tweets.return_value = mock_response

        assert client.search_tweets("query") == []

    def test_returns_empty_on_error(self):
        client = self._make_client()
        import tweepy

        client._mock_tweepy.search_recent_tweets.side_effect = tweepy.TweepyException("fail")

        assert client.search_tweets("query") == []

    def test_clamps_max_results(self):
        client = self._make_client()

        mock_response = MagicMock()
        mock_response.data = None
        client._mock_tweepy.search_recent_tweets.return_value = mock_response

        client.search_tweets("q", max_results=5)  # below 10 floor
        call_kwargs = client._mock_tweepy.search_recent_tweets.call_args[1]
        assert call_kwargs["max_results"] == 10

        client.search_tweets("q", max_results=200)  # above 100 cap
        call_kwargs = client._mock_tweepy.search_recent_tweets.call_args[1]
        assert call_kwargs["max_results"] == 100
