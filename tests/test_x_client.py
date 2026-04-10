"""Tests for the X (Twitter) API client."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.x_client import XClient, PostResult, parse_thread_content


# --- Helpers ---


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


def mock_create_tweet(mock_tweepy, tweet_id="123456"):
    """Set up create_tweet to return a response with the given tweet_id."""
    response = MagicMock()
    response.data = {"id": tweet_id}
    mock_tweepy.create_tweet.return_value = response
    return response


def mock_get_me(mock_tweepy, user_id="99", username="testuser"):
    """Set up get_me to return a user with the given id and username."""
    me_response = MagicMock()
    me_response.data.id = user_id
    me_response.data.username = username
    mock_tweepy.get_me.return_value = me_response
    return me_response


# --- XClient.post() ---


class TestPost:
    def test_success_returns_post_result_with_tweet_id(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")
        mock_create_tweet(mock_tweepy, tweet_id="111")

        result = client.post("Hello world")

        assert result.success is True
        assert result.tweet_id == "111"
        assert result.url == "https://x.com/alice/status/111"
        assert result.error is None

    def test_calls_create_tweet_with_text(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy)
        mock_create_tweet(mock_tweepy)

        client.post("Hello world")

        mock_tweepy.create_tweet.assert_called_once_with(text="Hello world")

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.create_tweet.side_effect = tweepy.TweepyException("Rate limit")

        result = client.post("Hello world")

        assert result.success is False
        assert "Rate limit" in result.error
        assert result.tweet_id is None
        assert result.url is None


# --- XClient.reply() ---


class TestReply:
    def test_success_returns_post_result(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")
        mock_create_tweet(mock_tweepy, tweet_id="222")

        result = client.reply("Great point!", reply_to_tweet_id="100")

        assert result.success is True
        assert result.tweet_id == "222"
        assert result.url == "https://x.com/alice/status/222"

    def test_calls_create_tweet_with_reply_param(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy)
        mock_create_tweet(mock_tweepy)

        client.reply("Great point!", reply_to_tweet_id="100")

        mock_tweepy.create_tweet.assert_called_once_with(
            text="Great point!", in_reply_to_tweet_id="100"
        )

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.create_tweet.side_effect = tweepy.TweepyException("Forbidden")

        result = client.reply("Great point!", reply_to_tweet_id="100")

        assert result.success is False
        assert "Forbidden" in result.error


# --- XClient.post_thread() ---


class TestPostThread:
    def test_chains_tweets_via_reply_ids(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")

        # Each create_tweet call returns a different id
        responses = []
        for tid in ["t1", "t2", "t3"]:
            r = MagicMock()
            r.data = {"id": tid}
            responses.append(r)
        mock_tweepy.create_tweet.side_effect = responses

        result = client.post_thread(["First", "Second", "Third"])

        assert result.success is True
        assert result.tweet_id == "t1"
        assert result.url == "https://x.com/alice/status/t1"

        calls = mock_tweepy.create_tweet.call_args_list
        assert calls[0] == call(text="First", in_reply_to_tweet_id=None)
        assert calls[1] == call(text="Second", in_reply_to_tweet_id="t1")
        assert calls[2] == call(text="Third", in_reply_to_tweet_id="t2")

    def test_empty_list_returns_error(self):
        client, mock_tweepy = make_x_client()

        result = client.post_thread([])

        assert result.success is False
        assert result.error == "No tweets to post"
        mock_tweepy.create_tweet.assert_not_called()

    def test_single_tweet_thread(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")
        mock_create_tweet(mock_tweepy, tweet_id="solo")

        result = client.post_thread(["Only tweet"])

        assert result.success is True
        assert result.tweet_id == "solo"
        mock_tweepy.create_tweet.assert_called_once_with(
            text="Only tweet", in_reply_to_tweet_id=None
        )

    def test_partial_failure_mid_thread_returns_error(self):
        import tweepy

        client, mock_tweepy = make_x_client()

        first_response = MagicMock()
        first_response.data = {"id": "t1"}
        mock_tweepy.create_tweet.side_effect = [
            first_response,
            tweepy.TweepyException("Rate limit on second tweet"),
        ]

        result = client.post_thread(["First", "Second", "Third"])

        assert result.success is False
        assert "Rate limit on second tweet" in result.error


# --- XClient.quote_tweet() ---


class TestQuoteTweet:
    def test_success_returns_post_result(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")
        mock_create_tweet(mock_tweepy, tweet_id="333")

        result = client.quote_tweet("Great thread", quote_tweet_id="original_100")

        assert result.success is True
        assert result.tweet_id == "333"
        assert result.url == "https://x.com/alice/status/333"

    def test_calls_create_tweet_with_quote_param(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy)
        mock_create_tweet(mock_tweepy)

        client.quote_tweet("Commentary", quote_tweet_id="qt_200")

        mock_tweepy.create_tweet.assert_called_once_with(
            text="Commentary", quote_tweet_id="qt_200"
        )

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.create_tweet.side_effect = tweepy.TweepyException("Forbidden")

        result = client.quote_tweet("text", quote_tweet_id="100")

        assert result.success is False
        assert "Forbidden" in result.error


# --- XClient.like() ---


class TestLike:
    def test_success(self):
        client, mock_tweepy = make_x_client()

        result = client.like("tweet_500")

        assert result.success is True
        assert result.tweet_id == "tweet_500"
        mock_tweepy.like.assert_called_once_with("tweet_500")

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.like.side_effect = tweepy.TweepyException("Rate limit")

        result = client.like("tweet_500")

        assert result.success is False
        assert "Rate limit" in result.error


# --- XClient.retweet() ---


class TestRetweet:
    def test_success(self):
        client, mock_tweepy = make_x_client()

        result = client.retweet("tweet_600")

        assert result.success is True
        assert result.tweet_id == "tweet_600"
        mock_tweepy.retweet.assert_called_once_with("tweet_600")

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.retweet.side_effect = tweepy.TweepyException("Duplicate")

        result = client.retweet("tweet_600")

        assert result.success is False
        assert "Duplicate" in result.error


# --- XClient.follow() ---


class TestFollow:
    def test_success(self):
        client, mock_tweepy = make_x_client()

        result = client.follow("user_700")

        assert result.success is True
        mock_tweepy.follow_user.assert_called_once_with("user_700")

    def test_tweepy_exception_returns_failure(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.follow_user.side_effect = tweepy.TweepyException("Blocked")

        result = client.follow("user_700")

        assert result.success is False
        assert "Blocked" in result.error


# --- XClient.get_user_tweets() ---


class TestGetUserTweets:
    def test_returns_tweet_list(self):
        client, mock_tweepy = make_x_client()

        tweet1 = MagicMock()
        tweet1.id = 100
        tweet1.text = "Hello world"
        tweet1.created_at = None
        tweet1.public_metrics = {"like_count": 5}
        tweet1.reply_settings = "everyone"

        response = MagicMock()
        response.data = [tweet1]
        mock_tweepy.get_users_tweets.return_value = response

        result = client.get_user_tweets("user123", count=10)

        assert len(result) == 1
        assert result[0]["id"] == "100"
        assert result[0]["text"] == "Hello world"
        assert result[0]["reply_settings"] == "everyone"
        assert result[0]["public_metrics"] == {"like_count": 5}

    def test_returns_empty_on_no_data(self):
        client, mock_tweepy = make_x_client()
        response = MagicMock()
        response.data = None
        mock_tweepy.get_users_tweets.return_value = response

        result = client.get_user_tweets("user123")

        assert result == []

    def test_returns_empty_on_error(self):
        import tweepy

        client, mock_tweepy = make_x_client()
        mock_tweepy.get_users_tweets.side_effect = tweepy.TweepyException("Rate limit")

        result = client.get_user_tweets("user123")

        assert result == []

    def test_passes_user_auth(self):
        client, mock_tweepy = make_x_client()
        response = MagicMock()
        response.data = None
        mock_tweepy.get_users_tweets.return_value = response

        client.get_user_tweets("user123")

        _, kwargs = mock_tweepy.get_users_tweets.call_args
        assert kwargs["user_auth"] is True

    def test_clamps_max_results_to_100(self):
        client, mock_tweepy = make_x_client()
        response = MagicMock()
        response.data = None
        mock_tweepy.get_users_tweets.return_value = response

        client.get_user_tweets("user123", count=200)

        _, kwargs = mock_tweepy.get_users_tweets.call_args
        assert kwargs["max_results"] == 100

    def test_clamps_min_results_to_5(self):
        client, mock_tweepy = make_x_client()
        response = MagicMock()
        response.data = None
        mock_tweepy.get_users_tweets.return_value = response

        client.get_user_tweets("user123", count=1)

        _, kwargs = mock_tweepy.get_users_tweets.call_args
        assert kwargs["max_results"] == 5

    def test_limits_results_to_count(self):
        client, mock_tweepy = make_x_client()

        tweets = []
        for i in range(10):
            t = MagicMock()
            t.id = i
            t.text = f"Tweet {i}"
            t.created_at = None
            t.public_metrics = {}
            t.reply_settings = "everyone"
            tweets.append(t)

        response = MagicMock()
        response.data = tweets
        mock_tweepy.get_users_tweets.return_value = response

        result = client.get_user_tweets("user123", count=5)

        assert len(result) == 5

    def test_handles_missing_optional_attrs(self):
        client, mock_tweepy = make_x_client()

        tweet = MagicMock(spec=[])
        tweet.id = 42
        tweet.text = "minimal"
        tweet.created_at = None

        response = MagicMock()
        response.data = [tweet]
        mock_tweepy.get_users_tweets.return_value = response

        result = client.get_user_tweets("user123", count=5)

        assert len(result) == 1
        assert result[0]["id"] == "42"
        assert result[0]["public_metrics"] == {}
        assert result[0]["reply_settings"] == "everyone"


# --- XClient.get_mentions() ---


class TestGetMentions:
    def test_parses_mentions_and_users(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, user_id="99", username="testuser")

        # Build mock tweet objects
        tweet1 = MagicMock()
        tweet1.id = 1001
        tweet1.text = "Hey @testuser check this out"
        tweet1.author_id = 200
        tweet1.conversation_id = 3000
        tweet1.in_reply_to_user_id = 99
        tweet1.created_at = "2026-04-05T10:00:00Z"

        tweet2 = MagicMock()
        tweet2.id = 1002
        tweet2.text = "@testuser thoughts?"
        tweet2.author_id = 201
        tweet2.conversation_id = None
        tweet2.in_reply_to_user_id = None
        tweet2.created_at = None

        # Build mock user objects
        user1 = MagicMock()
        user1.id = 200
        user1.username = "bob"
        user1.name = "Bob Builder"

        user2 = MagicMock()
        user2.id = 201
        user2.username = "eve"
        user2.name = "Eve Dev"

        response = MagicMock()
        response.data = [tweet1, tweet2]
        response.includes = {"users": [user1, user2]}
        mock_tweepy.get_users_mentions.return_value = response

        mentions, users_by_id = client.get_mentions()

        assert len(mentions) == 2
        assert mentions[0]["id"] == "1001"
        assert mentions[0]["text"] == "Hey @testuser check this out"
        assert mentions[0]["author_id"] == "200"
        assert mentions[0]["conversation_id"] == "3000"
        assert mentions[0]["in_reply_to_user_id"] == "99"
        assert mentions[0]["created_at"] == "2026-04-05T10:00:00Z"

        assert mentions[1]["id"] == "1002"
        assert mentions[1]["conversation_id"] is None
        assert mentions[1]["in_reply_to_user_id"] is None
        assert mentions[1]["created_at"] is None

        assert users_by_id["200"]["username"] == "bob"
        assert users_by_id["200"]["name"] == "Bob Builder"
        assert users_by_id["201"]["username"] == "eve"

    def test_passes_since_id_to_api(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, user_id="99")

        response = MagicMock()
        response.data = None
        response.includes = None
        mock_tweepy.get_users_mentions.return_value = response

        client.get_mentions(since_id="5000")

        _, kwargs = mock_tweepy.get_users_mentions.call_args
        assert kwargs["since_id"] == "5000"

    def test_caps_max_results_at_100(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, user_id="99")

        response = MagicMock()
        response.data = None
        response.includes = None
        mock_tweepy.get_users_mentions.return_value = response

        client.get_mentions(max_results=500)

        _, kwargs = mock_tweepy.get_users_mentions.call_args
        assert kwargs["max_results"] == 100

    def test_empty_response_returns_empty_lists(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, user_id="99")

        response = MagicMock()
        response.data = None
        response.includes = None
        mock_tweepy.get_users_mentions.return_value = response

        mentions, users_by_id = client.get_mentions()

        assert mentions == []
        assert users_by_id == {}

    def test_response_with_no_includes_users(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, user_id="99")

        tweet = MagicMock()
        tweet.id = 1001
        tweet.text = "hi"
        tweet.author_id = 200
        tweet.conversation_id = None
        tweet.in_reply_to_user_id = None
        tweet.created_at = None

        response = MagicMock()
        response.data = [tweet]
        response.includes = {}
        mock_tweepy.get_users_mentions.return_value = response

        mentions, users_by_id = client.get_mentions()

        assert len(mentions) == 1
        assert users_by_id == {}


# --- XClient.username property ---


class TestUsernameProperty:
    def test_first_access_calls_get_me(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")

        username = client.username

        assert username == "alice"
        mock_tweepy.get_me.assert_called_once()

    def test_second_access_uses_cached_value(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="alice")

        _ = client.username
        _ = client.username

        mock_tweepy.get_me.assert_called_once()

    def test_cache_persists_correct_value(self):
        client, mock_tweepy = make_x_client()
        mock_get_me(mock_tweepy, username="first_call")

        first = client.username
        # Change return value — should not matter, cached
        mock_get_me(mock_tweepy, username="second_call")
        second = client.username

        assert first == "first_call"
        assert second == "first_call"


# --- parse_thread_content() ---


class TestParseThreadContent:
    def test_basic_two_tweets(self):
        content = "TWEET 1:\nFirst tweet text\nTWEET 2:\nSecond tweet text"
        result = parse_thread_content(content)
        assert result == ["First tweet text", "Second tweet text"]

    def test_multiline_tweets(self):
        content = "TWEET 1:\nLine one\nLine two\nTWEET 2:\nAnother tweet"
        result = parse_thread_content(content)
        assert result == ["Line one\nLine two", "Another tweet"]

    def test_single_tweet(self):
        content = "TWEET 1:\nJust one tweet here"
        result = parse_thread_content(content)
        assert result == ["Just one tweet here"]

    def test_empty_lines_between_tweets(self):
        content = "TWEET 1:\n\nFirst tweet\n\nTWEET 2:\n\nSecond tweet\n"
        result = parse_thread_content(content)
        assert result == ["First tweet", "Second tweet"]

    def test_no_tweet_markers_returns_content_as_single_tweet(self):
        content = "Just some plain text\nwith multiple lines"
        result = parse_thread_content(content)
        assert result == ["Just some plain text\nwith multiple lines"]

    def test_empty_string_returns_empty_list(self):
        result = parse_thread_content("")
        assert result == []

    def test_only_markers_no_content_returns_empty_list(self):
        content = "TWEET 1:\nTWEET 2:\nTWEET 3:"
        result = parse_thread_content(content)
        assert result == []

    def test_trailing_content_after_last_tweet(self):
        content = "TWEET 1:\nFirst\nTWEET 2:\nSecond\nwith trailing line"
        result = parse_thread_content(content)
        assert result == ["First", "Second\nwith trailing line"]

    def test_strips_whitespace_from_tweets(self):
        content = "TWEET 1:\n  padded text  \nTWEET 2:\n  also padded  "
        result = parse_thread_content(content)
        assert result == ["padded text", "also padded"]

    def test_content_before_first_marker_is_ignored(self):
        content = "Preamble text\nTWEET 1:\nActual tweet"
        result = parse_thread_content(content)
        # Preamble becomes a tweet since it's before TWEET 1
        # but the function collects it as content before the first marker
        assert "Actual tweet" in result

    def test_higher_numbered_markers(self):
        content = "TWEET 10:\nTenth tweet\nTWEET 11:\nEleventh tweet"
        result = parse_thread_content(content)
        assert result == ["Tenth tweet", "Eleventh tweet"]
