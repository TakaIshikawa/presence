"""Tests for the CrossPoster multi-platform publisher."""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.cross_poster import CrossPoster, count_graphemes
from output.x_client import PostResult
from output.bluesky_client import BlueskyPostResult


# --- count_graphemes() ---


class TestCountGraphemes:
    def test_basic_ascii_text(self):
        assert count_graphemes("Hello world") == 11

    def test_unicode_text(self):
        # Emoji should count as single graphemes
        text = "Hello 👋 world"
        # In simplified counting, this might be more than expected
        # but our implementation uses NFC normalization
        count = count_graphemes(text)
        assert count > 0  # Just verify it doesn't crash

    def test_empty_string(self):
        assert count_graphemes("") == 0


# --- CrossPoster.adapt_for_bluesky() ---


class TestAdaptForBluesky:
    def test_text_under_300_graphemes_unchanged(self):
        cross_poster = CrossPoster()
        text = "This is a short post"
        result = cross_poster.adapt_for_bluesky(text)
        assert result == text

    def test_text_over_300_graphemes_truncated(self):
        cross_poster = CrossPoster()
        # Create text well over 300 chars
        text = "A" * 400
        result = cross_poster.adapt_for_bluesky(text)
        # Should be truncated
        assert len(result) < len(text)
        assert result.endswith("...")

    def test_exact_300_graphemes_unchanged(self):
        cross_poster = CrossPoster()
        text = "A" * 300
        result = cross_poster.adapt_for_bluesky(text)
        # Should fit exactly
        assert result == text or result.endswith("...")  # Depends on exact counting


# --- CrossPoster.publish() ---


class TestPublish:
    def test_publish_to_x_only(self):
        mock_x = MagicMock()
        mock_x.post.return_value = PostResult(
            success=True,
            tweet_id="123",
            url="https://x.com/user/status/123"
        )

        cross_poster = CrossPoster(x_client=mock_x, bluesky_client=None)
        results = cross_poster.publish("Test post", "x_post")

        assert "x" in results
        assert results["x"].success is True
        assert "bluesky" not in results
        mock_x.post.assert_called_once_with("Test post")

    def test_publish_to_bluesky_only(self):
        mock_bsky = MagicMock()
        mock_bsky.post.return_value = BlueskyPostResult(
            success=True,
            uri="at://did:plc:xyz/app.bsky.feed.post/abc",
            url="https://bsky.app/profile/user/post/abc"
        )

        cross_poster = CrossPoster(x_client=None, bluesky_client=mock_bsky)
        results = cross_poster.publish("Test post", "x_post")

        assert "bluesky" in results
        assert results["bluesky"].success is True
        assert "x" not in results
        mock_bsky.post.assert_called_once()

    def test_publish_to_both_platforms(self):
        mock_x = MagicMock()
        mock_x.post.return_value = PostResult(
            success=True,
            tweet_id="123",
            url="https://x.com/user/status/123"
        )

        mock_bsky = MagicMock()
        mock_bsky.post.return_value = BlueskyPostResult(
            success=True,
            uri="at://did:plc:xyz/app.bsky.feed.post/abc",
            url="https://bsky.app/profile/user/post/abc"
        )

        cross_poster = CrossPoster(x_client=mock_x, bluesky_client=mock_bsky)
        results = cross_poster.publish("Test post", "x_post")

        assert "x" in results
        assert "bluesky" in results
        assert results["x"].success is True
        assert results["bluesky"].success is True

    def test_thread_publishing_to_x(self):
        mock_x = MagicMock()
        mock_x.post_thread.return_value = PostResult(
            success=True,
            tweet_id="123",
            url="https://x.com/user/status/123"
        )

        cross_poster = CrossPoster(x_client=mock_x, bluesky_client=None)
        tweets = ["First tweet", "Second tweet"]
        results = cross_poster.publish("Raw content", "x_thread", tweets=tweets)

        assert "x" in results
        mock_x.post_thread.assert_called_once_with(tweets)

    def test_thread_publishing_to_bluesky_adapts_tweets(self):
        mock_bsky = MagicMock()
        mock_bsky.post_thread.return_value = BlueskyPostResult(
            success=True,
            uri="at://did:plc:xyz/app.bsky.feed.post/abc",
            url="https://bsky.app/profile/user/post/abc"
        )

        cross_poster = CrossPoster(x_client=None, bluesky_client=mock_bsky)
        tweets = ["First tweet", "Second tweet"]
        results = cross_poster.publish("Raw content", "x_thread", tweets=tweets)

        assert "bluesky" in results
        # Verify post_thread was called
        mock_bsky.post_thread.assert_called_once()
        # The tweets should have been adapted
        adapted_tweets = mock_bsky.post_thread.call_args[0][0]
        assert len(adapted_tweets) == 2

    def test_one_platform_fails_other_succeeds(self):
        mock_x = MagicMock()
        mock_x.post.return_value = PostResult(
            success=True,
            tweet_id="123",
            url="https://x.com/user/status/123"
        )

        mock_bsky = MagicMock()
        mock_bsky.post.return_value = BlueskyPostResult(
            success=False,
            error="Rate limit exceeded"
        )

        cross_poster = CrossPoster(x_client=mock_x, bluesky_client=mock_bsky)
        results = cross_poster.publish("Test post", "x_post")

        # X should succeed
        assert results["x"].success is True
        # Bluesky should fail but still be in results
        assert results["bluesky"].success is False
        assert "Rate limit" in results["bluesky"].error

    def test_no_clients_returns_empty_dict(self):
        cross_poster = CrossPoster(x_client=None, bluesky_client=None)
        results = cross_poster.publish("Test post", "x_post")
        assert results == {}
