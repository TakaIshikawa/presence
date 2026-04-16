"""Tests for the Bluesky (AT Protocol) API client."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from atproto.exceptions import AtProtocolError, NetworkError, UnauthorizedError

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.bluesky_client import BlueskyClient, BlueskyPostResult


# --- Helpers ---


def make_bluesky_client():
    """Create a BlueskyClient with mocked atproto.Client."""
    with patch("output.bluesky_client.Client") as mock_cls:
        mock_client = MagicMock()
        mock_cls.return_value = mock_client
        client = BlueskyClient(
            handle="test.bsky.social",
            app_password="test-password"
        )
        return client, mock_client


def mock_send_post(mock_client, uri="at://did:plc:abc/app.bsky.feed.post/123", cid="cid123"):
    """Set up send_post to return a response with the given URI and CID."""
    response = MagicMock()
    response.uri = uri
    response.cid = cid
    mock_client.send_post.return_value = response
    return response


# --- BlueskyClient._ensure_login() ---


class TestEnsureLogin:
    def test_first_call_triggers_login(self):
        client, mock_client = make_bluesky_client()

        client._ensure_login()

        mock_client.login.assert_called_once_with("test.bsky.social", "test-password")

    def test_subsequent_calls_skip_login(self):
        client, mock_client = make_bluesky_client()

        client._ensure_login()
        client._ensure_login()

        # login should only be called once
        assert mock_client.login.call_count == 1


# --- BlueskyClient.post() ---


class TestPost:
    def test_success_returns_post_result_with_uri_and_url(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(
            mock_client,
            uri="at://did:plc:xyz/app.bsky.feed.post/abc123",
            cid="bafy123"
        )

        result = client.post("Hello Bluesky!")

        assert result.success is True
        assert result.uri == "at://did:plc:xyz/app.bsky.feed.post/abc123"
        assert result.cid == "bafy123"
        assert result.url == "https://bsky.app/profile/test.bsky.social/post/abc123"
        assert result.error is None

    def test_calls_send_post_with_text(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(mock_client)

        client.post("Hello Bluesky!")

        mock_client.send_post.assert_called_once_with(text="Hello Bluesky!")

    def test_exception_returns_failure(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = AtProtocolError("Rate limit exceeded")

        result = client.post("Hello Bluesky!")

        assert result.success is False
        assert "Rate limit exceeded" in result.error
        assert result.uri is None
        assert result.url is None

    def test_extracts_rkey_from_uri_correctly(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(
            mock_client,
            uri="at://did:plc:long-did-string/app.bsky.feed.post/3kjxabcdefg"
        )

        result = client.post("Test post")

        assert "3kjxabcdefg" in result.url
        assert result.url == "https://bsky.app/profile/test.bsky.social/post/3kjxabcdefg"

    def test_atprotocol_error_includes_exception_type_name(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = NetworkError("Connection timeout")

        result = client.post("Hello Bluesky!")

        assert result.success is False
        assert "NetworkError" in result.error
        assert "Connection timeout" in result.error

    def test_unauthorized_error_includes_exception_type_name(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = UnauthorizedError("Invalid credentials")

        result = client.post("Hello Bluesky!")

        assert result.success is False
        assert "UnauthorizedError" in result.error
        assert "Invalid credentials" in result.error


# --- BlueskyClient.post_thread() ---


class TestPostThread:
    def test_chains_posts_via_reply_references(self):
        client, mock_client = make_bluesky_client()

        # Each send_post call returns a different URI
        responses = []
        for i, rkey in enumerate(["post1", "post2", "post3"]):
            r = MagicMock()
            r.uri = f"at://did:plc:xyz/app.bsky.feed.post/{rkey}"
            r.cid = f"cid{i+1}"
            responses.append(r)
        mock_client.send_post.side_effect = responses

        result = client.post_thread(["First post", "Second post", "Third post"])

        assert result.success is True
        assert result.uri == "at://did:plc:xyz/app.bsky.feed.post/post1"
        assert result.cid == "cid1"
        assert result.url == "https://bsky.app/profile/test.bsky.social/post/post1"

        # Verify the call pattern
        calls = mock_client.send_post.call_args_list
        # First post has no reply_to
        assert calls[0][1]["text"] == "First post"
        assert "reply_to" not in calls[0][1]

        # Second post replies to first
        assert calls[1][1]["text"] == "Second post"
        assert calls[1][1]["reply_to"]["root"]["uri"] == "at://did:plc:xyz/app.bsky.feed.post/post1"
        assert calls[1][1]["reply_to"]["parent"]["uri"] == "at://did:plc:xyz/app.bsky.feed.post/post1"

        # Third post replies to second, but root is still first
        assert calls[2][1]["text"] == "Third post"
        assert calls[2][1]["reply_to"]["root"]["uri"] == "at://did:plc:xyz/app.bsky.feed.post/post1"
        assert calls[2][1]["reply_to"]["parent"]["uri"] == "at://did:plc:xyz/app.bsky.feed.post/post2"

    def test_empty_list_returns_error(self):
        client, mock_client = make_bluesky_client()

        result = client.post_thread([])

        assert result.success is False
        assert result.error == "No texts to post"
        mock_client.send_post.assert_not_called()

    def test_single_post_thread(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(
            mock_client,
            uri="at://did:plc:xyz/app.bsky.feed.post/solo"
        )

        result = client.post_thread(["Only post"])

        assert result.success is True
        assert "solo" in result.url
        # Single post should not have reply_to
        mock_client.send_post.assert_called_once()
        call_kwargs = mock_client.send_post.call_args[1]
        assert "reply_to" not in call_kwargs

    def test_partial_failure_mid_thread_returns_error(self):
        client, mock_client = make_bluesky_client()

        first_response = MagicMock()
        first_response.uri = "at://did:plc:xyz/app.bsky.feed.post/post1"
        first_response.cid = "cid1"
        mock_client.send_post.side_effect = [
            first_response,
            AtProtocolError("Rate limit on second post"),
        ]

        result = client.post_thread(["First", "Second", "Third"])

        assert result.success is False
        assert "Rate limit on second post" in result.error

    def test_atprotocol_error_includes_exception_type_name(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = NetworkError("Network unavailable")

        result = client.post_thread(["First post", "Second post"])

        assert result.success is False
        assert "NetworkError" in result.error
        assert "Network unavailable" in result.error


# --- BlueskyClient.reply() ---


class TestReply:
    def test_success_returns_post_result(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(
            mock_client,
            uri="at://did:plc:xyz/app.bsky.feed.post/reply123",
            cid="replycid"
        )

        result = client.reply(
            "Great post!",
            parent_uri="at://did:plc:parent/app.bsky.feed.post/p1",
            parent_cid="pcid1",
            root_uri="at://did:plc:root/app.bsky.feed.post/r1",
            root_cid="rcid1"
        )

        assert result.success is True
        assert result.uri == "at://did:plc:xyz/app.bsky.feed.post/reply123"
        assert "reply123" in result.url

    def test_calls_send_post_with_reply_params(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(mock_client)

        client.reply(
            "Thanks!",
            parent_uri="at://did:plc:parent/app.bsky.feed.post/p1",
            parent_cid="pcid1",
            root_uri="at://did:plc:root/app.bsky.feed.post/r1",
            root_cid="rcid1"
        )

        call_kwargs = mock_client.send_post.call_args[1]
        assert call_kwargs["text"] == "Thanks!"
        assert call_kwargs["reply_to"]["parent"]["uri"] == "at://did:plc:parent/app.bsky.feed.post/p1"
        assert call_kwargs["reply_to"]["parent"]["cid"] == "pcid1"
        assert call_kwargs["reply_to"]["root"]["uri"] == "at://did:plc:root/app.bsky.feed.post/r1"
        assert call_kwargs["reply_to"]["root"]["cid"] == "rcid1"

    def test_exception_returns_failure(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = AtProtocolError("Forbidden")

        result = client.reply(
            "Reply text",
            parent_uri="at://did:plc:parent/app.bsky.feed.post/p1",
            parent_cid="pcid1",
            root_uri="at://did:plc:root/app.bsky.feed.post/r1",
            root_cid="rcid1"
        )

        assert result.success is False
        assert "Forbidden" in result.error

    def test_atprotocol_error_includes_exception_type_name(self):
        client, mock_client = make_bluesky_client()
        mock_client.send_post.side_effect = UnauthorizedError("Token expired")

        result = client.reply(
            "Reply text",
            parent_uri="at://did:plc:parent/app.bsky.feed.post/p1",
            parent_cid="pcid1",
            root_uri="at://did:plc:root/app.bsky.feed.post/r1",
            root_cid="rcid1"
        )

        assert result.success is False
        assert "UnauthorizedError" in result.error
        assert "Token expired" in result.error


# --- BlueskyClient.get_post_metrics() ---


class TestGetPostMetrics:
    def test_atprotocol_error_logs_warning_and_returns_none(self):
        client, mock_client = make_bluesky_client()
        mock_client.get_post_thread.side_effect = NetworkError("Post not found")

        with patch("output.bluesky_client.logger") as mock_logger:
            result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

            assert result is None
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            assert "Failed to fetch metrics for" in args[0]
            assert "at://did:plc:xyz/app.bsky.feed.post/123" in args
            assert "Post not found" in str(args[2])
