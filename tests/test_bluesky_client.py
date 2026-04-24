"""Tests for the Bluesky (AT Protocol) API client."""

import sys
from pathlib import Path
from types import SimpleNamespace
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


def make_notification(
    uri="at://did:plc:reply/app.bsky.feed.post/r1",
    cid="reply-cid",
    reason="reply",
    text="Nice post",
):
    """Build a minimal atproto-shaped notification object."""
    return SimpleNamespace(
        uri=uri,
        cid=cid,
        reason=reason,
        reason_subject="at://did:plc:me/app.bsky.feed.post/root",
        indexed_at="2026-04-21T00:00:00Z",
        is_read=False,
        author=SimpleNamespace(
            did="did:plc:alice",
            handle="alice.bsky.social",
            display_name="Alice",
        ),
        record=SimpleNamespace(
            text=text,
            created_at="2026-04-21T00:00:00Z",
            reply=SimpleNamespace(
                root=SimpleNamespace(
                    uri="at://did:plc:me/app.bsky.feed.post/root",
                    cid="root-cid",
                ),
                parent=SimpleNamespace(
                    uri="at://did:plc:me/app.bsky.feed.post/root",
                    cid="root-cid",
                ),
            ),
        ),
    )


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

    def test_post_with_media_sends_alt_text(self, tmp_path):
        client, mock_client = make_bluesky_client()
        image_path = tmp_path / "visual.png"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\npng")
        response = mock_send_post(mock_client)
        blob = {"$type": "blob", "mimeType": "image/png", "size": 11, "ref": "bafkrei-test"}
        mock_client.upload_blob.return_value = SimpleNamespace(blob=blob)

        result = client.post_with_media(
            "Hello Bluesky!",
            str(image_path),
            alt_text="Text graphic about a launch",
        )

        assert result.success is True
        mock_client.upload_blob.assert_called_once_with(b"\x89PNG\r\n\x1a\npng")
        mock_client.send_post.assert_called_once()
        assert mock_client.send_post.call_args.kwargs["text"] == "Hello Bluesky!"
        embed = mock_client.send_post.call_args.kwargs["embed"]
        if isinstance(embed, dict):
            assert embed["images"][0]["alt"] == "Text graphic about a launch"
            assert embed["images"][0]["image"] == blob
        else:
            assert embed.images[0].alt == "Text graphic about a launch"
            assert embed.images[0].image == blob

    def test_post_with_media_missing_file_returns_media_error(self, tmp_path):
        client, mock_client = make_bluesky_client()

        result = client.post_with_media(
            text="Hello Bluesky!",
            media_path=str(tmp_path / "missing.png"),
            alt_text="Text graphic about a launch",
        )

        assert result.success is False
        assert result.error_category == "media"
        assert "not found" in result.error
        mock_client.upload_blob.assert_not_called()
        mock_client.send_post.assert_not_called()

    def test_post_with_media_oversized_file_returns_media_error(self, tmp_path):
        client, mock_client = make_bluesky_client()
        image_path = tmp_path / "visual.png"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\n" + b"x" * 1_000_001)

        result = client.post_with_media(
            text="Hello Bluesky!",
            media_path=str(image_path),
            alt_text="Text graphic about a launch",
        )

        assert result.success is False
        assert result.error_category == "media"
        assert "exceeds" in result.error
        mock_client.upload_blob.assert_not_called()
        mock_client.send_post.assert_not_called()

    def test_post_with_media_missing_alt_text_returns_media_error(self, tmp_path):
        client, mock_client = make_bluesky_client()
        image_path = tmp_path / "visual.png"
        image_path.write_bytes(b"\x89PNG\r\n\x1a\npng")

        result = client.post_with_media("Hello Bluesky!", str(image_path), alt_text="")

        assert result.success is False
        assert result.error_category == "media"
        assert "alt text" in result.error
        mock_client.upload_blob.assert_not_called()
        mock_client.send_post.assert_not_called()


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

    def test_reply_from_queue_metadata_uses_inbound_as_parent(self):
        client, mock_client = make_bluesky_client()
        mock_send_post(
            mock_client,
            uri="at://did:plc:me/app.bsky.feed.post/reply123",
            cid="replycid",
        )

        result = client.reply_from_queue_metadata(
            "Thanks!",
            inbound_uri="at://did:plc:alice/app.bsky.feed.post/inbound1",
            inbound_cid="inbound-cid",
            platform_metadata={
                "reply_root": {
                    "uri": "at://did:plc:me/app.bsky.feed.post/root1",
                    "cid": "root-cid",
                }
            },
            our_platform_id="at://did:plc:me/app.bsky.feed.post/root1",
        )

        assert result.success is True
        call_kwargs = mock_client.send_post.call_args.kwargs
        assert call_kwargs["text"] == "Thanks!"
        assert call_kwargs["reply_to"] == {
            "root": {
                "uri": "at://did:plc:me/app.bsky.feed.post/root1",
                "cid": "root-cid",
            },
            "parent": {
                "uri": "at://did:plc:alice/app.bsky.feed.post/inbound1",
                "cid": "inbound-cid",
            },
        }

    def test_reply_from_queue_metadata_missing_root_cid_returns_failure(self):
        client, mock_client = make_bluesky_client()

        result = client.reply_from_queue_metadata(
            "Thanks!",
            inbound_uri="at://did:plc:alice/app.bsky.feed.post/inbound1",
            inbound_cid="inbound-cid",
            platform_metadata={"parent_post_uri": "at://did:plc:me/app.bsky.feed.post/root1"},
            our_platform_id="at://did:plc:me/app.bsky.feed.post/root1",
        )

        assert result.success is False
        assert "Missing Bluesky reply references" in result.error
        mock_client.send_post.assert_not_called()


# --- BlueskyClient.get_notifications() ---


class TestGetNotifications:
    def test_calls_atproto_notifications_with_cursor_and_limit(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.notification.list_notifications.return_value = (
            SimpleNamespace(notifications=[], cursor="next-cursor")
        )

        notifications, cursor = client.get_notifications(
            cursor="existing-cursor",
            limit=25,
        )

        assert notifications == []
        assert cursor == "next-cursor"
        mock_client.app.bsky.notification.list_notifications.assert_called_once_with(
            params={"limit": 25, "cursor": "existing-cursor"}
        )

    def test_normalizes_notification_payloads(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.notification.list_notifications.return_value = (
            SimpleNamespace(
                notifications=[make_notification(text="What about this?")],
                cursor="next-cursor",
            )
        )

        notifications, cursor = client.get_notifications()

        assert cursor == "next-cursor"
        assert notifications == [
            {
                "uri": "at://did:plc:reply/app.bsky.feed.post/r1",
                "cid": "reply-cid",
                "reason": "reply",
                "reason_subject": "at://did:plc:me/app.bsky.feed.post/root",
                "indexed_at": "2026-04-21T00:00:00Z",
                "is_read": False,
                "author": {
                    "did": "did:plc:alice",
                    "handle": "alice.bsky.social",
                    "display_name": "Alice",
                },
                "record": {
                    "text": "What about this?",
                    "created_at": "2026-04-21T00:00:00Z",
                    "reply": {
                        "root": {
                            "uri": "at://did:plc:me/app.bsky.feed.post/root",
                            "cid": "root-cid",
                        },
                        "parent": {
                            "uri": "at://did:plc:me/app.bsky.feed.post/root",
                            "cid": "root-cid",
                        },
                    },
                },
            }
        ]


class TestGetUnreadMentions:
    def test_filters_unread_reply_and_mention_notifications_with_refs(self):
        client, mock_client = make_bluesky_client()
        read_reply = make_notification(
            uri="at://did:plc:reply/app.bsky.feed.post/read",
        )
        read_reply.is_read = True
        like = make_notification(
            uri="at://did:plc:reply/app.bsky.feed.post/like",
            reason="like",
        )
        mock_client.app.bsky.notification.list_notifications.return_value = (
            SimpleNamespace(
                notifications=[
                    make_notification(text="What about this?"),
                    read_reply,
                    like,
                ],
                cursor="next-cursor",
            )
        )

        notifications, cursor = client.get_unread_mentions(
            cursor="existing-cursor",
            limit=25,
        )

        assert cursor == "next-cursor"
        assert len(notifications) == 1
        assert notifications[0]["root_uri"] == "at://did:plc:me/app.bsky.feed.post/root"
        assert notifications[0]["root_cid"] == "root-cid"
        assert notifications[0]["parent_uri"] == "at://did:plc:me/app.bsky.feed.post/root"
        assert notifications[0]["parent_cid"] == "root-cid"
        assert notifications[0]["reply_root"] == {
            "uri": "at://did:plc:me/app.bsky.feed.post/root",
            "cid": "root-cid",
        }
        assert notifications[0]["reply_parent"] == {
            "uri": "at://did:plc:me/app.bsky.feed.post/root",
            "cid": "root-cid",
        }


class TestGetConversationContext:
    def test_fetches_parent_quote_and_sibling_excerpts(self):
        client, mock_client = make_bluesky_client()

        parent_uri = "at://did:plc:me/app.bsky.feed.post/root"
        inbound_uri = "at://did:plc:alice/app.bsky.feed.post/inbound"
        sibling_uri = "at://did:plc:bob/app.bsky.feed.post/sibling"

        quoted_record = SimpleNamespace(value=SimpleNamespace(text="Quoted post"))
        parent_record = SimpleNamespace(
            text="Parent post",
            embed=SimpleNamespace(record=quoted_record),
        )
        parent_post = SimpleNamespace(
            uri=parent_uri,
            cid="root-cid",
            author=SimpleNamespace(handle="me.bsky.social"),
            record=parent_record,
        )
        inbound_post = SimpleNamespace(
            uri=inbound_uri,
            cid="inbound-cid",
            author=SimpleNamespace(handle="alice.bsky.social"),
            record=SimpleNamespace(
                text="Inbound reply",
                reply=SimpleNamespace(parent=SimpleNamespace(uri=parent_uri)),
            ),
        )
        sibling_post = SimpleNamespace(
            uri=sibling_uri,
            cid="sibling-cid",
            author=SimpleNamespace(handle="bob.bsky.social"),
            record=SimpleNamespace(
                text="Sibling reply",
                reply=SimpleNamespace(parent=SimpleNamespace(uri=parent_uri)),
            ),
        )
        mock_client.get_post_thread.return_value = SimpleNamespace(
            thread=SimpleNamespace(
                post=parent_post,
                replies=[
                    SimpleNamespace(post=inbound_post, replies=[]),
                    SimpleNamespace(post=sibling_post, replies=[]),
                ],
            )
        )

        context = client.get_conversation_context(
            root_uri=parent_uri,
            parent_uri=parent_uri,
            inbound_uri=inbound_uri,
        )

        assert context["parent_post_uri"] == parent_uri
        assert context["parent_post_text"] == "Parent post"
        assert context["quoted_text"] == "Quoted post"
        assert context["sibling_replies"] == [
            {
                "uri": sibling_uri,
                "cid": "sibling-cid",
                "text": "Sibling reply",
                "author_handle": "bob.bsky.social",
            }
        ]


# --- BlueskyClient.__init__() ---


class TestInit:
    def test_initializes_with_handle_and_password(self):
        with patch("output.bluesky_client.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            client = BlueskyClient(handle="user.bsky.social", app_password="secret123")

            assert client.handle == "user.bsky.social"
            assert client.app_password == "secret123"
            assert client._logged_in is False
            mock_cls.assert_called_once()

    def test_creates_atproto_client_instance(self):
        with patch("output.bluesky_client.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client

            client = BlueskyClient(handle="test.bsky.social", app_password="pwd")

            assert client.client is mock_client


# --- BlueskyClient.get_post_metrics() ---


class TestGetPostMetrics:
    def test_success_returns_metrics_dict(self):
        client, mock_client = make_bluesky_client()

        # Mock the response structure
        mock_post = MagicMock()
        mock_post.like_count = 42
        mock_post.repost_count = 10
        mock_post.reply_count = 5
        mock_post.quote_count = 3

        mock_thread = MagicMock()
        mock_thread.post = mock_post

        mock_response = MagicMock()
        mock_response.thread = mock_thread

        mock_client.get_post_thread.return_value = mock_response

        result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

        assert result == {
            'like_count': 42,
            'repost_count': 10,
            'reply_count': 5,
            'quote_count': 3,
        }

    def test_handles_missing_metrics_with_defaults(self):
        client, mock_client = make_bluesky_client()

        # Mock post with missing attributes
        mock_post = MagicMock()
        del mock_post.like_count
        del mock_post.repost_count
        del mock_post.reply_count
        del mock_post.quote_count

        mock_thread = MagicMock()
        mock_thread.post = mock_post

        mock_response = MagicMock()
        mock_response.thread = mock_thread

        mock_client.get_post_thread.return_value = mock_response

        result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

        assert result == {
            'like_count': 0,
            'repost_count': 0,
            'reply_count': 0,
            'quote_count': 0,
        }

    def test_handles_none_metrics_values(self):
        client, mock_client = make_bluesky_client()

        # Mock post with None values
        mock_post = MagicMock()
        mock_post.like_count = None
        mock_post.repost_count = None
        mock_post.reply_count = None
        mock_post.quote_count = None

        mock_thread = MagicMock()
        mock_thread.post = mock_post

        mock_response = MagicMock()
        mock_response.thread = mock_thread

        mock_client.get_post_thread.return_value = mock_response

        result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

        # The `or 0` in the implementation should convert None to 0
        assert result == {
            'like_count': 0,
            'repost_count': 0,
            'reply_count': 0,
            'quote_count': 0,
        }

    def test_accepts_bare_post_id_for_authenticated_handle(self):
        client, mock_client = make_bluesky_client()
        mock_post = MagicMock()
        mock_post.like_count = 1
        mock_post.repost_count = 2
        mock_post.reply_count = 3
        mock_post.quote_count = 4
        mock_client.get_post_thread.return_value = SimpleNamespace(
            thread=SimpleNamespace(post=mock_post)
        )

        result = client.get_post_engagement("3kjxabcdefg")

        assert result["like_count"] == 1
        mock_client.get_post_thread.assert_called_once_with(
            uri="at://test.bsky.social/app.bsky.feed.post/3kjxabcdefg"
        )

    def test_accepts_bsky_app_post_url(self):
        client, mock_client = make_bluesky_client()
        mock_post = MagicMock()
        mock_post.like_count = 1
        mock_post.repost_count = 0
        mock_post.reply_count = 0
        mock_post.quote_count = 0
        mock_client.get_post_thread.return_value = SimpleNamespace(
            thread=SimpleNamespace(post=mock_post)
        )

        client.get_post_engagement("https://bsky.app/profile/alice.test/post/rkey123")

        mock_client.get_post_thread.assert_called_once_with(
            uri="at://alice.test/app.bsky.feed.post/rkey123"
        )

    def test_returns_none_when_response_is_none(self):
        client, mock_client = make_bluesky_client()
        mock_client.get_post_thread.return_value = None

        result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

        assert result is None

    def test_returns_none_when_thread_attribute_missing(self):
        client, mock_client = make_bluesky_client()
        mock_response = MagicMock(spec=[])  # No 'thread' attribute
        mock_client.get_post_thread.return_value = mock_response

        result = client.get_post_metrics("at://did:plc:xyz/app.bsky.feed.post/123")

        assert result is None

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


# --- BlueskyClient.get_post_metrics_batch() ---


class TestGetPostMetricsBatch:
    def test_fetches_metrics_for_all_uris(self):
        client, mock_client = make_bluesky_client()

        uris = [
            "at://did:plc:xyz/app.bsky.feed.post/post1",
            "at://did:plc:xyz/app.bsky.feed.post/post2",
            "at://did:plc:xyz/app.bsky.feed.post/post3",
        ]

        # Mock get_post_metrics to return different results
        with patch.object(client, 'get_post_metrics') as mock_get:
            mock_get.side_effect = [
                {'like_count': 10, 'repost_count': 2, 'reply_count': 1, 'quote_count': 0},
                {'like_count': 20, 'repost_count': 5, 'reply_count': 3, 'quote_count': 1},
                {'like_count': 30, 'repost_count': 8, 'reply_count': 4, 'quote_count': 2},
            ]

            with patch("output.bluesky_client.time.sleep") as mock_sleep:
                results = client.get_post_metrics_batch(uris)

            assert len(results) == 3
            assert results[0]['like_count'] == 10
            assert results[1]['like_count'] == 20
            assert results[2]['like_count'] == 30

            # Verify get_post_metrics was called for each URI
            assert mock_get.call_count == 3
            mock_get.assert_any_call(uris[0])
            mock_get.assert_any_call(uris[1])
            mock_get.assert_any_call(uris[2])

    def test_rate_limits_between_requests(self):
        client, mock_client = make_bluesky_client()

        uris = [
            "at://did:plc:xyz/app.bsky.feed.post/post1",
            "at://did:plc:xyz/app.bsky.feed.post/post2",
            "at://did:plc:xyz/app.bsky.feed.post/post3",
        ]

        with patch.object(client, 'get_post_metrics') as mock_get:
            mock_get.return_value = {'like_count': 10, 'repost_count': 2, 'reply_count': 1, 'quote_count': 0}

            with patch("output.bluesky_client.time.sleep") as mock_sleep:
                client.get_post_metrics_batch(uris)

                # Should sleep between each request (N-1 times for N requests)
                assert mock_sleep.call_count == 2
                mock_sleep.assert_called_with(1.0)

    def test_no_sleep_after_last_request(self):
        client, mock_client = make_bluesky_client()

        uris = ["at://did:plc:xyz/app.bsky.feed.post/single"]

        with patch.object(client, 'get_post_metrics') as mock_get:
            mock_get.return_value = {'like_count': 10, 'repost_count': 2, 'reply_count': 1, 'quote_count': 0}

            with patch("output.bluesky_client.time.sleep") as mock_sleep:
                client.get_post_metrics_batch(uris)

                # Single URI should not sleep
                mock_sleep.assert_not_called()

    def test_includes_none_for_failed_fetches(self):
        client, mock_client = make_bluesky_client()

        uris = [
            "at://did:plc:xyz/app.bsky.feed.post/post1",
            "at://did:plc:xyz/app.bsky.feed.post/post2",
            "at://did:plc:xyz/app.bsky.feed.post/post3",
        ]

        with patch.object(client, 'get_post_metrics') as mock_get:
            # Second request fails
            mock_get.side_effect = [
                {'like_count': 10, 'repost_count': 2, 'reply_count': 1, 'quote_count': 0},
                None,
                {'like_count': 30, 'repost_count': 8, 'reply_count': 4, 'quote_count': 2},
            ]

            with patch("output.bluesky_client.time.sleep"):
                results = client.get_post_metrics_batch(uris)

            assert len(results) == 3
            assert results[0]['like_count'] == 10
            assert results[1] is None
            assert results[2]['like_count'] == 30

    def test_empty_list_returns_empty_list(self):
        client, mock_client = make_bluesky_client()

        with patch("output.bluesky_client.time.sleep") as mock_sleep:
            results = client.get_post_metrics_batch([])

        assert results == []
        mock_sleep.assert_not_called()


# --- BlueskyClient.get_profile_metrics() ---


class TestGetProfileMetrics:
    def test_success_returns_profile_metrics_dict(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.actor.get_profile.return_value = SimpleNamespace(
            followers_count=123,
            follows_count=45,
            posts_count=678,
        )

        result = client.get_profile_metrics()

        assert result == {
            "follower_count": 123,
            "following_count": 45,
            "tweet_count": 678,
            "listed_count": None,
        }
        mock_client.app.bsky.actor.get_profile.assert_called_once_with(
            params={"actor": "test.bsky.social"}
        )

    def test_missing_counts_default_to_zero(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.actor.get_profile.return_value = SimpleNamespace()

        result = client.get_profile_metrics()

        assert result == {
            "follower_count": 0,
            "following_count": 0,
            "tweet_count": 0,
            "listed_count": None,
        }

    def test_returns_none_when_profile_missing(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.actor.get_profile.return_value = None

        assert client.get_profile_metrics() is None

    def test_atprotocol_error_logs_warning_and_returns_none(self):
        client, mock_client = make_bluesky_client()
        mock_client.app.bsky.actor.get_profile.side_effect = NetworkError(
            "Profile unavailable"
        )

        with patch("output.bluesky_client.logger") as mock_logger:
            result = client.get_profile_metrics()

        assert result is None
        mock_logger.warning.assert_called_once()
