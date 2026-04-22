"""Bluesky (AT Protocol) API client for posting content."""

import logging
import json
import re
import time
from dataclasses import dataclass
from typing import Optional
from atproto import Client
from atproto.exceptions import AtProtocolError

from .publish_errors import classify_publish_error, PublishErrorCategory

logger = logging.getLogger(__name__)


def _limit_alt_text(alt_text: str, max_chars: int = 1000) -> str:
    alt_text = re.sub(r"\s+", " ", (alt_text or "").strip())
    if len(alt_text) <= max_chars:
        return alt_text
    return alt_text[: max_chars - 3].rstrip(" ,;:.") + "..."


@dataclass
class BlueskyPostResult:
    success: bool
    uri: Optional[str] = None  # at:// URI
    cid: Optional[str] = None
    url: Optional[str] = None  # https://bsky.app/... URL
    error: Optional[str] = None
    error_category: Optional[PublishErrorCategory] = None


class BlueskyClient:
    def __init__(self, handle: str, app_password: str):
        """Initialize Bluesky client.

        Args:
            handle: Bluesky handle (e.g. user.bsky.social)
            app_password: App password (not main account password)
        """
        self.client = Client()
        self.handle = handle
        self.app_password = app_password
        self._logged_in = False

    def _ensure_login(self) -> None:
        """Ensure the client is logged in before making requests."""
        if not self._logged_in:
            self.client.login(self.handle, self.app_password)
            self._logged_in = True

    def _failure_result(self, error: object) -> BlueskyPostResult:
        error_text = f"{type(error).__name__}: {error}"
        return BlueskyPostResult(
            success=False,
            error=error_text,
            error_category=classify_publish_error(error_text, platform="bluesky"),
        )

    def _success_result(self, response) -> BlueskyPostResult:
        uri = response.uri
        rkey = uri.split('/')[-1]
        url = f"https://bsky.app/profile/{self.handle}/post/{rkey}"
        return BlueskyPostResult(
            success=True,
            uri=uri,
            cid=response.cid,
            url=url
        )

    def post(self, text: str) -> BlueskyPostResult:
        """Create a single Bluesky post (max 300 graphemes).

        Args:
            text: Post content (max 300 graphemes)

        Returns:
            BlueskyPostResult with success status and post details
        """
        try:
            self._ensure_login()
            response = self.client.send_post(text=text)
            return self._success_result(response)
        except AtProtocolError as e:
            return self._failure_result(e)

    def post_with_media(
        self,
        text: str,
        media_path: str,
        alt_text: str = "",
    ) -> BlueskyPostResult:
        """Create a Bluesky post with one attached image when supported."""
        send_image = getattr(self.client, "send_image", None)
        if not callable(send_image):
            logger.warning("Bluesky client does not support image upload; posting text only")
            return self.post(text)

        try:
            self._ensure_login()
            with open(media_path, "rb") as fh:
                image = fh.read()
            alt_text = _limit_alt_text(alt_text)

            try:
                response = send_image(text=text, image=image, image_alt=alt_text)
            except TypeError as e:
                logger.warning(f"Bluesky image alt text unsupported; retrying without it: {e}")
                response = send_image(text=text, image=image)

            return self._success_result(response)
        except (OSError, AtProtocolError) as e:
            return self._failure_result(e)

    def post_thread(self, texts: list[str]) -> BlueskyPostResult:
        """Post a thread as a series of replies to self.

        Args:
            texts: List of post texts (each max 300 graphemes)

        Returns:
            BlueskyPostResult for the first post in the thread
        """
        if not texts:
            return BlueskyPostResult(
                success=False,
                error="No texts to post",
                error_category="unknown",
            )

        try:
            self._ensure_login()
            first_response = None
            parent_ref = None
            root_ref = None

            for i, text in enumerate(texts):
                if i == 0:
                    # First post starts the thread
                    response = self.client.send_post(text=text)
                    first_response = response
                    parent_ref = {'uri': response.uri, 'cid': response.cid}
                    root_ref = parent_ref
                else:
                    # Subsequent posts reply to previous post
                    response = self.client.send_post(
                        text=text,
                        reply_to={'root': root_ref, 'parent': parent_ref}
                    )
                    parent_ref = {'uri': response.uri, 'cid': response.cid}

            rkey = first_response.uri.split('/')[-1]
            url = f"https://bsky.app/profile/{self.handle}/post/{rkey}"
            return BlueskyPostResult(
                success=True,
                uri=first_response.uri,
                cid=first_response.cid,
                url=url
            )
        except AtProtocolError as e:
            return self._failure_result(e)

    def reply(
        self,
        text: str,
        parent_uri: str,
        parent_cid: str,
        root_uri: str,
        root_cid: str
    ) -> BlueskyPostResult:
        """Reply to a specific post.

        Args:
            text: Reply text (max 300 graphemes)
            parent_uri: AT URI of the post being replied to
            parent_cid: CID of the post being replied to
            root_uri: AT URI of the root post in the thread
            root_cid: CID of the root post in the thread

        Returns:
            BlueskyPostResult with reply details
        """
        try:
            self._ensure_login()
            response = self.client.send_post(
                text=text,
                reply_to={
                    'root': {'uri': root_uri, 'cid': root_cid},
                    'parent': {'uri': parent_uri, 'cid': parent_cid}
                }
            )
            rkey = response.uri.split('/')[-1]
            url = f"https://bsky.app/profile/{self.handle}/post/{rkey}"
            return BlueskyPostResult(
                success=True,
                uri=response.uri,
                cid=response.cid,
                url=url
            )
        except AtProtocolError as e:
            return self._failure_result(e)

    def reply_from_queue_metadata(
        self,
        text: str,
        *,
        inbound_uri: str,
        inbound_cid: str,
        platform_metadata: str | dict | None = None,
        our_platform_id: Optional[str] = None,
    ) -> BlueskyPostResult:
        """Reply to a queued Bluesky inbound post using stored queue metadata."""
        metadata = self._parse_queue_metadata(platform_metadata)
        root_ref = self._queue_ref(metadata, "root")
        root_uri = (
            root_ref.get("uri")
            or metadata.get("root_uri")
            or metadata.get("parent_post_uri")
            or our_platform_id
        )
        root_cid = root_ref.get("cid") or metadata.get("root_cid")

        if not inbound_uri or not inbound_cid or not root_uri or not root_cid:
            return BlueskyPostResult(
                success=False,
                error=(
                    "Missing Bluesky reply references: inbound_uri, "
                    "inbound_cid, root_uri, and root_cid are required"
                ),
                error_category="unknown",
            )

        return self.reply(
            text,
            parent_uri=inbound_uri,
            parent_cid=inbound_cid,
            root_uri=root_uri,
            root_cid=root_cid,
        )

    @staticmethod
    def _parse_queue_metadata(platform_metadata: str | dict | None) -> dict:
        if isinstance(platform_metadata, dict):
            return platform_metadata
        if not platform_metadata:
            return {}
        try:
            parsed = json.loads(platform_metadata)
        except (json.JSONDecodeError, TypeError):
            return {}
        return parsed if isinstance(parsed, dict) else {}

    @staticmethod
    def _queue_ref(metadata: dict, name: str) -> dict:
        for key in (f"reply_{name}", name):
            value = metadata.get(key)
            if isinstance(value, dict):
                return value
        return {}

    def get_notifications(
        self,
        cursor: Optional[str] = None,
        limit: int = 50,
    ) -> tuple[list[dict], Optional[str]]:
        """Fetch Bluesky notifications using AT Protocol notification APIs.

        Args:
            cursor: Optional AT Protocol pagination cursor.
            limit: Maximum notifications to fetch.

        Returns:
            Tuple of normalized notification dicts and the next cursor.
        """
        self._ensure_login()
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor

        response = self.client.app.bsky.notification.list_notifications(
            params=params
        )
        notifications = [
            self._notification_to_dict(notification)
            for notification in (getattr(response, "notifications", None) or [])
        ]
        return notifications, getattr(response, "cursor", None)

    def get_unread_mentions(
        self,
        cursor: Optional[str] = None,
        limit: int = 50,
    ) -> tuple[list[dict], Optional[str]]:
        """Fetch unread Bluesky reply/mention notifications.

        Returns normalized notifications with top-level reply reference metadata
        used by the reply review queue for later publishing.
        """
        notifications, next_cursor = self.get_notifications(
            cursor=cursor,
            limit=limit,
        )
        unread_mentions = []
        for notification in notifications:
            if notification.get("reason") not in {"reply", "mention"}:
                continue
            if notification.get("is_read") is not False:
                continue
            unread_mentions.append(self._with_reply_reference_metadata(notification))
        return unread_mentions, next_cursor

    @staticmethod
    def _with_reply_reference_metadata(notification: dict) -> dict:
        record = notification.get("record") or {}
        reply = record.get("reply") or {}
        root = reply.get("root") or {}
        parent = reply.get("parent") or {}

        enriched = dict(notification)
        if root.get("uri") or root.get("cid"):
            enriched["reply_root"] = {
                k: v for k, v in {
                    "uri": root.get("uri"),
                    "cid": root.get("cid"),
                }.items() if v
            }
            enriched["root_uri"] = root.get("uri")
            enriched["root_cid"] = root.get("cid")
        if parent.get("uri") or parent.get("cid"):
            enriched["reply_parent"] = {
                k: v for k, v in {
                    "uri": parent.get("uri"),
                    "cid": parent.get("cid"),
                }.items() if v
            }
            enriched["parent_uri"] = parent.get("uri")
            enriched["parent_cid"] = parent.get("cid")
        return enriched

    @staticmethod
    def _post_to_excerpt(post) -> dict:
        author = getattr(post, "author", None)
        record = getattr(post, "record", None)
        return {
            "uri": getattr(post, "uri", None),
            "cid": getattr(post, "cid", None),
            "text": (getattr(record, "text", "") if record else "")[:300],
            "author_handle": getattr(author, "handle", None),
        }

    @staticmethod
    def _quoted_text_from_record(record) -> Optional[str]:
        embed = getattr(record, "embed", None)
        quoted = getattr(embed, "record", None) if embed else None
        quoted_value = getattr(quoted, "value", None) if quoted else None
        text = getattr(quoted_value, "text", None) if quoted_value else None
        return text or None

    def get_conversation_context(
        self,
        *,
        root_uri: str,
        parent_uri: Optional[str] = None,
        inbound_uri: Optional[str] = None,
        max_siblings: int = 3,
    ) -> dict:
        """Fetch bounded context around a Bluesky reply thread."""
        self._ensure_login()
        context: dict = {}
        try:
            response = self.client.get_post_thread(uri=root_uri)
        except AtProtocolError as e:
            logger.warning("Failed to fetch Bluesky conversation context: %s", e)
            return context

        thread = getattr(response, "thread", None)
        if not thread:
            return context

        parent_uri = parent_uri or root_uri

        def walk(node):
            if not node:
                return
            post = getattr(node, "post", None)
            if post:
                yield node
            for reply in getattr(node, "replies", None) or []:
                yield from walk(reply)

        nodes = list(walk(thread))
        posts_by_uri = {
            getattr(node.post, "uri", None): node.post
            for node in nodes
            if getattr(node, "post", None)
        }

        parent_post = posts_by_uri.get(parent_uri)
        if parent_post:
            record = getattr(parent_post, "record", None)
            context["parent_post_uri"] = parent_uri
            context["parent_post_text"] = (
                getattr(record, "text", "") if record else ""
            )
            quoted_text = self._quoted_text_from_record(record)
            if quoted_text:
                context["quoted_text"] = quoted_text

        sibling_replies = []
        for node in nodes:
            post = node.post
            post_uri = getattr(post, "uri", None)
            if post_uri in {root_uri, parent_uri, inbound_uri}:
                continue
            record = getattr(post, "record", None)
            reply = getattr(record, "reply", None) if record else None
            reply_parent = getattr(reply, "parent", None) if reply else None
            if getattr(reply_parent, "uri", None) != parent_uri:
                continue
            sibling_replies.append(self._post_to_excerpt(post))
            if len(sibling_replies) >= max_siblings:
                break
        if sibling_replies:
            context["sibling_replies"] = sibling_replies

        return context

    @staticmethod
    def _notification_to_dict(notification) -> dict:
        """Convert an atproto notification model to plain Python data."""
        author = getattr(notification, "author", None)
        record = getattr(notification, "record", None)
        record_reply = getattr(record, "reply", None) if record else None

        def ref_to_dict(ref) -> Optional[dict]:
            if not ref:
                return None
            return {
                "uri": getattr(ref, "uri", None),
                "cid": getattr(ref, "cid", None),
            }

        reply = None
        if record_reply:
            reply = {
                "root": ref_to_dict(getattr(record_reply, "root", None)),
                "parent": ref_to_dict(getattr(record_reply, "parent", None)),
            }

        return {
            "uri": getattr(notification, "uri", None),
            "cid": getattr(notification, "cid", None),
            "reason": getattr(notification, "reason", None),
            "reason_subject": getattr(notification, "reason_subject", None),
            "indexed_at": getattr(notification, "indexed_at", None),
            "is_read": getattr(notification, "is_read", None),
            "author": {
                "did": getattr(author, "did", None),
                "handle": getattr(author, "handle", None),
                "display_name": getattr(author, "display_name", None),
            },
            "record": {
                "text": getattr(record, "text", "") if record else "",
                "created_at": getattr(record, "created_at", None) if record else None,
                "reply": reply,
            },
        }

    def get_post_metrics(self, uri: str) -> Optional[dict]:
        """Fetch engagement metrics for a single post.

        Args:
            uri: AT Protocol URI (e.g., at://did:plc:.../app.bsky.feed.post/...)

        Returns:
            Dict with like_count, repost_count, reply_count, quote_count
            or None if post not found or error occurred
        """
        self._ensure_login()
        try:
            # Get post thread to access metrics
            response = self.client.get_post_thread(uri=uri)

            if not response or not hasattr(response, 'thread'):
                return None

            post = response.thread.post

            # Extract metrics from post record
            like_count = getattr(post, 'like_count', 0) or 0
            repost_count = getattr(post, 'repost_count', 0) or 0
            reply_count = getattr(post, 'reply_count', 0) or 0
            quote_count = getattr(post, 'quote_count', 0) or 0

            return {
                'like_count': like_count,
                'repost_count': repost_count,
                'reply_count': reply_count,
                'quote_count': quote_count,
            }
        except AtProtocolError as e:
            # Post not found or other error
            logger.warning('Failed to fetch metrics for %s: %s', uri, e)
            return None

    def get_post_metrics_batch(self, uris: list[str]) -> list[dict]:
        """Fetch metrics for multiple posts with rate limiting.

        Args:
            uris: List of AT Protocol URIs

        Returns:
            List of dicts with metrics (None for failed fetches)
        """
        results = []
        for uri in uris:
            metrics = self.get_post_metrics(uri)
            results.append(metrics)
            # Rate limit: 1 request per second to be safe
            if len(results) < len(uris):
                time.sleep(1.0)
        return results

    def get_profile_metrics(self) -> dict | None:
        """Fetch authenticated user's public profile metrics.

        Returns:
            Dict with follower_count, following_count, tweet_count, listed_count.
            Bluesky does not expose listed_count here, so it is always None.
        """
        self._ensure_login()
        try:
            profile = self.client.app.bsky.actor.get_profile(
                params={"actor": self.handle}
            )
            if not profile:
                return None

            return {
                "follower_count": getattr(profile, "followers_count", None) or 0,
                "following_count": getattr(profile, "follows_count", None) or 0,
                "tweet_count": getattr(profile, "posts_count", None) or 0,
                "listed_count": None,
            }
        except AtProtocolError as e:
            logger.warning("Failed to fetch Bluesky profile metrics: %s", e)
            return None
