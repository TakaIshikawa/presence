"""Bluesky (AT Protocol) API client for posting content."""

import logging
import time
from dataclasses import dataclass
from typing import Optional
from atproto import Client
from atproto.exceptions import AtProtocolError

logger = logging.getLogger(__name__)


@dataclass
class BlueskyPostResult:
    success: bool
    uri: Optional[str] = None  # at:// URI
    cid: Optional[str] = None
    url: Optional[str] = None  # https://bsky.app/... URL
    error: Optional[str] = None


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

    def post(self, text: str) -> BlueskyPostResult:
        """Create a single Bluesky post (max 300 graphemes).

        Args:
            text: Post content (max 300 graphemes)

        Returns:
            BlueskyPostResult with success status and post details
        """
        self._ensure_login()
        try:
            response = self.client.send_post(text=text)
            # Extract rkey from URI: at://did:plc:.../app.bsky.feed.post/{rkey}
            uri = response.uri
            rkey = uri.split('/')[-1]
            url = f"https://bsky.app/profile/{self.handle}/post/{rkey}"
            return BlueskyPostResult(
                success=True,
                uri=uri,
                cid=response.cid,
                url=url
            )
        except AtProtocolError as e:
            return BlueskyPostResult(success=False, error=f'{type(e).__name__}: {e}')

    def post_thread(self, texts: list[str]) -> BlueskyPostResult:
        """Post a thread as a series of replies to self.

        Args:
            texts: List of post texts (each max 300 graphemes)

        Returns:
            BlueskyPostResult for the first post in the thread
        """
        if not texts:
            return BlueskyPostResult(success=False, error="No texts to post")

        self._ensure_login()
        try:
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
            return BlueskyPostResult(success=False, error=f'{type(e).__name__}: {e}')

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
        self._ensure_login()
        try:
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
            return BlueskyPostResult(success=False, error=f'{type(e).__name__}: {e}')

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
