"""Bluesky (AT Protocol) API client for posting content."""

from dataclasses import dataclass
from typing import Optional
from atproto import Client


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
        except Exception as e:
            return BlueskyPostResult(success=False, error=str(e))

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
        except Exception as e:
            return BlueskyPostResult(success=False, error=str(e))

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
        except Exception as e:
            return BlueskyPostResult(success=False, error=str(e))
