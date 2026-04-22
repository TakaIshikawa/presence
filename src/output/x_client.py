"""X (Twitter) API client for posting content."""

import logging
import re
import tweepy
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class PostResult:
    success: bool
    tweet_id: Optional[str] = None
    url: Optional[str] = None
    error: Optional[str] = None


class XClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        access_token: str,
        access_token_secret: str
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._access_token = access_token
        self._access_token_secret = access_token_secret
        self.client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
        self._username: Optional[str] = None
        self._user_id: Optional[str] = None
        self._v1_api_instance: Optional[tweepy.API] = None
        self.last_error: Optional[str] = None

    def _clear_error(self) -> None:
        self.last_error = None

    def _record_error(self, error: object) -> str:
        self.last_error = str(error)
        return self.last_error

    def _status_url(self, tweet_id: str) -> str:
        return f"https://x.com/{self.username}/status/{tweet_id}"

    @property
    def username(self) -> str:
        if not self._username:
            try:
                me = self.client.get_me()
                self._clear_error()
            except tweepy.TweepyException as e:
                self._record_error(e)
                raise
            self._username = me.data.username
            self._user_id = str(me.data.id)
        return self._username

    def get_authenticated_user(self) -> tuple[str, str]:
        """Return authenticated user's (id, username), cached per client."""
        if not self._user_id or not self._username:
            try:
                me = self.client.get_me()
                self._clear_error()
            except tweepy.TweepyException as e:
                self._record_error(e)
                raise
            self._user_id = str(me.data.id)
            self._username = me.data.username
        return self._user_id, self._username

    @property
    def _v1_api(self) -> tweepy.API:
        """Lazily create a v1.1 API instance for media uploads."""
        if self._v1_api_instance is None:
            auth = tweepy.OAuth1UserHandler(
                self._api_key, self._api_secret,
                self._access_token, self._access_token_secret,
            )
            self._v1_api_instance = tweepy.API(auth)
        return self._v1_api_instance

    def upload_media(self, file_path: str, alt_text: str = "") -> Optional[str]:
        """Upload a media file via v1.1 API.

        Returns the media_id string, or None on failure.
        """
        try:
            media = self._v1_api.media_upload(filename=file_path)
            self._clear_error()
            if alt_text:
                self._v1_api.create_media_metadata(media.media_id, alt_text=alt_text)
            return str(media.media_id)
        except tweepy.TweepyException as e:
            self._record_error(e)
            logger.warning(f"Media upload failed: {e}")
            return None

    def post_with_media(
        self, text: str, media_path: str, alt_text: str = ""
    ) -> PostResult:
        """Post a tweet with an attached image.

        Uploads the image via v1.1, then creates the tweet via v2.
        """
        media_id = self.upload_media(media_path, alt_text)
        if not media_id:
            return PostResult(success=False, error="Media upload failed")

        try:
            response = self.client.create_tweet(
                text=text, media_ids=[media_id]
            )
            self._clear_error()
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=self._status_url(tweet_id),
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def get_profile_metrics(self) -> dict | None:
        """Fetch authenticated user's public metrics (follower/following counts)."""
        try:
            me = self.client.get_me(user_fields=["public_metrics"])
            if me and me.data:
                self._clear_error()
                pm = me.data.public_metrics
                return {
                    "follower_count": pm["followers_count"],
                    "following_count": pm["following_count"],
                    "tweet_count": pm["tweet_count"],
                    "listed_count": pm["listed_count"],
                }
        except Exception as e:
            self._record_error(e)
            logger.warning(f"Failed to fetch profile metrics: {e}")
        return None

    def search_tweets(self, query: str, max_results: int = 10) -> list[dict]:
        """Search recent tweets via X API v2.

        Returns list of dicts with keys: id, text, created_at,
        public_metrics, reply_settings, author_id, author_username.
        Empty list on error.
        """
        try:
            max_results = max(10, min(max_results, 100))
            response = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=[
                    "created_at", "public_metrics", "reply_settings",
                    "author_id", "conversation_id",
                ],
                expansions=["author_id"],
                user_auth=True,
            )
            self._clear_error()
            if not response.data:
                return []

            # Build author lookup from expansions
            users_by_id = {}
            if response.includes and "users" in response.includes:
                for user in response.includes["users"]:
                    users_by_id[str(user.id)] = user.username

            return [
                {
                    "id": str(t.id),
                    "text": t.text or "",
                    "created_at": (
                        t.created_at.isoformat() if t.created_at else ""
                    ),
                    "public_metrics": getattr(t, "public_metrics", {}) or {},
                    "reply_settings": getattr(t, "reply_settings", "everyone"),
                    "author_id": str(t.author_id) if t.author_id else "",
                    "author_username": users_by_id.get(
                        str(t.author_id), ""
                    ),
                }
                for t in response.data
            ]
        except tweepy.TweepyException as e:
            self._record_error(e)
            logger.warning(f"Search failed: {e}")
            return []

    def get_following(self, max_results: int = 200) -> list[dict]:
        """Fetch the list of accounts the authenticated user follows.

        Returns list of dicts with keys: id, username, name.
        """
        try:
            me = self.client.get_me(user_auth=True)
            self._clear_error()
            if not me or not me.data:
                return []
            my_id = me.data.id

            results = []
            pagination_token = None

            while len(results) < max_results:
                kwargs = {
                    "id": my_id,
                    "max_results": min(max_results - len(results), 1000),
                    "user_auth": True,
                }
                if pagination_token:
                    kwargs["pagination_token"] = pagination_token

                response = self.client.get_users_following(**kwargs)
                self._clear_error()
                if not response.data:
                    break

                for user in response.data:
                    results.append({
                        "id": str(user.id),
                        "username": user.username,
                        "name": user.name or user.username,
                    })

                pagination_token = (
                    response.meta.get("next_token")
                    if response.meta else None
                )
                if not pagination_token:
                    break

            return results[:max_results]
        except tweepy.TweepyException as e:
            self._record_error(e)
            logger.warning(f"Failed to fetch following list: {e}")
            return []

    def get_user_id(self, username: str) -> Optional[str]:
        """Resolve a username to a user ID.

        Returns None if user not found or API error.
        """
        try:
            user = self.client.get_user(username=username, user_auth=True)
            self._clear_error()
            if user.data:
                return str(user.data.id)
            return None
        except tweepy.TweepyException as e:
            self._record_error(e)
            logger.debug(f"Failed to resolve username '{username}': {e}")
            return None

    def post(self, text: str) -> PostResult:
        """Post a single tweet."""
        try:
            response = self.client.create_tweet(text=text)
            self._clear_error()
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=self._status_url(tweet_id)
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def reply(self, text: str, reply_to_tweet_id: str) -> PostResult:
        """Post a reply to a specific tweet."""
        try:
            response = self.client.create_tweet(
                text=text,
                in_reply_to_tweet_id=reply_to_tweet_id
            )
            self._clear_error()
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=self._status_url(tweet_id)
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def quote_post(self, text: str, quoted_tweet_id: str) -> PostResult:
        """Publish a quote post that references another tweet/post."""
        try:
            response = self.client.create_tweet(
                text=text, quote_tweet_id=quoted_tweet_id
            )
            self._clear_error()
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=self._status_url(tweet_id),
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def quote_tweet(self, text: str, quote_tweet_id: str) -> PostResult:
        """Post a quote tweet.

        Kept for callers that still use Twitter-era terminology.
        """
        return self.quote_post(text, quoted_tweet_id=quote_tweet_id)

    def like(self, tweet_id: str) -> PostResult:
        """Like a tweet."""
        try:
            self.client.like(tweet_id)
            self._clear_error()
            return PostResult(success=True, tweet_id=tweet_id)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def retweet(self, tweet_id: str) -> PostResult:
        """Retweet a tweet."""
        try:
            self.client.retweet(tweet_id)
            self._clear_error()
            return PostResult(success=True, tweet_id=tweet_id)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def follow(self, user_id: str) -> PostResult:
        """Follow a user by ID."""
        try:
            self.client.follow_user(user_id)
            self._clear_error()
            return PostResult(success=True)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))

    def get_user_tweets(
        self, user_id: str, count: int = 10
    ) -> list[dict]:
        """Fetch a user's recent tweets.

        Returns list of dicts with keys: id, text, created_at,
        public_metrics, reply_settings. Empty list on error.
        """
        try:
            response = self.client.get_users_tweets(
                id=user_id,
                max_results=min(max(count, 5), 100),
                tweet_fields=[
                    "created_at", "text", "public_metrics", "reply_settings"
                ],
                user_auth=True,
            )
            self._clear_error()
            if not response.data:
                return []
            return [
                {
                    "id": str(t.id),
                    "text": t.text or "",
                    "created_at": (
                        t.created_at.isoformat() if t.created_at else ""
                    ),
                    "public_metrics": getattr(t, "public_metrics", {}) or {},
                    "reply_settings": getattr(t, "reply_settings", "everyone"),
                }
                for t in response.data[:count]
            ]
        except tweepy.TweepyException as e:
            self._record_error(e)
            logger.debug(f"Failed to fetch timeline for user {user_id}: {e}")
            return []

    def get_mentions(
        self,
        since_id: Optional[str] = None,
        max_results: int = 50,
        user_id: Optional[str] = None,
    ) -> tuple[list[dict], dict]:
        """Fetch tweets mentioning us (includes replies to our posts).

        Returns (mentions, users_by_id) where users_by_id maps author IDs
        to user data for handle lookup.
        """
        if user_id is None:
            user_id, _ = self.get_authenticated_user()
        response = self.client.get_users_mentions(
            user_id,
            since_id=since_id,
            max_results=min(max_results, 100),
            tweet_fields=["author_id", "conversation_id",
                          "in_reply_to_user_id", "created_at"],
            expansions=["author_id"],
            user_auth=True,
        )
        self._clear_error()

        mentions = []
        users_by_id = {}

        if response.includes and "users" in response.includes:
            for user in response.includes["users"]:
                users_by_id[str(user.id)] = {
                    "id": str(user.id),
                    "username": user.username,
                    "name": user.name,
                }

        if response.data:
            for tweet in response.data:
                mentions.append({
                    "id": str(tweet.id),
                    "text": tweet.text,
                    "author_id": str(tweet.author_id),
                    "conversation_id": str(tweet.conversation_id) if tweet.conversation_id else None,
                    "in_reply_to_user_id": str(tweet.in_reply_to_user_id) if tweet.in_reply_to_user_id else None,
                    "created_at": str(tweet.created_at) if tweet.created_at else None,
                })

        return mentions, users_by_id

    def post_thread(self, tweets: list[str]) -> PostResult:
        """Post a thread of tweets."""
        if not tweets:
            return PostResult(success=False, error="No tweets to post")

        try:
            previous_id = None
            first_id = None

            for tweet_text in tweets:
                response = self.client.create_tweet(
                    text=tweet_text,
                    in_reply_to_tweet_id=previous_id
                )
                self._clear_error()
                previous_id = response.data["id"]
                if first_id is None:
                    first_id = previous_id

            return PostResult(
                success=True,
                tweet_id=first_id,
                url=self._status_url(first_id)
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=self._record_error(e))


def parse_thread_content(content: str) -> list[str]:
    """Parse generated thread content into individual tweets."""
    tweets = []
    current_tweet = []

    for line in content.split("\n"):
        if re.match(r"^TWEET \d+:", line):
            if current_tweet:
                tweets.append("\n".join(current_tweet).strip())
            current_tweet = []
        else:
            current_tweet.append(line)

    if current_tweet:
        tweets.append("\n".join(current_tweet).strip())

    # Filter out empty tweets
    return [t for t in tweets if t]
