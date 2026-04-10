"""X (Twitter) API client for posting content."""

import re
import tweepy
from typing import Optional
from dataclasses import dataclass


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
        self.client = tweepy.Client(
            consumer_key=api_key,
            consumer_secret=api_secret,
            access_token=access_token,
            access_token_secret=access_token_secret
        )
        self._username: Optional[str] = None

    @property
    def username(self) -> str:
        if not self._username:
            me = self.client.get_me()
            self._username = me.data.username
        return self._username

    def post(self, text: str) -> PostResult:
        """Post a single tweet."""
        try:
            response = self.client.create_tweet(text=text)
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=f"https://x.com/{self.username}/status/{tweet_id}"
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

    def reply(self, text: str, reply_to_tweet_id: str) -> PostResult:
        """Post a reply to a specific tweet."""
        try:
            response = self.client.create_tweet(
                text=text,
                in_reply_to_tweet_id=reply_to_tweet_id
            )
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=f"https://x.com/{self.username}/status/{tweet_id}"
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

    def quote_tweet(self, text: str, quote_tweet_id: str) -> PostResult:
        """Post a quote tweet."""
        try:
            response = self.client.create_tweet(
                text=text, quote_tweet_id=quote_tweet_id
            )
            tweet_id = response.data["id"]
            return PostResult(
                success=True,
                tweet_id=tweet_id,
                url=f"https://x.com/{self.username}/status/{tweet_id}",
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

    def like(self, tweet_id: str) -> PostResult:
        """Like a tweet."""
        try:
            self.client.like(tweet_id)
            return PostResult(success=True, tweet_id=tweet_id)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

    def retweet(self, tweet_id: str) -> PostResult:
        """Retweet a tweet."""
        try:
            self.client.retweet(tweet_id)
            return PostResult(success=True, tweet_id=tweet_id)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

    def follow(self, user_id: str) -> PostResult:
        """Follow a user by ID."""
        try:
            self.client.follow_user(user_id)
            return PostResult(success=True)
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))

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
        except tweepy.TweepyException:
            return []

    def get_mentions(
        self, since_id: Optional[str] = None, max_results: int = 50
    ) -> tuple[list[dict], dict]:
        """Fetch tweets mentioning us (includes replies to our posts).

        Returns (mentions, users_by_id) where users_by_id maps author IDs
        to user data for handle lookup.
        """
        me = self.client.get_me()
        response = self.client.get_users_mentions(
            me.data.id,
            since_id=since_id,
            max_results=min(max_results, 100),
            tweet_fields=["author_id", "conversation_id",
                          "in_reply_to_user_id", "created_at"],
            expansions=["author_id"],
            user_auth=True,
        )

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
                previous_id = response.data["id"]
                if first_id is None:
                    first_id = previous_id

            return PostResult(
                success=True,
                tweet_id=first_id,
                url=f"https://x.com/{self.username}/status/{first_id}"
            )
        except tweepy.TweepyException as e:
            return PostResult(success=False, error=str(e))


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
