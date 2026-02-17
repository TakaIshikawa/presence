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
