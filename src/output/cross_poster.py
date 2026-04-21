"""Cross-posting helper for publishing content to multiple platforms."""

from typing import Optional
from .platform_adapter import BlueskyPlatformAdapter, count_graphemes
from .x_client import XClient, PostResult
from .bluesky_client import BlueskyClient, BlueskyPostResult


class CrossPoster:
    """Publishes content to multiple platforms."""

    def __init__(
        self,
        x_client: Optional[XClient] = None,
        bluesky_client: Optional[BlueskyClient] = None,
        platform_adapter: Optional[BlueskyPlatformAdapter] = None,
        divergence_analyzer: object = None,
    ):
        self.x_client = x_client
        self.bluesky_client = bluesky_client
        self.platform_adapter = platform_adapter or BlueskyPlatformAdapter(
            context_provider=divergence_analyzer
        )

    def adapt_for_bluesky(self, text: str, content_type: str = "x_post") -> str:
        """Adapt X content for Bluesky (300 grapheme limit).

        This uses a deterministic platform adapter by default. If the adapter
        is configured with a PlatformDivergenceAnalyzer, its adaptation context
        can influence rule-based cleanup without requiring an LLM.

        Args:
            text: Original text content
            content_type: Type of content (x_post, x_thread, etc.)

        Returns:
            Adapted text suitable for Bluesky
        """
        return self.platform_adapter.adapt(text, content_type)

    def publish(
        self,
        content: str,
        content_type: str,
        tweets: list[str] = None
    ) -> dict:
        """Publish to all configured platforms.

        Args:
            content: Raw content text
            content_type: Type of content (x_post, x_thread, etc.)
            tweets: Pre-split tweets for thread posting

        Returns:
            Dictionary mapping platform name to PostResult/BlueskyPostResult
        """
        results = {}

        # Publish to X/Twitter
        if self.x_client:
            if content_type == 'x_thread' and tweets:
                results['x'] = self.x_client.post_thread(tweets)
            else:
                results['x'] = self.x_client.post(content)

        # Cross-post to Bluesky
        if self.bluesky_client:
            if content_type == 'x_thread' and tweets:
                # Adapt each tweet in the thread for Bluesky
                bsky_tweets = [
                    self.adapt_for_bluesky(t, content_type) for t in tweets
                ]
                results['bluesky'] = self.bluesky_client.post_thread(bsky_tweets)
            else:
                adapted = self.adapt_for_bluesky(content, content_type)
                results['bluesky'] = self.bluesky_client.post(adapted)

        return results
