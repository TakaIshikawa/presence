"""Cross-posting helper for publishing content to multiple platforms."""

import re
import unicodedata
from typing import Optional
from .x_client import XClient, PostResult
from .bluesky_client import BlueskyClient, BlueskyPostResult


def count_graphemes(text: str) -> int:
    """Count grapheme clusters in text (Bluesky uses grapheme length)."""
    # Simple grapheme counting using Unicode grapheme breaks
    # This is a basic implementation; production might use the 'grapheme' package
    return len(list(unicodedata.normalize('NFC', text)))


class CrossPoster:
    """Publishes content to multiple platforms."""

    def __init__(
        self,
        x_client: Optional[XClient] = None,
        bluesky_client: Optional[BlueskyClient] = None
    ):
        self.x_client = x_client
        self.bluesky_client = bluesky_client

    def adapt_for_bluesky(self, text: str, content_type: str = "x_post") -> str:
        """Adapt X content for Bluesky (300 grapheme limit).

        Bluesky has a 300 grapheme limit vs X's 280 character limit.
        Content is generated platform-agnostically, so minimal adaptation
        is needed.

        Args:
            text: Original text content
            content_type: Type of content (x_post, x_thread, etc.)

        Returns:
            Adapted text suitable for Bluesky
        """
        adapted = text

        # Truncate at 300 graphemes if needed
        grapheme_count = count_graphemes(adapted)
        if grapheme_count > 300:
            # Simple truncation - could be smarter about sentence boundaries
            # For now, just cut at 297 graphemes and add ellipsis
            # Normalize once, then slice (after NFC normalization, len == grapheme count)
            normalized = unicodedata.normalize('NFC', adapted)
            adapted = normalized[:297] + '...'

        return adapted

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
