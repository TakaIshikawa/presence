"""Cross-posting helper for publishing content to multiple platforms."""

from typing import Optional
from .platform_adapter import BlueskyPlatformAdapter, count_graphemes
from .x_client import XClient, PostResult
from .bluesky_client import BlueskyClient, BlueskyPostResult


def _variant_type_for_content_type(content_type: str) -> str:
    if content_type == "x_thread":
        return "thread"
    return "post"


def _thread_variant_content(posts: list[str]) -> str:
    return "\n".join(
        f"TWEET {index}:\n{post}" for index, post in enumerate(posts, start=1)
    )


class CrossPoster:
    """Publishes content to multiple platforms."""

    def __init__(
        self,
        x_client: Optional[XClient] = None,
        bluesky_client: Optional[BlueskyClient] = None,
        platform_adapter: Optional[BlueskyPlatformAdapter] = None,
        divergence_analyzer: object = None,
        db: object = None,
    ):
        self.x_client = x_client
        self.bluesky_client = bluesky_client
        self.db = db
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

    def _persist_bluesky_variant(
        self,
        *,
        content_id: int | None,
        content_type: str,
        source: str | list[str],
        adapted: str | list[str],
    ) -> None:
        if content_id is None or self.db is None:
            return

        variant_type = _variant_type_for_content_type(content_type)
        source_text = (
            _thread_variant_content(source)
            if isinstance(source, list)
            else source
        )
        adapted_text = (
            _thread_variant_content(adapted)
            if isinstance(adapted, list)
            else adapted
        )
        source_graphemes = count_graphemes(source_text)
        adapted_graphemes = count_graphemes(adapted_text)
        grapheme_limit = getattr(self.platform_adapter, "grapheme_limit", None)
        metadata = {
            "source_content_type": content_type,
            "adapter": type(self.platform_adapter).__name__,
            "source_graphemes": source_graphemes,
            "adapted_graphemes": adapted_graphemes,
            "was_changed": adapted_text != source_text,
            "was_trimmed": (
                source_graphemes > grapheme_limit
                if isinstance(grapheme_limit, int)
                else adapted_graphemes < source_graphemes
            ),
        }
        if isinstance(adapted, list):
            metadata["part_count"] = len(adapted)

        self.db.upsert_content_variant(
            content_id=content_id,
            platform="bluesky",
            variant_type=variant_type,
            content=adapted_text,
            metadata=metadata,
        )

    def publish(
        self,
        content: str,
        content_type: str,
        tweets: list[str] = None,
        content_id: int | None = None,
        image_path: Optional[str] = None,
        image_alt_text: Optional[str] = None,
    ) -> dict:
        """Publish to all configured platforms.

        Args:
            content: Raw content text
            content_type: Type of content (x_post, x_thread, etc.)
            tweets: Pre-split tweets for thread posting
            image_path: Optional local image path for visual posts
            image_alt_text: Alt text for the image

        Returns:
            Dictionary mapping platform name to PostResult/BlueskyPostResult
        """
        results = {}

        # Publish to X/Twitter
        if self.x_client:
            if content_type == 'x_thread' and tweets:
                results['x'] = self.x_client.post_thread(tweets)
            elif image_path and hasattr(self.x_client, "post_with_media"):
                results['x'] = self.x_client.post_with_media(
                    content,
                    image_path,
                    alt_text=image_alt_text or "",
                )
            else:
                results['x'] = self.x_client.post(content)

        # Cross-post to Bluesky
        if self.bluesky_client:
            if content_type == 'x_thread' and tweets:
                # Adapt each tweet in the thread for Bluesky
                bsky_tweets = [
                    self.adapt_for_bluesky(t, content_type) for t in tweets
                ]
                self._persist_bluesky_variant(
                    content_id=content_id,
                    content_type=content_type,
                    source=tweets,
                    adapted=bsky_tweets,
                )
                results['bluesky'] = self.bluesky_client.post_thread(bsky_tweets)
            else:
                adapted = self.adapt_for_bluesky(content, content_type)
                self._persist_bluesky_variant(
                    content_id=content_id,
                    content_type=content_type,
                    source=content,
                    adapted=adapted,
                )
                if image_path and hasattr(self.bluesky_client, "post_with_media"):
                    results['bluesky'] = self.bluesky_client.post_with_media(
                        adapted,
                        image_path,
                        alt_text=image_alt_text or "",
                    )
                else:
                    results['bluesky'] = self.bluesky_client.post(adapted)

        return results
