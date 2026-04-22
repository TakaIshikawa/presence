"""Ingest content into knowledge base."""

import anthropic
from anthropic import APIError, APIConnectionError, RateLimitError, AuthenticationError
from typing import Optional
from dataclasses import dataclass

from .store import KnowledgeStore, KnowledgeItem


class InsightExtractionError(Exception):
    """Raised when insight extraction from content fails."""
    pass


@dataclass
class InsightExtractor:
    """Extract insights from content using Claude."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-6", timeout: float = 300.0):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def extract_insight(self, content: str, context: Optional[str] = None) -> str:
        """Extract the key insight from content."""
        prompt = f"""Extract the key insight or learning from this content.
Focus on:
- What's the core technical or strategic insight?
- What pattern or principle does this reveal?
- What would be valuable for someone building AI agents to know?

Return ONLY the insight in 1-2 sentences, no preamble.

Content:
{content}
"""
        if context:
            prompt += f"\nContext: {context}"

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=200,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text.strip()
        except APIConnectionError as e:
            raise InsightExtractionError(
                f"Failed to connect to Anthropic API: {e}"
            ) from e
        except RateLimitError as e:
            raise InsightExtractionError(
                f"Anthropic API rate limit exceeded: {e}"
            ) from e
        except AuthenticationError as e:
            raise InsightExtractionError(
                f"Anthropic API authentication failed: {e}"
            ) from e
        except APIError as e:
            raise InsightExtractionError(
                f"Anthropic API error: {e}"
            ) from e


def ingest_own_post(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    post_id: str,
    content: str,
    url: str,
    author: str
) -> Optional[int]:
    """Ingest an own X post into knowledge base."""
    if store.exists("own_post", post_id):
        return None

    insight = extractor.extract_insight(content)

    item = KnowledgeItem(
        id=None,
        source_type="own_post",
        source_id=post_id,
        source_url=url,
        author=author,
        content=content,
        insight=insight,
        embedding=None,  # Will be generated
        attribution_required=False,
        approved=True,  # Own content is auto-approved
        created_at=None,
        license="open",
    )
    return store.add_item(item)


def ingest_own_conversation(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    message_uuid: str,
    prompt: str,
    project_path: str
) -> Optional[int]:
    """Ingest a Claude Code conversation prompt into knowledge base."""
    if store.exists("own_conversation", message_uuid):
        return None

    # Only extract insights from substantial prompts
    if len(prompt) < 50:
        return None

    insight = extractor.extract_insight(prompt, context=f"Project: {project_path}")

    item = KnowledgeItem(
        id=None,
        source_type="own_conversation",
        source_id=message_uuid,
        source_url=None,
        author="self",
        content=prompt,
        insight=insight,
        embedding=None,
        attribution_required=False,
        approved=True,
        created_at=None,
        license="open",
    )
    return store.add_item(item)


def ingest_curated_post(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    post_id: str,
    content: str,
    url: str,
    author: str,
    license_type: str = "attribution_required",
    published_at: str | None = None,
) -> Optional[int]:
    """Ingest a curated external X post into knowledge base."""
    if store.exists("curated_x", post_id):
        return None

    insight = extractor.extract_insight(content, context=f"Author: {author}")

    item = KnowledgeItem(
        id=None,
        source_type="curated_x",
        source_id=post_id,
        source_url=url,
        author=author,
        content=content,
        insight=insight,
        embedding=None,
        attribution_required=(license_type != "open"),
        approved=True,  # Curated = pre-approved
        created_at=None,
        published_at=published_at,
        license=license_type,
    )
    return store.add_item(item)


def ingest_curated_article(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    url: str,
    content: str,
    title: str,
    author: str,
    license_type: str = "attribution_required",
    published_at: str | None = None,
) -> Optional[int]:
    """Ingest a curated article/blog post into knowledge base."""
    if store.exists("curated_article", url):
        return None

    insight = extractor.extract_insight(
        content[:2000],  # Limit content length
        context=f"Article: {title} by {author}"
    )

    item = KnowledgeItem(
        id=None,
        source_type="curated_article",
        source_id=url,
        source_url=url,
        author=author,
        content=content[:5000],  # Store truncated
        insight=insight,
        embedding=None,
        attribution_required=(license_type != "open"),
        approved=True,
        created_at=None,
        published_at=published_at,
        license=license_type,
    )
    return store.add_item(item)


def ingest_curated_newsletter(
    store: KnowledgeStore,
    extractor: InsightExtractor,
    url: str,
    content: str,
    title: str,
    author: str,
    license_type: str = "attribution_required",
    published_at: str | None = None,
) -> Optional[int]:
    """Ingest a curated newsletter issue into knowledge base."""
    if store.exists("curated_newsletter", url):
        return None

    insight = extractor.extract_insight(
        content[:2000],
        context=f"Newsletter: {title} by {author}"
    )

    item = KnowledgeItem(
        id=None,
        source_type="curated_newsletter",
        source_id=url,
        source_url=url,
        author=author,
        content=content[:5000],
        insight=insight,
        embedding=None,
        attribution_required=(license_type != "open"),
        approved=True,
        created_at=None,
        published_at=published_at,
        license=license_type,
    )
    return store.add_item(item)
