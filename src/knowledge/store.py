"""Knowledge store for accumulated insights."""

from __future__ import annotations

import logging
import math
import sqlite3
from typing import Optional, Any
from dataclasses import dataclass
from datetime import datetime, timezone

from .embeddings import (
    EmbeddingProvider,
    EmbeddingError,
    EmbeddingGenerationError,
    EmbeddingProviderUnavailableError,
    serialize_embedding,
    deserialize_embedding,
    cosine_similarity
)

logger = logging.getLogger(__name__)


@dataclass
class KnowledgeItem:
    id: Optional[int]
    source_type: str  # 'own_post', 'own_conversation', 'curated_x', 'curated_article'
    source_id: str
    source_url: Optional[str]
    author: str
    content: str
    insight: Optional[str]
    embedding: Optional[list[float]]
    attribution_required: bool
    approved: bool
    created_at: Optional[datetime]
    license: str = "attribution_required"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "author": self.author,
            "content": self.content,
            "insight": self.insight,
            "attribution_required": self.attribution_required,
            "license": self.license,
            "approved": self.approved,
        }


@dataclass(frozen=True)
class KnowledgeSearchResult:
    """Search result with tuple-compatible unpacking.

    Iteration yields (item, adjusted_score) so existing callers that unpack
    ``for item, score in results`` keep working while newer callers can inspect
    both the raw embedding similarity and freshness-adjusted score.
    """

    item: KnowledgeItem
    raw_similarity: float
    adjusted_score: float

    def __iter__(self):
        yield self.item
        yield self.adjusted_score

    def __len__(self) -> int:
        return 2

    def __getitem__(self, index: int):
        if index == 0:
            return self.item
        if index == 1:
            return self.adjusted_score
        raise IndexError(index)


class KnowledgeStore:
    STRICT_LICENSE_BEHAVIOR = "strict"
    PERMISSIVE_LICENSE_BEHAVIOR = "permissive"
    RESTRICTED_LICENSE = "restricted"

    def __init__(
        self,
        conn: sqlite3.Connection,
        embedder: EmbeddingProvider,
        freshness_half_life_days: Optional[float] = None,
    ) -> None:
        self.conn = conn
        self.embedder = embedder
        self.freshness_half_life_days = freshness_half_life_days

    @staticmethod
    def is_prompt_allowed(
        item: KnowledgeItem,
        restricted_behavior: str = STRICT_LICENSE_BEHAVIOR,
    ) -> bool:
        """Return whether a knowledge item may be injected into prompts."""
        if restricted_behavior not in {
            KnowledgeStore.STRICT_LICENSE_BEHAVIOR,
            KnowledgeStore.PERMISSIVE_LICENSE_BEHAVIOR,
        }:
            raise ValueError(
                "restricted_behavior must be 'strict' or 'permissive'"
            )

        if restricted_behavior == KnowledgeStore.PERMISSIVE_LICENSE_BEHAVIOR:
            return True

        return item.license != KnowledgeStore.RESTRICTED_LICENSE

    @staticmethod
    def filter_prompt_safe(
        items: list[tuple[KnowledgeItem, float] | KnowledgeSearchResult],
        restricted_behavior: str = STRICT_LICENSE_BEHAVIOR,
    ) -> list[tuple[KnowledgeItem, float] | KnowledgeSearchResult]:
        """Filter search results down to knowledge allowed in prompt context."""
        return [
            result
            for result in items
            if KnowledgeStore.is_prompt_allowed(result[0], restricted_behavior)
        ]

    @staticmethod
    def _row_license(row: sqlite3.Row) -> str:
        if "license" in row.keys():
            return row["license"] or "attribution_required"
        return "attribution_required"

    @staticmethod
    def _parse_created_at(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None
        return None

    @staticmethod
    def _freshness_adjusted_score(
        similarity: float,
        created_at: Any,
        half_life_days: Optional[float],
    ) -> float:
        if half_life_days is None:
            return similarity
        if half_life_days <= 0:
            raise ValueError("freshness_half_life_days must be positive when set")

        created = KnowledgeStore._parse_created_at(created_at)
        if created is None:
            return similarity

        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        age_days = max((now - created.astimezone(timezone.utc)).total_seconds(), 0) / 86400
        freshness_weight = math.pow(0.5, age_days / half_life_days)
        return similarity * freshness_weight

    def add_item(self, item: KnowledgeItem) -> int:
        """Add a knowledge item with embedding.

        Raises:
            EmbeddingGenerationError: If embedding generation fails
            EmbeddingProviderUnavailableError: If the embedding provider is unreachable
        """
        logger.debug("Adding knowledge item: source_type=%s source_id=%s", item.source_type, item.source_id)

        # Generate embedding if not provided
        if item.embedding is None:
            text_to_embed = item.insight or item.content
            item.embedding = self.embedder.embed(text_to_embed)

        embedding_blob = serialize_embedding(item.embedding)

        cursor = self.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight,
                embedding, attribution_required, license, approved)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_type, source_id) DO UPDATE SET
               content = excluded.content,
               insight = excluded.insight,
               embedding = excluded.embedding,
               attribution_required = excluded.attribution_required,
               license = excluded.license,
               approved = excluded.approved""",
            (
                item.source_type,
                item.source_id,
                item.source_url,
                item.author,
                item.content,
                item.insight,
                embedding_blob,
                1 if item.attribution_required else 0,
                item.license,
                1 if item.approved else 0
            )
        )
        self.conn.commit()
        row_id = cursor.lastrowid
        assert row_id is not None, "Failed to get row ID after insert/update"
        logger.debug("Stored knowledge item id=%d", row_id)
        return row_id

    def search_similar(
        self,
        query: str,
        source_types: Optional[list[str]] = None,
        limit: int = 5,
        min_similarity: float = 0.5,
        approved_only: bool = True,
        freshness_half_life_days: Optional[float] = None,
    ) -> list[KnowledgeSearchResult]:
        """Search for similar knowledge items.

        Raises:
            EmbeddingGenerationError: If embedding generation fails
            EmbeddingProviderUnavailableError: If the embedding provider is unreachable
        """
        logger.debug("Searching similar knowledge: query_len=%d source_types=%s limit=%d", len(query), source_types, limit)

        query_embedding = self.embedder.embed(query)

        # Build query
        sql = "SELECT * FROM knowledge WHERE embedding IS NOT NULL"
        params = []

        if source_types:
            placeholders = ",".join("?" * len(source_types))
            sql += f" AND source_type IN ({placeholders})"
            params.extend(source_types)

        if approved_only:
            sql += " AND approved = 1"

        cursor = self.conn.execute(sql, params)

        effective_half_life_days = (
            freshness_half_life_days
            if freshness_half_life_days is not None
            else self.freshness_half_life_days
        )

        # Calculate similarities
        results = []
        for row in cursor.fetchall():
            embedding = deserialize_embedding(row["embedding"])
            similarity = cosine_similarity(query_embedding, embedding)

            if similarity >= min_similarity:
                item = KnowledgeItem(
                    id=row["id"],
                    source_type=row["source_type"],
                    source_id=row["source_id"],
                    source_url=row["source_url"],
                    author=row["author"],
                    content=row["content"],
                    insight=row["insight"],
                    embedding=embedding,
                    attribution_required=bool(row["attribution_required"]),
                    approved=bool(row["approved"]),
                    created_at=row["created_at"],
                    license=self._row_license(row),
                )
                adjusted_score = self._freshness_adjusted_score(
                    similarity,
                    row["created_at"],
                    effective_half_life_days,
                )
                results.append(KnowledgeSearchResult(
                    item=item,
                    raw_similarity=similarity,
                    adjusted_score=adjusted_score,
                ))

        # Sort by adjusted score and limit. With freshness disabled, this is
        # identical to sorting by raw embedding similarity.
        results.sort(key=lambda x: x.adjusted_score, reverse=True)
        final_results = results[:limit]
        logger.debug("Found %d similar items (min_similarity=%.2f)", len(final_results), min_similarity)
        return final_results

    def get_by_source(self, source_type: str, source_id: str) -> Optional[KnowledgeItem]:
        """Get a knowledge item by source."""
        logger.debug("Looking up knowledge: source_type=%s source_id=%s", source_type, source_id)

        cursor = self.conn.execute(
            "SELECT * FROM knowledge WHERE source_type = ? AND source_id = ?",
            (source_type, source_id)
        )
        row = cursor.fetchone()
        if not row:
            return None

        embedding = None
        if row["embedding"]:
            embedding = deserialize_embedding(row["embedding"])

        return KnowledgeItem(
            id=row["id"],
            source_type=row["source_type"],
            source_id=row["source_id"],
            source_url=row["source_url"],
            author=row["author"],
            content=row["content"],
            insight=row["insight"],
            embedding=embedding,
            attribution_required=bool(row["attribution_required"]),
            approved=bool(row["approved"]),
            created_at=row["created_at"],
            license=self._row_license(row),
        )

    def exists(self, source_type: str, source_id: str) -> bool:
        """Check if a knowledge item exists."""
        cursor = self.conn.execute(
            "SELECT 1 FROM knowledge WHERE source_type = ? AND source_id = ?",
            (source_type, source_id)
        )
        return cursor.fetchone() is not None

    def get_own_insights(self, limit: int = 50) -> list[KnowledgeItem]:
        """Get recent insights from own content."""
        cursor = self.conn.execute(
            """SELECT * FROM knowledge
               WHERE source_type IN ('own_post', 'own_conversation')
               ORDER BY created_at DESC LIMIT ?""",
            (limit,)
        )

        items = []
        for row in cursor.fetchall():
            embedding = None
            if row["embedding"]:
                embedding = deserialize_embedding(row["embedding"])

            items.append(KnowledgeItem(
                id=row["id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                source_url=row["source_url"],
                author=row["author"],
                content=row["content"],
                insight=row["insight"],
                embedding=embedding,
                attribution_required=bool(row["attribution_required"]),
                approved=bool(row["approved"]),
                created_at=row["created_at"],
                license=self._row_license(row),
            ))
        return items

    def get_recent_by_source_type(
        self,
        source_type: str,
        limit: int = 20,
        max_age_hours: int = 72,
        prompt_safe: bool = False,
        restricted_behavior: str = STRICT_LICENSE_BEHAVIOR,
    ) -> list[KnowledgeItem]:
        """Get recent knowledge items by source type, ordered by recency.

        Used for trend context: fetch recent curated tweets without
        requiring a semantic search query.
        """
        cursor = self.conn.execute(
            """SELECT * FROM knowledge
               WHERE source_type = ?
                 AND approved = 1
                 AND created_at >= datetime('now', ?)
               ORDER BY created_at DESC
               LIMIT ?""",
            (source_type, f'-{max_age_hours} hours', limit)
        )
        items = []
        for row in cursor.fetchall():
            items.append(KnowledgeItem(
                id=row["id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                source_url=row["source_url"],
                author=row["author"],
                content=row["content"],
                insight=row["insight"],
                embedding=None,  # Skip deserialization — not needed for trend context
                attribution_required=bool(row["attribution_required"]),
                approved=bool(row["approved"]),
                created_at=row["created_at"],
                license=self._row_license(row),
            ))
        if not prompt_safe:
            return items

        return [
            item
            for item in items
            if self.is_prompt_allowed(item, restricted_behavior)
        ]

    def link_to_content(self, content_id: int, knowledge_id: int, relevance: float) -> None:
        """Track which knowledge was used in generated content."""
        logger.debug("Linking knowledge %d to content %d (relevance=%.2f)", knowledge_id, content_id, relevance)

        self.conn.execute(
            """INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score)
               VALUES (?, ?, ?)""",
            (content_id, knowledge_id, relevance)
        )
        self.conn.commit()
