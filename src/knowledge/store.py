"""Knowledge store for accumulated insights."""

import sqlite3
from typing import Optional
from dataclasses import dataclass
from datetime import datetime

from .embeddings import (
    EmbeddingProvider,
    serialize_embedding,
    deserialize_embedding,
    cosine_similarity
)


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

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "author": self.author,
            "content": self.content,
            "insight": self.insight,
            "attribution_required": self.attribution_required,
            "approved": self.approved,
        }


class KnowledgeStore:
    def __init__(self, conn: sqlite3.Connection, embedder: EmbeddingProvider):
        self.conn = conn
        self.embedder = embedder

    def add_item(self, item: KnowledgeItem) -> int:
        """Add a knowledge item with embedding."""
        # Generate embedding if not provided
        if item.embedding is None:
            text_to_embed = item.insight or item.content
            item.embedding = self.embedder.embed(text_to_embed)

        embedding_blob = serialize_embedding(item.embedding)

        cursor = self.conn.execute(
            """INSERT INTO knowledge
               (source_type, source_id, source_url, author, content, insight,
                embedding, attribution_required, approved)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(source_type, source_id) DO UPDATE SET
               content = excluded.content,
               insight = excluded.insight,
               embedding = excluded.embedding""",
            (
                item.source_type,
                item.source_id,
                item.source_url,
                item.author,
                item.content,
                item.insight,
                embedding_blob,
                1 if item.attribution_required else 0,
                1 if item.approved else 0
            )
        )
        self.conn.commit()
        return cursor.lastrowid

    def search_similar(
        self,
        query: str,
        source_types: Optional[list[str]] = None,
        limit: int = 5,
        min_similarity: float = 0.5,
        approved_only: bool = True
    ) -> list[tuple[KnowledgeItem, float]]:
        """Search for similar knowledge items."""
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
                    created_at=row["created_at"]
                )
                results.append((item, similarity))

        # Sort by similarity and limit
        results.sort(key=lambda x: x[1], reverse=True)
        return results[:limit]

    def get_by_source(self, source_type: str, source_id: str) -> Optional[KnowledgeItem]:
        """Get a knowledge item by source."""
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
            created_at=row["created_at"]
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
                created_at=row["created_at"]
            ))
        return items

    def link_to_content(self, content_id: int, knowledge_id: int, relevance: float) -> None:
        """Track which knowledge was used in generated content."""
        self.conn.execute(
            """INSERT INTO content_knowledge_links (content_id, knowledge_id, relevance_score)
               VALUES (?, ?, ?)""",
            (content_id, knowledge_id, relevance)
        )
        self.conn.commit()
