"""Audit newsletter sends for source mix and evidence diversity."""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any


MIN_SOURCE_COUNT = 3
SINGLE_TOPIC_HEAVY_SHARE = 0.75


@dataclass
class NewsletterSourceMixRow:
    """Source composition for one newsletter send."""

    newsletter_send_id: int
    issue_id: str
    subject: str
    sent_at: str
    status: str
    source_content_ids: list[int]
    source_count: int
    found_source_count: int
    missing_source_ids: list[int] = field(default_factory=list)
    x_post_count: int = 0
    thread_count: int = 0
    blog_post_count: int = 0
    other_content_count: int = 0
    topic_distribution: dict[str, int] = field(default_factory=dict)
    knowledge_backed_item_count: int = 0
    warnings: list[str] = field(default_factory=list)


class NewsletterSourceMix:
    """Compute per-send source diversity metrics for recent newsletters."""

    def __init__(self, db) -> None:
        self.db = db

    def summarize(
        self, days: int = 30, limit: int | None = None
    ) -> list[NewsletterSourceMixRow]:
        """Return source composition rows newest-first."""
        rows = self._load_sends(days=days, limit=limit)
        return [self._summarize_send(dict(row)) for row in rows]

    def _load_sends(self, days: int, limit: int | None) -> list[Any]:
        sql = """SELECT id, issue_id, subject, source_content_ids, status, sent_at
                 FROM newsletter_sends
                 WHERE sent_at >= datetime('now', ?)
                 ORDER BY sent_at DESC, id DESC"""
        params: list[Any] = [f"-{days} days"]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        return self.db.conn.execute(sql, params).fetchall()

    def _summarize_send(self, send: dict[str, Any]) -> NewsletterSourceMixRow:
        source_ids, parse_warnings = parse_source_content_ids(
            send.get("source_content_ids")
        )
        content_rows = self._load_content(source_ids)
        topics = self._load_topics(source_ids)
        knowledge_backed_ids = self._load_knowledge_backed_ids(source_ids)

        found_ids = set(content_rows)
        missing_source_ids = sorted(
            {content_id for content_id in source_ids if content_id not in found_ids}
        )
        content_type_counts = Counter(
            content_rows[content_id].get("content_type") or "unknown"
            for content_id in source_ids
            if content_id in content_rows
        )
        topic_distribution = {
            topic: count for topic, count in sorted(Counter(topics).items())
        }
        knowledge_backed_item_count = sum(
            1 for content_id in source_ids if content_id in knowledge_backed_ids
        )

        warnings = set(parse_warnings)
        if len(source_ids) < MIN_SOURCE_COUNT:
            warnings.add("too_few_sources")
        if missing_source_ids:
            warnings.add("missing_source_rows")
        if source_ids and knowledge_backed_item_count == 0:
            warnings.add("no_knowledge_links")
        if _is_single_topic_heavy(topic_distribution, max(len(source_ids), 1)):
            warnings.add("single_topic_heavy")

        x_post_count = content_type_counts.get("x_post", 0)
        thread_count = content_type_counts.get("x_thread", 0)
        blog_post_count = content_type_counts.get("blog_post", 0)
        known_count = x_post_count + thread_count + blog_post_count

        return NewsletterSourceMixRow(
            newsletter_send_id=int(send["id"]),
            issue_id=send.get("issue_id") or "",
            subject=send.get("subject") or "",
            sent_at=send.get("sent_at") or "",
            status=send.get("status") or "",
            source_content_ids=source_ids,
            source_count=len(source_ids),
            found_source_count=sum(
                1 for content_id in source_ids if content_id in found_ids
            ),
            missing_source_ids=missing_source_ids,
            x_post_count=x_post_count,
            thread_count=thread_count,
            blog_post_count=blog_post_count,
            other_content_count=sum(content_type_counts.values()) - known_count,
            topic_distribution=topic_distribution,
            knowledge_backed_item_count=knowledge_backed_item_count,
            warnings=sorted(warnings),
        )

    def _load_content(self, source_ids: list[int]) -> dict[int, dict[str, Any]]:
        if not source_ids:
            return {}
        placeholders = ",".join("?" for _ in sorted(set(source_ids)))
        rows = self.db.conn.execute(
            f"""SELECT id, content_type
                FROM generated_content
                WHERE id IN ({placeholders})""",
            sorted(set(source_ids)),
        ).fetchall()
        return {int(row["id"]): dict(row) for row in rows}

    def _load_topics(self, source_ids: list[int]) -> list[str]:
        if not source_ids:
            return []
        placeholders = ",".join("?" for _ in sorted(set(source_ids)))
        rows = self.db.conn.execute(
            f"""SELECT topic
                FROM content_topics
                WHERE content_id IN ({placeholders})
                ORDER BY topic""",
            sorted(set(source_ids)),
        ).fetchall()
        return [row["topic"] for row in rows if row["topic"]]

    def _load_knowledge_backed_ids(self, source_ids: list[int]) -> set[int]:
        if not source_ids:
            return set()
        placeholders = ",".join("?" for _ in sorted(set(source_ids)))
        rows = self.db.conn.execute(
            f"""SELECT DISTINCT content_id
                FROM content_knowledge_links
                WHERE content_id IN ({placeholders})""",
            sorted(set(source_ids)),
        ).fetchall()
        return {int(row["content_id"]) for row in rows}


def parse_source_content_ids(raw_value: Any) -> tuple[list[int], list[str]]:
    """Parse newsletter_sends.source_content_ids without raising on bad data."""
    if raw_value in (None, ""):
        return [], []
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError):
        return [], ["malformed_source_content_ids"]

    if not isinstance(parsed, list):
        return [], ["malformed_source_content_ids"]

    source_ids: list[int] = []
    malformed = False
    for item in parsed:
        try:
            content_id = int(item)
        except (TypeError, ValueError):
            malformed = True
            continue
        if content_id <= 0:
            malformed = True
            continue
        source_ids.append(content_id)

    warnings = ["malformed_source_content_ids"] if malformed else []
    return source_ids, warnings


def _is_single_topic_heavy(
    topic_distribution: dict[str, int], source_count: int
) -> bool:
    if source_count < MIN_SOURCE_COUNT or not topic_distribution:
        return False
    return max(topic_distribution.values()) / source_count >= SINGLE_TOPIC_HEAVY_SHARE
