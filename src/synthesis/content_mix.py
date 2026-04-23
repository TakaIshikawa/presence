"""Operational content mix planning for the auto pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContentMixDecision:
    content_type: str
    reason: str


@dataclass(frozen=True)
class ContentMixSnapshot:
    recent_limit: int
    recent_content_types: list[str]
    counts: dict[str, int]


class ContentMixPlanner:
    """Choose what kind of content the next automatic run should produce."""

    CONTENT_TYPES = ("x_post", "x_thread", "x_visual", "blog_post")

    def __init__(
        self,
        db,
        thread_token_threshold: int = 1400,
        recent_limit: int = 6,
        visual_token_threshold: int = 1800,
        blog_token_threshold: int = 3600,
    ):
        self.db = db
        self.thread_token_threshold = thread_token_threshold
        self.recent_limit = recent_limit
        self.visual_token_threshold = visual_token_threshold
        self.blog_token_threshold = blog_token_threshold

    def snapshot(self) -> ContentMixSnapshot:
        recent = self._recent_content_types()
        counts = {content_type: recent.count(content_type) for content_type in self.CONTENT_TYPES}
        return ContentMixSnapshot(
            recent_limit=self.recent_limit,
            recent_content_types=recent,
            counts=counts,
        )

    def choose(
        self,
        accumulated_tokens: int,
        has_prompts: bool,
    ) -> ContentMixDecision:
        snapshot = self.snapshot()
        recent = snapshot.recent_content_types
        recent_threads = snapshot.counts["x_thread"]
        recent_posts = snapshot.counts["x_post"]
        recent_visuals = snapshot.counts["x_visual"]
        recent_blog_posts = snapshot.counts["blog_post"]

        if not has_prompts:
            return ContentMixDecision(
                "x_post",
                "No prompt context available; keep output short if generation is forced.",
            )

        if (
            recent
            and accumulated_tokens >= self.blog_token_threshold
            and (
                recent_blog_posts >= 1
                or recent_visuals >= 1
                or recent_threads >= 2
                or recent_posts >= 2
            )
        ):
            return ContentMixDecision(
                "blog_post",
                "Recent mix has enough breadth for a durable blog post, and the source depth is strong.",
            )

        if (
            recent
            and self.visual_token_threshold <= accumulated_tokens < (self.thread_token_threshold + 900)
            and recent_blog_posts == 0
            and recent_visuals == 0
            and (recent_posts >= 2 or recent_threads >= 1)
        ):
            return ContentMixDecision(
                "x_visual",
                "Moderate depth plus a text-heavy recent mix supports a visual pattern interrupt.",
            )

        if recent and recent_threads >= max(2, len(recent) // 2 + 1):
            return ContentMixDecision(
                "x_post",
                "Recent mix is thread-heavy; use a shorter post to avoid presence fatigue.",
            )

        if accumulated_tokens >= self.thread_token_threshold:
            return ContentMixDecision(
                "x_thread",
                f"{accumulated_tokens} tokens of source material supports a thread.",
            )

        if recent_posts >= 3 and accumulated_tokens >= self.thread_token_threshold * 0.7:
            return ContentMixDecision(
                "x_thread",
                "Recent mix has enough short posts and this run has moderate source depth.",
            )

        return ContentMixDecision(
            "x_post",
            f"{accumulated_tokens} tokens is better suited to one sharp post.",
        )

    def _recent_content_types(self) -> list[str]:
        conn = getattr(self.db, "conn", None)
        if conn is None:
            method = getattr(self.db, "get_recent_published_content_all", None)
            if not callable(method):
                return []
            try:
                rows = method(limit=self.recent_limit)
            except Exception:
                return []
            return [
                row.get("content_type")
                for row in rows
                if isinstance(row, dict) and row.get("content_type") in self.CONTENT_TYPES
            ]

        placeholders = ",".join("?" for _ in self.CONTENT_TYPES)
        try:
            cursor = conn.execute(
                f"""SELECT content_type
                    FROM generated_content
                    WHERE published = 1
                      AND content_type IN ({placeholders})
                    ORDER BY published_at DESC, id DESC
                    LIMIT ?""",
                (*self.CONTENT_TYPES, self.recent_limit),
            )
            rows = cursor.fetchall()
        except Exception:
            return []

        def _content_type(row: object) -> str | None:
            if isinstance(row, dict):
                value = row.get("content_type")
            else:
                try:
                    value = row["content_type"]  # type: ignore[index]
                except Exception:
                    value = None
            return value if value in self.CONTENT_TYPES else None

        return [content_type for row in rows if (content_type := _content_type(row))]
