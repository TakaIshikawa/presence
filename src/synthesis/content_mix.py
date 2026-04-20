"""Operational content mix planning for the auto pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ContentMixDecision:
    content_type: str
    reason: str


class ContentMixPlanner:
    """Choose what kind of content the next automatic run should produce."""

    def __init__(
        self,
        db,
        thread_token_threshold: int = 1400,
        recent_limit: int = 6,
    ):
        self.db = db
        self.thread_token_threshold = thread_token_threshold
        self.recent_limit = recent_limit

    def choose(
        self,
        accumulated_tokens: int,
        has_prompts: bool,
    ) -> ContentMixDecision:
        recent = self._recent_content_types()
        recent_threads = recent.count("x_thread")
        recent_posts = recent.count("x_post")

        if not has_prompts:
            return ContentMixDecision(
                "x_post",
                "No prompt context available; keep output short if generation is forced.",
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
            if isinstance(row, dict) and row.get("content_type") in {"x_post", "x_thread"}
        ]
