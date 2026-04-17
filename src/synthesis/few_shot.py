"""Few-shot example selection for content generation."""

import re
from dataclasses import dataclass
from storage.db import Database
from synthesis.stale_patterns import has_stale_pattern


# Topic keyword used for quota selection. Matches existing
# _build_avoidance_context() heuristic to keep diversity logic aligned.
AGENT_PATTERN = re.compile(r"\bagent", re.IGNORECASE)


@dataclass
class FewShotExample:
    content: str
    engagement_score: float


class FewShotSelector:
    """Selects high-performing posts as few-shot examples for generation prompts."""

    def __init__(self, db: Database):
        self.db = db

    def get_examples(
        self,
        content_type: str = "x_post",
        limit: int = 3,
        exclude_ids: set[int] = None,
        max_per_topic: int = 2,
    ) -> list[FewShotExample]:
        """Get top-performing posts as few-shot examples.

        Uses engagement data when available, falls back to eval scores.
        Excludes posts flagged as too_specific via exclude_ids.
        Filters out posts matching stale rhetorical patterns.

        Applies topic quota to avoid overfitting generation to a single theme:
        when non-agent posts are available, at most ``max_per_topic`` of the
        examples will be agent-themed. Falls back to engagement order when
        only one topic bucket has content.
        """
        # Fetch more than needed to allow for pattern filtering
        fetch_limit = limit * 4
        top_posts = self.db.get_top_performing_posts(
            limit=fetch_limit, content_type=content_type
        )

        if top_posts:
            # Filter excluded + stale, preserving engagement order
            filtered = []
            for p in top_posts:
                if exclude_ids and p["id"] in exclude_ids:
                    continue
                if has_stale_pattern(p["content"]):
                    continue
                filtered.append(p)

            selected = self._apply_topic_quota(filtered, limit, max_per_topic)
            if selected:
                return [
                    FewShotExample(
                        content=p["content"],
                        engagement_score=p["engagement_score"],
                    )
                    for p in selected
                ]

        # Cold start: fall back to highest eval scores among published posts
        return self._fallback_by_eval_score(content_type, limit, exclude_ids)

    @staticmethod
    def _apply_topic_quota(
        candidates: list[dict],
        limit: int,
        max_per_topic: int,
    ) -> list[dict]:
        """Pick up to ``limit`` posts, capping agent-themed at ``max_per_topic``.

        Within each topic bucket the original engagement order is preserved.
        If the non-agent bucket is empty, backfills from remaining agent posts
        so callers get ``limit`` examples whenever enough material exists.
        """
        agent_pool = [p for p in candidates if AGENT_PATTERN.search(p["content"])]
        other_pool = [p for p in candidates if not AGENT_PATTERN.search(p["content"])]

        selected: list[dict] = []
        selected.extend(agent_pool[:max_per_topic])
        remaining = limit - len(selected)
        if remaining > 0:
            selected.extend(other_pool[:remaining])

        # Backfill from leftover agent posts if non-agent pool was too small
        if len(selected) < limit:
            needed = limit - len(selected)
            selected.extend(agent_pool[max_per_topic : max_per_topic + needed])

        return selected[:limit]

    def _fallback_by_eval_score(
        self, content_type: str, limit: int, exclude_ids: set[int] = None
    ) -> list[FewShotExample]:
        """Fallback: select examples by eval score when no engagement data exists."""
        fetch_limit = limit * 4 + (len(exclude_ids) if exclude_ids else 0)
        cursor = self.db.conn.execute(
            """SELECT id, content, eval_score FROM generated_content
               WHERE content_type = ? AND published = 1
                 AND COALESCE(curation_quality, '') != 'too_specific'
               ORDER BY eval_score DESC
               LIMIT ?""",
            (content_type, fetch_limit),
        )
        examples = []
        for row in cursor.fetchall():
            if exclude_ids and row["id"] in exclude_ids:
                continue
            if has_stale_pattern(row["content"]):
                continue
            examples.append(FewShotExample(content=row["content"], engagement_score=0.0))
            if len(examples) >= limit:
                break
        return examples

    def format_examples(self, examples: list[FewShotExample]) -> str:
        """Format examples for injection into a generation prompt."""
        if not examples:
            return ""
        lines = []
        for i, ex in enumerate(examples, 1):
            lines.append(f"{i}. {ex.content}")
        return "\n\n".join(lines)
