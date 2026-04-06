"""Few-shot example selection for content generation."""

from dataclasses import dataclass
from storage.db import Database
from synthesis.stale_patterns import has_stale_pattern


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
    ) -> list[FewShotExample]:
        """Get top-performing posts as few-shot examples.

        Uses engagement data when available, falls back to eval scores.
        Excludes posts flagged as too_specific via exclude_ids.
        Filters out posts matching stale rhetorical patterns.
        """
        # Fetch more than needed to allow for pattern filtering
        fetch_limit = limit * 4
        top_posts = self.db.get_top_performing_posts(
            limit=fetch_limit, content_type=content_type
        )

        if top_posts:
            examples = []
            for p in top_posts:
                if exclude_ids and p["id"] in exclude_ids:
                    continue
                if has_stale_pattern(p["content"]):
                    continue
                examples.append(FewShotExample(
                    content=p["content"],
                    engagement_score=p["engagement_score"],
                ))
                if len(examples) >= limit:
                    break

            if examples:
                return examples

        # Cold start: fall back to highest eval scores among published posts
        return self._fallback_by_eval_score(content_type, limit, exclude_ids)

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
            if _has_stale_pattern(row["content"]):
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
