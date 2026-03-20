"""Few-shot example selection for content generation."""

from dataclasses import dataclass
from storage.db import Database


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
    ) -> list[FewShotExample]:
        """Get top-performing posts as few-shot examples.

        Uses engagement data when available, falls back to eval scores.
        """
        # Try engagement-ranked posts first
        top_posts = self.db.get_top_performing_posts(limit=limit, content_type=content_type)

        if top_posts:
            return [
                FewShotExample(
                    content=p["content"],
                    engagement_score=p["engagement_score"],
                )
                for p in top_posts
            ]

        # Cold start: fall back to highest eval scores among published posts
        return self._fallback_by_eval_score(content_type, limit)

    def _fallback_by_eval_score(
        self, content_type: str, limit: int
    ) -> list[FewShotExample]:
        """Fallback: select examples by eval score when no engagement data exists."""
        cursor = self.db.conn.execute(
            """SELECT content, eval_score FROM generated_content
               WHERE content_type = ? AND published = 1
               ORDER BY eval_score DESC
               LIMIT ?""",
            (content_type, limit),
        )
        return [
            FewShotExample(content=row["content"], engagement_score=0.0)
            for row in cursor.fetchall()
        ]

    def format_examples(self, examples: list[FewShotExample]) -> str:
        """Format examples for injection into a generation prompt."""
        if not examples:
            return ""
        lines = []
        for i, ex in enumerate(examples, 1):
            lines.append(f"{i}. {ex.content}")
        return "\n\n".join(lines)
