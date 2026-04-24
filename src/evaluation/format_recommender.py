"""Format recommendations from recent engagement history."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

from storage.db import Database


@dataclass(frozen=True)
class FormatRecommendation:
    """Ranked recommendation for a content format."""

    content_format: str
    score: float
    sample_count: int
    avg_engagement: float
    reason: str
    is_fallback: bool = False


class FormatRecommender:
    """Recommend content formats that have recently performed well."""

    DEFAULT_FORMATS = {
        "x_post": ["micro_story", "question", "tip"],
        "x_thread": ["mid_action", "bold_claim", "question_hook"],
    }

    ENGAGEMENT_TABLES = {
        "x": "post_engagement",
        "bluesky": "bluesky_engagement",
    }

    def __init__(
        self,
        db: Database,
        *,
        min_samples: int = 3,
        lookback_days: int = 90,
    ):
        self.db = db
        self.min_samples = min_samples
        self.lookback_days = lookback_days

    def recommend(
        self,
        content_type: str,
        *,
        platform: str = "x",
        limit: int = 3,
    ) -> list[FormatRecommendation]:
        """Return ranked format recommendations with human-readable reasons."""
        if limit <= 0:
            return []

        stats = self._format_stats(content_type=content_type, platform=platform)
        eligible = [
            row for row in stats
            if (row["sample_count"] or 0) >= self.min_samples
        ]
        if not eligible:
            return self._fallback_recommendations(content_type, limit, stats)

        ranked = sorted(
            eligible,
            key=lambda row: (
                self._score(row),
                row["avg_engagement"] or 0.0,
                row["sample_count"] or 0,
                row["content_format"] or "",
            ),
            reverse=True,
        )
        return [
            self._recommendation_from_row(row)
            for row in ranked[:limit]
        ]

    def recommend_format_names(
        self,
        content_type: str,
        *,
        platform: str = "x",
        limit: int = 3,
    ) -> list[str]:
        """Return only format names for callers that already own logging."""
        return [
            recommendation.content_format
            for recommendation in self.recommend(
                content_type=content_type,
                platform=platform,
                limit=limit,
            )
        ]

    def _format_stats(self, content_type: str, platform: str) -> list[dict]:
        engagement_table = self.ENGAGEMENT_TABLES.get(platform)
        if not engagement_table:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=self.lookback_days)
        cursor = self.db.conn.execute(
            f"""SELECT
                   gc.content_format AS content_format,
                   COUNT(*) AS sample_count,
                   AVG(COALESCE(e.engagement_score, 0)) AS avg_engagement,
                   MAX(COALESCE(e.engagement_score, 0)) AS best_engagement,
                   SUM(CASE WHEN gc.auto_quality = 'resonated' THEN 1 ELSE 0 END)
                       AS resonated_count
               FROM generated_content gc
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC
                          ) AS rn
                   FROM {engagement_table}
                   WHERE engagement_score IS NOT NULL
               ) e ON e.content_id = gc.id AND e.rn = 1
               LEFT JOIN content_publications cp
                 ON cp.content_id = gc.id
                AND cp.platform = ?
                AND cp.status = 'published'
               WHERE gc.content_format IS NOT NULL
                 AND gc.content_type = ?
                 AND (
                     cp.id IS NOT NULL
                     OR (? = 'x' AND gc.published = 1)
                 )
                 AND COALESCE(cp.published_at, gc.published_at) >= ?
               GROUP BY gc.content_format""",
            (platform, content_type, platform, cutoff.isoformat()),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _fallback_recommendations(
        self,
        content_type: str,
        limit: int,
        stats: list[dict],
    ) -> list[FormatRecommendation]:
        total_samples = sum(row["sample_count"] or 0 for row in stats)
        reason = (
            f"Fallback default: only {total_samples} recent samples; "
            f"need at least {self.min_samples} samples for a format-specific signal."
        )
        defaults = self.DEFAULT_FORMATS.get(content_type, self.DEFAULT_FORMATS["x_post"])
        return [
            FormatRecommendation(
                content_format=format_name,
                score=0.0,
                sample_count=0,
                avg_engagement=0.0,
                reason=reason,
                is_fallback=True,
            )
            for format_name in defaults[:limit]
        ]

    def _recommendation_from_row(self, row: dict) -> FormatRecommendation:
        score = self._score(row)
        sample_count = row["sample_count"] or 0
        avg_engagement = row["avg_engagement"] or 0.0
        resonated_count = row["resonated_count"] or 0
        reason = (
            f"{row['content_format']} averaged {avg_engagement:.1f} engagement "
            f"across {sample_count} recent {self._pluralize(sample_count, 'post')}; "
            f"{resonated_count} classified as resonated."
        )
        return FormatRecommendation(
            content_format=row["content_format"],
            score=round(score, 4),
            sample_count=sample_count,
            avg_engagement=round(avg_engagement, 2),
            reason=reason,
            is_fallback=False,
        )

    @staticmethod
    def _score(row: dict) -> float:
        sample_count = row["sample_count"] or 0
        avg_engagement = row["avg_engagement"] or 0.0
        sample_confidence = 1.0 + min(sample_count, 30) / 30.0
        return avg_engagement * sample_confidence

    @staticmethod
    def _pluralize(count: int, singular: str) -> str:
        return singular if count == 1 else f"{singular}s"
