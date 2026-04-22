"""Format performance analysis for engagement-based format selection weighting."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from storage.db import Database


@dataclass
class FormatStat:
    """Statistics for a single content format."""
    format_name: str
    sample_count: int
    avg_engagement: float
    resonated_rate: float  # Fraction classified as 'resonated'


@dataclass
class FormatReport:
    """Aggregated performance report across all formats."""
    format_stats: list[FormatStat]
    recommended_weights: dict[str, float]


class FormatPerformanceAnalyzer:
    """Analyzes format performance and computes selection weights."""

    # Minimum samples required for weighting (otherwise use neutral weight)
    MIN_SAMPLES = 3

    # Floor for weight values to ensure exploration
    WEIGHT_FLOOR = 0.5

    def __init__(self, db: Database):
        self.db = db

    def get_recommended_formats(
        self,
        content_type: str,
        platform: str = "x",
        limit: int = 3,
        days: int = 90,
    ) -> list[str]:
        """Return high-confidence format recommendations for a platform.

        Formats with fewer than MIN_SAMPLES are intentionally excluded so noisy
        early engagement does not collapse generation into one format. Callers
        should fill any remaining slots with random formats to keep exploration.
        """
        if limit <= 0:
            return []

        stats = self._get_platform_format_stats(
            content_type=content_type,
            platform=platform,
            days=days,
        )
        eligible = [
            row for row in stats
            if (row["count"] or 0) >= self.MIN_SAMPLES
        ]
        if not eligible:
            return []

        def recommendation_score(row: dict) -> float:
            avg_engagement = row["avg_engagement"] or 0.0
            sample_count = row["count"] or 0
            sample_boost = 1.0 + min(sample_count, 30) / 30.0
            return avg_engagement * sample_boost

        ranked = sorted(
            eligible,
            key=lambda row: (
                recommendation_score(row),
                row["avg_engagement"] or 0.0,
                row["count"] or 0,
            ),
            reverse=True,
        )
        return [row["format"] for row in ranked[:limit]]

    def _get_platform_format_stats(
        self,
        content_type: str,
        platform: str,
        days: int,
    ) -> list[dict]:
        """Get recent format stats scoped to content type and platform."""
        engagement_tables = {
            "x": "post_engagement",
            "bluesky": "bluesky_engagement",
        }
        if platform not in engagement_tables:
            return []

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        engagement_table = engagement_tables[platform]

        cursor = self.db.conn.execute(
            f"""SELECT
                   gc.content_format AS format,
                   COUNT(*) AS count,
                   AVG(COALESCE(e.engagement_score, 0)) AS avg_engagement,
                   SUM(CASE WHEN gc.auto_quality = 'resonated' THEN 1 ELSE 0 END) AS resonated_count,
                   SUM(CASE WHEN gc.auto_quality IS NOT NULL THEN 1 ELSE 0 END) AS total_classified
               FROM generated_content gc
               LEFT JOIN content_publications cp
                 ON cp.content_id = gc.id
                AND cp.platform = ?
                AND cp.status = 'published'
               LEFT JOIN (
                   SELECT content_id, engagement_score,
                          ROW_NUMBER() OVER (PARTITION BY content_id ORDER BY fetched_at DESC) AS rn
                   FROM {engagement_table}
                   WHERE engagement_score IS NOT NULL
               ) e ON e.content_id = gc.id AND e.rn = 1
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

    def analyze_format_performance(self, days: int = 90) -> FormatReport:
        """Analyze format performance based on engagement data.

        Args:
            days: Lookback window for published content

        Returns:
            FormatReport with stats and recommended weights
        """
        raw_stats = self.db.get_format_engagement_stats(days=days)

        format_stats = []
        for row in raw_stats:
            resonated_count = row["resonated_count"] or 0
            total_classified = row["total_classified"] or 0
            resonated_rate = (
                resonated_count / total_classified if total_classified > 0 else 0.0
            )

            format_stats.append(
                FormatStat(
                    format_name=row["format"],
                    sample_count=row["count"],
                    avg_engagement=row["avg_engagement"] or 0.0,
                    resonated_rate=resonated_rate,
                )
            )

        weights = self.compute_selection_weights(days=days)

        return FormatReport(
            format_stats=format_stats,
            recommended_weights=weights,
        )

    def compute_selection_weights(self, days: int = 90) -> dict[str, float]:
        """Compute format selection weights based on performance.

        Formula: weight = 1.0 + (normalized_avg_engagement * 2)
        - Better formats get up to 3x weight (1.0 + 2.0)
        - Formats with < MIN_SAMPLES get neutral weight (1.0)
        - All weights have a floor of WEIGHT_FLOOR for exploration

        Args:
            days: Lookback window for published content

        Returns:
            Dict mapping format name to selection weight
        """
        raw_stats = self.db.get_format_engagement_stats(days=days)

        if not raw_stats:
            return {}

        # Extract engagement scores for normalization
        format_engagement = {}
        for row in raw_stats:
            format_name = row["format"]
            sample_count = row["count"]
            avg_engagement = row["avg_engagement"] or 0.0

            # Only consider formats with sufficient samples
            if sample_count >= self.MIN_SAMPLES:
                format_engagement[format_name] = avg_engagement

        if not format_engagement:
            # No formats have enough samples, return neutral weights
            return {row["format"]: 1.0 for row in raw_stats}

        # Normalize engagement scores to [0, 1]
        max_engagement = max(format_engagement.values())
        min_engagement = min(format_engagement.values())
        engagement_range = max_engagement - min_engagement

        weights = {}
        for row in raw_stats:
            format_name = row["format"]
            sample_count = row["count"]

            if sample_count < self.MIN_SAMPLES:
                # Insufficient samples, use neutral weight
                weights[format_name] = 1.0
            else:
                avg_engagement = row["avg_engagement"] or 0.0

                if engagement_range > 0:
                    normalized = (avg_engagement - min_engagement) / engagement_range
                else:
                    # All formats have same engagement, use neutral weights
                    normalized = 0.0

                # weight = 1.0 + (normalized * 2) means:
                # - worst format: 1.0
                # - best format: 3.0
                weight = 1.0 + (normalized * 2.0)

                # Apply floor to ensure exploration
                weight = max(weight, self.WEIGHT_FLOOR)
                weights[format_name] = weight

        return weights
