"""Source quality scoring system for curated knowledge sources.

Measures which curated sources (X accounts, blogs) consistently contribute to
high-engagement content, enabling smarter knowledge retrieval prioritization.
"""

from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from storage.db import Database


@dataclass
class SourceScore:
    """Quality score for a curated knowledge source."""
    author: str
    source_type: str
    quality_score: float
    usage_count: int
    avg_engagement: float
    hit_rate: float
    tier: str  # 'gold', 'silver', 'bronze'


class SourceScorer:
    """Computes quality scores for curated knowledge sources based on engagement."""

    def __init__(self, db: "Database") -> None:
        """Initialize scorer with database connection.

        Args:
            db: Database instance with connection
        """
        self.db = db
        self._tier_cache: dict[tuple[str, str], str] = {}  # Cache for quick tier lookups

    def compute_scores(self, days: int = 90, min_uses: int = 2) -> list[SourceScore]:
        """Compute quality scores for all curated sources.

        Args:
            days: Number of days to look back for usage stats
            min_uses: Minimum number of uses required to be scored

        Returns:
            List of SourceScore objects sorted by quality_score descending
        """
        # Get engagement details for sources
        source_data = self.db.get_source_engagement_details(days=days, min_uses=min_uses)

        if not source_data:
            return []

        # Compute quality scores
        scores = []
        for row in source_data:
            author = row['author']
            source_type = row['source_type']
            usage_count = row['total_uses']
            avg_engagement = row['avg_engagement']
            resonated_count = row['resonated_count']
            classified_count = row['classified_count']

            # Calculate hit rate (fraction of classified posts that resonated)
            hit_rate = resonated_count / classified_count if classified_count > 0 else 0.0

            scores.append({
                'author': author,
                'source_type': source_type,
                'usage_count': usage_count,
                'avg_engagement': avg_engagement,
                'hit_rate': hit_rate,
            })

        # Normalize avg_engagement to 0-1 range for scoring
        if scores:
            max_engagement = max(s['avg_engagement'] for s in scores)
            min_engagement = min(s['avg_engagement'] for s in scores)
            engagement_range = max_engagement - min_engagement

            for score in scores:
                # Normalize engagement (handle edge case where all have same engagement)
                if engagement_range > 0:
                    normalized_engagement = (score['avg_engagement'] - min_engagement) / engagement_range
                else:
                    normalized_engagement = 0.5

                # Quality score: 60% engagement, 40% hit rate
                quality_score = 0.6 * normalized_engagement + 0.4 * score['hit_rate']
                score['quality_score'] = quality_score

        # Sort by quality score descending
        scores.sort(key=lambda x: x['quality_score'], reverse=True)

        # Assign tiers based on percentile ranks
        total_count = len(scores)
        for idx, score in enumerate(scores):
            percentile = idx / total_count if total_count > 0 else 0

            if percentile < 0.20:
                tier = 'gold'
            elif percentile < 0.60:
                tier = 'silver'
            else:
                tier = 'bronze'

            score['tier'] = tier

        # Convert to SourceScore dataclass instances
        result = [
            SourceScore(
                author=s['author'],
                source_type=s['source_type'],
                quality_score=s['quality_score'],
                usage_count=s['usage_count'],
                avg_engagement=s['avg_engagement'],
                hit_rate=s['hit_rate'],
                tier=s['tier']
            )
            for s in scores
        ]

        # Update tier cache for quick lookups
        self._tier_cache = {
            (score.author, score.source_type): score.tier
            for score in result
        }

        return result

    def get_source_tier(self, author: str, source_type: str) -> Optional[str]:
        """Get the tier for a specific source.

        Args:
            author: Author/account name
            source_type: Type of source (e.g., 'curated_x', 'curated_article')

        Returns:
            Tier string ('gold', 'silver', 'bronze') or None if not found
        """
        # Check cache first
        if (author, source_type) in self._tier_cache:
            return self._tier_cache[(author, source_type)]

        # If cache is empty, compute fresh scores
        # This ensures get_source_tier works even if compute_scores wasn't called
        scores = self.compute_scores()

        # Return from updated cache
        return self._tier_cache.get((author, source_type))

    def generate_retrieval_boost_context(self, days: int = 90) -> str:
        """Generate text summary for injection into knowledge retrieval prompts.

        Args:
            days: Number of days to look back

        Returns:
            Formatted string describing gold and bronze tier sources,
            or empty string if insufficient data
        """
        scores = self.compute_scores(days=days)

        if not scores:
            return ""

        # Separate by tier
        gold_sources = [s for s in scores if s.tier == 'gold']
        bronze_sources = [s for s in scores if s.tier == 'bronze']

        if not gold_sources and not bronze_sources:
            return ""

        lines = []

        if gold_sources:
            gold_names = [f"@{s.author}" if s.source_type == 'curated_x' else s.author
                         for s in gold_sources]
            lines.append(f"Gold-tier sources (consistently drive engagement): {', '.join(gold_names)}.")

        if bronze_sources:
            bronze_names = [f"@{s.author}" if s.source_type == 'curated_x' else s.author
                           for s in bronze_sources]
            lines.append(f"Bronze-tier sources (low engagement correlation): {', '.join(bronze_names)}.")

        return " ".join(lines)
