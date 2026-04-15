"""Platform divergence analysis for cross-posted content.

Compares engagement patterns between X and Bluesky for the same content,
identifies platform-specific performance divergences, and surfaces actionable
insights for content adaptation.
"""

from dataclasses import dataclass
from typing import Optional
from storage.db import Database


@dataclass
class DivergenceItem:
    """Single content item with divergent platform performance."""
    content_id: int
    content_type: str
    content_preview: str
    x_score: float
    bluesky_score: float
    divergence_ratio: float
    winning_platform: str


@dataclass
class PlatformComparison:
    """Aggregated platform performance comparison by content type."""
    content_type: str
    count: int
    avg_x_score: float
    avg_bluesky_score: float
    winner: str


@dataclass
class DivergenceReport:
    """Complete platform divergence analysis report."""
    total_cross_posted: int
    avg_x_score: float
    avg_bluesky_score: float
    platform_winner: str
    high_divergence_items: list[DivergenceItem]
    content_type_breakdown: dict[str, PlatformComparison]
    format_insights: list[str]


class PlatformDivergenceAnalyzer:
    """Analyzes engagement divergence between X and Bluesky platforms."""

    def __init__(self, db: Database):
        """Initialize analyzer with database connection.

        Args:
            db: Connected Database instance
        """
        self.db = db

    def analyze_divergence(self, days: int = 60) -> DivergenceReport:
        """Analyze platform performance divergence for cross-posted content.

        Args:
            days: Number of days to look back for analysis

        Returns:
            DivergenceReport with comparative metrics and insights
        """
        # Get cross-platform engagement data
        cross_platform_data = self.db.get_cross_platform_engagement(days=days)

        if not cross_platform_data:
            return self._empty_report()

        total_count = len(cross_platform_data)

        # Calculate aggregate scores
        total_x_score = sum(item['x_score'] for item in cross_platform_data)
        total_bluesky_score = sum(item['bluesky_score'] for item in cross_platform_data)

        avg_x = total_x_score / total_count
        avg_bluesky = total_bluesky_score / total_count

        # Determine overall platform winner
        platform_winner = "bluesky" if avg_bluesky > avg_x else "x"
        if abs(avg_bluesky - avg_x) < 0.01:
            platform_winner = "tie"

        # Identify high-divergence items (ratio > 2.0)
        high_divergence = []
        for item in cross_platform_data:
            x_score = item['x_score']
            bluesky_score = item['bluesky_score']

            # Calculate divergence ratio (larger / smaller)
            if x_score == 0 and bluesky_score == 0:
                continue  # Skip items with no engagement on either platform

            if bluesky_score > x_score:
                ratio = bluesky_score / max(x_score, 0.1)  # Avoid division by zero
                winner = "bluesky"
            else:
                ratio = x_score / max(bluesky_score, 0.1)
                winner = "x"

            if ratio > 2.0:
                high_divergence.append(DivergenceItem(
                    content_id=item['content_id'],
                    content_type=item['content_type'],
                    content_preview=item['content_preview'],
                    x_score=x_score,
                    bluesky_score=bluesky_score,
                    divergence_ratio=ratio,
                    winning_platform=winner
                ))

        # Sort by divergence ratio descending
        high_divergence.sort(key=lambda x: x.divergence_ratio, reverse=True)

        # Build content type breakdown
        type_breakdown = {}
        type_data = {}  # Collect data by type

        for item in cross_platform_data:
            content_type = item['content_type']
            if content_type not in type_data:
                type_data[content_type] = {'x_scores': [], 'bluesky_scores': []}

            type_data[content_type]['x_scores'].append(item['x_score'])
            type_data[content_type]['bluesky_scores'].append(item['bluesky_score'])

        for content_type, data in type_data.items():
            count = len(data['x_scores'])
            avg_x_type = sum(data['x_scores']) / count
            avg_bluesky_type = sum(data['bluesky_scores']) / count

            winner = "bluesky" if avg_bluesky_type > avg_x_type else "x"
            if abs(avg_bluesky_type - avg_x_type) < 0.01:
                winner = "tie"

            type_breakdown[content_type] = PlatformComparison(
                content_type=content_type,
                count=count,
                avg_x_score=round(avg_x_type, 2),
                avg_bluesky_score=round(avg_bluesky_type, 2),
                winner=winner
            )

        # Generate format insights
        insights = self._generate_format_insights(type_breakdown, avg_x, avg_bluesky)

        return DivergenceReport(
            total_cross_posted=total_count,
            avg_x_score=round(avg_x, 2),
            avg_bluesky_score=round(avg_bluesky, 2),
            platform_winner=platform_winner,
            high_divergence_items=high_divergence,
            content_type_breakdown=type_breakdown,
            format_insights=insights
        )

    def _empty_report(self) -> DivergenceReport:
        """Return empty report when no data available."""
        return DivergenceReport(
            total_cross_posted=0,
            avg_x_score=0.0,
            avg_bluesky_score=0.0,
            platform_winner="tie",
            high_divergence_items=[],
            content_type_breakdown={},
            format_insights=[]
        )

    def _generate_format_insights(
        self,
        type_breakdown: dict[str, PlatformComparison],
        overall_avg_x: float,
        overall_avg_bluesky: float
    ) -> list[str]:
        """Generate natural language insights about format performance.

        Args:
            type_breakdown: Content type performance breakdown
            overall_avg_x: Overall average X score
            overall_avg_bluesky: Overall average Bluesky score

        Returns:
            List of insight strings
        """
        insights = []

        # Overall platform preference
        if overall_avg_bluesky > overall_avg_x * 1.2:
            pct = int(((overall_avg_bluesky - overall_avg_x) / overall_avg_x) * 100)
            insights.append(f"Posts tend to get {pct}% more engagement on Bluesky")
        elif overall_avg_x > overall_avg_bluesky * 1.2:
            pct = int(((overall_avg_x - overall_avg_bluesky) / overall_avg_bluesky) * 100)
            insights.append(f"Posts tend to get {pct}% more engagement on X")
        else:
            insights.append("Posts perform similarly across platforms")

        # Format-specific insights
        for content_type, comparison in type_breakdown.items():
            type_name = content_type.replace("x_", "").replace("_", " ").title()

            if comparison.avg_bluesky_score > comparison.avg_x_score * 1.3:
                pct = int(((comparison.avg_bluesky_score - comparison.avg_x_score) / comparison.avg_x_score) * 100)
                insights.append(f"{type_name}s perform {pct}% better on Bluesky than X")
            elif comparison.avg_x_score > comparison.avg_bluesky_score * 1.3:
                pct = int(((comparison.avg_x_score - comparison.avg_bluesky_score) / comparison.avg_bluesky_score) * 100)
                insights.append(f"{type_name}s perform {pct}% better on X than Bluesky")
            else:
                insights.append(f"{type_name}s perform similarly across platforms")

        return insights

    def generate_adaptation_context(self, days: int = 60) -> str:
        """Generate context text for injection into generation prompts.

        Args:
            days: Number of days to look back for analysis

        Returns:
            Context string for prompt injection, or empty if insufficient data
        """
        report = self.analyze_divergence(days=days)

        # Require at least 5 cross-posted items for meaningful insights
        if report.total_cross_posted < 5:
            return ""

        # Build context block
        lines = ["PLATFORM NOTES:"]

        # Add format insights
        for insight in report.format_insights:
            lines.append(f"- {insight}")

        # Add high-divergence examples if present
        if report.high_divergence_items:
            top_divergence = report.high_divergence_items[0]
            lines.append(
                f"- Recent {top_divergence.winning_platform.upper()} outlier: "
                f"{top_divergence.divergence_ratio:.1f}x better performance"
            )

        return "\n".join(lines)
