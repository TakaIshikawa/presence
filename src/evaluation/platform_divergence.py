"""Platform divergence analysis for cross-posted content.

Compares engagement patterns between X and Bluesky for the same content,
identifies platform-specific performance divergences, and surfaces actionable
insights for content adaptation.
"""

from dataclasses import dataclass, field
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
    recommendation: str = ""


@dataclass
class ContentTypeRecommendation:
    """Structured recommendation for adapting a content type."""
    content_type: str
    content_type_label: str
    count: int
    avg_x_score: float
    avg_bluesky_score: float
    winner: str
    recommendation: str
    rationale: str
    score_gap: float


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
    platform_takeaway: str = ""
    recommendations: list[ContentTypeRecommendation] = field(default_factory=list)


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
        recommendations: list[ContentTypeRecommendation] = []

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

            recommendation = self._build_content_type_recommendation(
                content_type=content_type,
                count=count,
                avg_x_score=avg_x_type,
                avg_bluesky_score=avg_bluesky_type,
                winner=winner,
            )

            type_breakdown[content_type] = PlatformComparison(
                content_type=content_type,
                count=count,
                avg_x_score=round(avg_x_type, 2),
                avg_bluesky_score=round(avg_bluesky_type, 2),
                winner=winner,
                recommendation=recommendation.recommendation,
            )
            recommendations.append(recommendation)

        # Generate format insights
        insights = self._generate_format_insights(type_breakdown, avg_x, avg_bluesky)
        platform_takeaway = self._generate_platform_takeaway(
            avg_x,
            avg_bluesky,
            platform_winner,
            recommendations,
        )

        return DivergenceReport(
            total_cross_posted=total_count,
            avg_x_score=round(avg_x, 2),
            avg_bluesky_score=round(avg_bluesky, 2),
            platform_winner=platform_winner,
            platform_takeaway=platform_takeaway,
            high_divergence_items=high_divergence,
            content_type_breakdown=type_breakdown,
            recommendations=recommendations,
            format_insights=insights
        )

    def _empty_report(self) -> DivergenceReport:
        """Return empty report when no data available."""
        return DivergenceReport(
            total_cross_posted=0,
            avg_x_score=0.0,
            avg_bluesky_score=0.0,
            platform_winner="tie",
            platform_takeaway="No cross-posted content with engagement data yet.",
            high_divergence_items=[],
            content_type_breakdown={},
            recommendations=[],
            format_insights=[]
        )

    def _content_type_label(self, content_type: str) -> str:
        """Convert an internal content type name into readable text."""
        return content_type.replace("x_", "").replace("_", " ").title()

    def _build_content_type_recommendation(
        self,
        content_type: str,
        count: int,
        avg_x_score: float,
        avg_bluesky_score: float,
        winner: str,
    ) -> ContentTypeRecommendation:
        """Build an actionable recommendation for a content type."""
        type_label = self._content_type_label(content_type)
        score_gap = abs(avg_bluesky_score - avg_x_score)

        if winner == "bluesky":
            recommendation = (
                f"Favor Bluesky for {type_label.lower()} content. Keep the fuller framing, "
                f"conversation hooks, and community-oriented tone."
            )
            rationale = (
                f"Bluesky averaged {avg_bluesky_score:.2f} vs X at {avg_x_score:.2f}, "
                f"a gap of {score_gap:.2f} points."
            )
        elif winner == "x":
            recommendation = (
                f"Favor X for {type_label.lower()} content. Tighten the hook, compress the copy, "
                f"and lead with the strongest claim."
            )
            rationale = (
                f"X averaged {avg_x_score:.2f} vs Bluesky at {avg_bluesky_score:.2f}, "
                f"a gap of {score_gap:.2f} points."
            )
        else:
            recommendation = (
                f"Use {type_label.lower()} content on both platforms without major changes."
            )
            rationale = (
                f"Average performance was effectively tied at {avg_x_score:.2f} on X and "
                f"{avg_bluesky_score:.2f} on Bluesky."
            )

        return ContentTypeRecommendation(
            content_type=content_type,
            content_type_label=type_label,
            count=count,
            avg_x_score=round(avg_x_score, 2),
            avg_bluesky_score=round(avg_bluesky_score, 2),
            winner=winner,
            recommendation=recommendation,
            rationale=rationale,
            score_gap=round(score_gap, 2),
        )

    def _generate_platform_takeaway(
        self,
        overall_avg_x: float,
        overall_avg_bluesky: float,
        platform_winner: str,
        recommendations: list[ContentTypeRecommendation],
    ) -> str:
        """Generate a concise platform-level takeaway."""
        if platform_winner == "tie":
            base = (
                f"Overall performance is balanced: X averaged {overall_avg_x:.2f} and "
                f"Bluesky averaged {overall_avg_bluesky:.2f}."
            )
        elif platform_winner == "bluesky":
            base = (
                f"Bluesky is the stronger overall platform here, averaging "
                f"{overall_avg_bluesky:.2f} vs {overall_avg_x:.2f} on X."
            )
        else:
            base = (
                f"X is the stronger overall platform here, averaging "
                f"{overall_avg_x:.2f} vs {overall_avg_bluesky:.2f} on Bluesky."
            )

        if recommendations:
            top_recommendation = max(recommendations, key=lambda item: item.score_gap)
            return f"{base} Strongest format signal: {top_recommendation.recommendation}"

        return base

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
        if overall_avg_bluesky > overall_avg_x * 1.2 and overall_avg_x > 0:
            pct = int(((overall_avg_bluesky - overall_avg_x) / overall_avg_x) * 100)
            insights.append(f"Posts tend to get {pct}% more engagement on Bluesky")
        elif overall_avg_x > overall_avg_bluesky * 1.2 and overall_avg_bluesky > 0:
            pct = int(((overall_avg_x - overall_avg_bluesky) / overall_avg_bluesky) * 100)
            insights.append(f"Posts tend to get {pct}% more engagement on X")
        elif overall_avg_bluesky > overall_avg_x:
            insights.append("Posts tend to perform better on Bluesky")
        elif overall_avg_x > overall_avg_bluesky:
            insights.append("Posts tend to perform better on X")
        else:
            insights.append("Posts perform similarly across platforms")

        # Format-specific insights
        for content_type, comparison in type_breakdown.items():
            type_name = content_type.replace("x_", "").replace("_", " ").title()

            if comparison.avg_bluesky_score > comparison.avg_x_score * 1.3 and comparison.avg_x_score > 0:
                pct = int(((comparison.avg_bluesky_score - comparison.avg_x_score) / comparison.avg_x_score) * 100)
                insights.append(f"{type_name}s perform {pct}% better on Bluesky than X")
            elif comparison.avg_x_score > comparison.avg_bluesky_score * 1.3 and comparison.avg_bluesky_score > 0:
                pct = int(((comparison.avg_x_score - comparison.avg_bluesky_score) / comparison.avg_bluesky_score) * 100)
                insights.append(f"{type_name}s perform {pct}% better on X than Bluesky")
            elif comparison.avg_bluesky_score > comparison.avg_x_score:
                insights.append(f"{type_name}s tend to perform better on Bluesky")
            elif comparison.avg_x_score > comparison.avg_bluesky_score:
                insights.append(f"{type_name}s tend to perform better on X")
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

        # Add the strongest overall takeaway first so prompt consumers get the main signal.
        if report.platform_takeaway:
            lines.append(f"- {report.platform_takeaway}")

        # Add the most actionable content-type recommendations first.
        for recommendation in sorted(
            report.recommendations,
            key=lambda item: item.score_gap,
            reverse=True,
        )[:3]:
            lines.append(
                f"- {recommendation.content_type_label}: {recommendation.recommendation}"
            )

        # Add format insights as broader context.
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
