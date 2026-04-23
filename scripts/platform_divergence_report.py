#!/usr/bin/env python3
"""Generate platform divergence report comparing X and Bluesky engagement."""

import argparse
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from evaluation.platform_divergence import PlatformDivergenceAnalyzer

logger = logging.getLogger(__name__)


def format_report(report):
    """Format divergence report as human-readable text."""
    lines = []
    lines.append("=" * 70)
    lines.append("PLATFORM DIVERGENCE ANALYSIS REPORT")
    lines.append("=" * 70)
    lines.append("")

    if report.total_cross_posted == 0:
        lines.append("No cross-posted content with engagement data yet.")
        return "\n".join(lines)

    # Summary statistics
    lines.append(f"Total cross-posted items: {report.total_cross_posted}")
    lines.append(f"Average X score: {report.avg_x_score:.2f}")
    lines.append(f"Average Bluesky score: {report.avg_bluesky_score:.2f}")
    lines.append(f"Platform winner: {report.platform_winner.upper()}")
    lines.append("")

    # Strongest overall takeaway
    if report.platform_takeaway:
        lines.append("STRONGEST TAKEAWAY:")
        lines.append(f"  {report.platform_takeaway}")
        lines.append("")

    # Content type recommendations
    if report.recommendations:
        lines.append("CONTENT TYPE RECOMMENDATIONS:")
        for recommendation in sorted(
            report.recommendations,
            key=lambda item: item.score_gap,
            reverse=True,
        ):
            lines.append(f"  {recommendation.content_type_label}:")
            lines.append(f"    Recommendation: {recommendation.recommendation}")
            lines.append(f"    Rationale: {recommendation.rationale}")
        lines.append("")

    # Format insights
    if report.format_insights:
        lines.append("FORMAT INSIGHTS:")
        for insight in report.format_insights:
            lines.append(f"  • {insight}")
        lines.append("")

    # Content type breakdown
    if report.content_type_breakdown:
        lines.append("CONTENT TYPE BREAKDOWN:")
        for content_type, comparison in sorted(
            report.content_type_breakdown.items(),
            key=lambda x: x[1].count,
            reverse=True
        ):
            type_name = content_type.replace("x_", "").replace("_", " ").title()
            lines.append(f"  {type_name}:")
            lines.append(f"    Count: {comparison.count}")
            lines.append(f"    Avg X score: {comparison.avg_x_score:.2f}")
            lines.append(f"    Avg Bluesky score: {comparison.avg_bluesky_score:.2f}")
            lines.append(f"    Winner: {comparison.winner.upper()}")
        lines.append("")

    # High divergence items
    if report.high_divergence_items:
        lines.append("HIGH DIVERGENCE EXAMPLES (ratio > 2.0):")
        for i, item in enumerate(report.high_divergence_items[:3], 1):
            preview = item.content_preview[:60] + "..." if len(item.content_preview) > 60 else item.content_preview
            lines.append(f"  {i}. [{item.content_type}] {item.winning_platform.upper()} wins {item.divergence_ratio:.1f}x")
            lines.append(f"     X: {item.x_score:.1f} | Bluesky: {item.bluesky_score:.1f}")
            lines.append(f"     \"{preview}\"")

        if len(report.high_divergence_items) > 3:
            lines.append(f"  ... and {len(report.high_divergence_items) - 3} more")
        lines.append("")

    return "\n".join(lines)


def format_json(report, adaptation_context: str = ""):
    """Format divergence report as JSON."""
    return json.dumps(
        {
            "comparative_stats": {
                "total_cross_posted": report.total_cross_posted,
                "avg_x_score": report.avg_x_score,
                "avg_bluesky_score": report.avg_bluesky_score,
                "platform_winner": report.platform_winner,
                "platform_takeaway": report.platform_takeaway,
            },
            "format_insights": report.format_insights,
            "content_type_breakdown": {
                content_type: {
                    "content_type": comparison.content_type,
                    "count": comparison.count,
                    "avg_x_score": comparison.avg_x_score,
                    "avg_bluesky_score": comparison.avg_bluesky_score,
                    "winner": comparison.winner,
                    "recommendation": comparison.recommendation,
                }
                for content_type, comparison in report.content_type_breakdown.items()
            },
            "recommendations": [
                {
                    "content_type": recommendation.content_type,
                    "content_type_label": recommendation.content_type_label,
                    "count": recommendation.count,
                    "avg_x_score": recommendation.avg_x_score,
                    "avg_bluesky_score": recommendation.avg_bluesky_score,
                    "winner": recommendation.winner,
                    "recommendation": recommendation.recommendation,
                    "rationale": recommendation.rationale,
                    "score_gap": recommendation.score_gap,
                }
                for recommendation in report.recommendations
            ],
            "high_divergence_items": [
                {
                    "content_id": item.content_id,
                    "content_type": item.content_type,
                    "content_preview": item.content_preview,
                    "x_score": item.x_score,
                    "bluesky_score": item.bluesky_score,
                    "divergence_ratio": item.divergence_ratio,
                    "winning_platform": item.winning_platform,
                }
                for item in report.high_divergence_items
            ],
            "adaptation_context": adaptation_context,
        },
        indent=2,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Generate platform divergence report comparing X and Bluesky engagement"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=60,
        help="Number of days to look back (default: 60)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON instead of human-readable format",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        analyzer = PlatformDivergenceAnalyzer(db)

        logger.info(f"Analyzing platform divergence (last {args.days} days)...")
        report = analyzer.analyze_divergence(days=args.days)
        context = analyzer.generate_adaptation_context(days=args.days)

        if args.json:
            print(format_json(report, adaptation_context=context))
        else:
            # Print formatted report
            print(format_report(report))

            # Print adaptation context
            if context:
                print("ADAPTATION CONTEXT (for generation prompts):")
                print("-" * 70)
                print(context)
                print("-" * 70)
                print()

        # Log summary
        logger.info(f"Total cross-posted: {report.total_cross_posted}")
        logger.info(f"Platform winner: {report.platform_winner.upper()}")
        logger.info(f"High-divergence items: {len(report.high_divergence_items)}")

        update_monitoring("platform_divergence")
        logger.info("Done.")


if __name__ == "__main__":
    main()
