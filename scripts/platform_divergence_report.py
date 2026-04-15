#!/usr/bin/env python3
"""Generate platform divergence report comparing X and Bluesky engagement."""

import argparse
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

    winner_display = report.platform_winner.upper()
    if report.platform_winner == "tie":
        lines.append(f"Platform winner: {winner_display}")
    else:
        lines.append(f"Platform winner: {winner_display}")
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
        lines.append("HIGH DIVERGENCE ITEMS (ratio > 2.0):")
        for i, item in enumerate(report.high_divergence_items[:10], 1):  # Show top 10
            preview = item.content_preview[:60] + "..." if len(item.content_preview) > 60 else item.content_preview
            lines.append(f"  {i}. [{item.content_type}] {item.winning_platform.upper()} wins {item.divergence_ratio:.1f}x")
            lines.append(f"     X: {item.x_score:.1f} | Bluesky: {item.bluesky_score:.1f}")
            lines.append(f"     \"{preview}\"")

        if len(report.high_divergence_items) > 10:
            lines.append(f"  ... and {len(report.high_divergence_items) - 10} more")
        lines.append("")

    return "\n".join(lines)


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

        # Print formatted report
        print(format_report(report))

        # Print adaptation context
        context = analyzer.generate_adaptation_context(days=args.days)
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
