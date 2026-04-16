#!/usr/bin/env python3
"""Generate pipeline analytics reports.

Displays comprehensive pipeline health metrics including conversion rates,
filter effectiveness, score distributions, and engagement correlation.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from evaluation.pipeline_analytics import PipelineAnalytics

logger = logging.getLogger(__name__)


def format_text_report(analytics: PipelineAnalytics, args) -> str:
    """Format a human-readable text report."""
    report = analytics.health_report(
        content_type=args.content_type,
        days=args.days
    )

    if not report:
        return f"No pipeline data found for {args.content_type} in last {args.days} days."

    lines = []
    lines.append("")
    lines.append("=" * 70)
    lines.append(f"Pipeline Health Report (last {args.days} days)")
    lines.append("=" * 70)
    lines.append("")

    # Overview
    lines.append(f"Content type:  {args.content_type}")
    lines.append(f"Period:        {report.period_start.strftime('%Y-%m-%d')} to {report.period_end.strftime('%Y-%m-%d')}")
    lines.append(f"Total runs:    {report.total_runs}")
    lines.append("")

    # Outcomes
    lines.append("Outcomes:")
    for outcome, count in sorted(report.outcomes.items()):
        percentage = (count / report.total_runs * 100) if report.total_runs > 0 else 0
        lines.append(f"  {outcome:20s}: {count:4d} ({percentage:5.1f}%)")
    lines.append("")

    # Key metrics
    lines.append("Key Metrics:")
    lines.append(f"  Conversion rate:       {report.conversion_rate * 100:5.1f}%")
    lines.append(f"  Avg final score:       {report.avg_final_score:5.1f}/10")
    lines.append(f"  Avg candidates/run:    {report.avg_candidates_per_run:5.1f}")
    lines.append("")

    # Filter effectiveness
    if report.filter_breakdown:
        lines.append("Filter Effectiveness:")
        # Sort by count descending
        sorted_filters = sorted(
            report.filter_breakdown.items(),
            key=lambda x: x[1],
            reverse=True
        )
        total_filtered = sum(report.filter_breakdown.values())
        for filter_name, count in sorted_filters:
            pct = (count / total_filtered * 100) if total_filtered > 0 else 0
            lines.append(f"  {filter_name:30s}: {count:4d} rejected ({pct:5.1f}%)")
        lines.append("")

    # Score distribution
    lines.append("Score Distribution:")
    for band, count in report.score_distribution.items():
        lines.append(f"  {band:6s}: {count:4d} runs")
    lines.append("")

    # Refinement stats
    if report.refinement_stats['total_refined'] > 0:
        lines.append("Refinement:")
        stats = report.refinement_stats
        refined_pct = (stats['picked_refined'] / stats['total_refined'] * 100) if stats['total_refined'] > 0 else 0
        original_pct = (stats['picked_original'] / stats['total_refined'] * 100) if stats['total_refined'] > 0 else 0
        lines.append(f"  Total refined:         {stats['total_refined']}")
        lines.append(f"  Picked refined:        {stats['picked_refined']:4d} ({refined_pct:5.1f}%)")
        lines.append(f"  Kept original:         {stats['picked_original']:4d} ({original_pct:5.1f}%)")
        lines.append("")

    # Engagement correlation
    if any(v > 0 for v in report.avg_engagement_by_score_band.values()):
        lines.append("Score vs Engagement Correlation:")
        for band in ['9-10', '7-9', '5-7', '3-5', '0-3']:
            avg_eng = report.avg_engagement_by_score_band[band]
            if avg_eng > 0:
                lines.append(f"  {band:6s} band: avg engagement {avg_eng:5.1f}")
        lines.append("")

    # Weekly trends
    trends = analytics.trend(content_type=args.content_type, weeks=8)
    if trends:
        lines.append("Weekly Trends (last 8 weeks):")
        lines.append(f"  {'Week':8s} {'Runs':>6s} {'Published':>10s} {'Conv%':>8s} {'AvgScore':>10s} {'AvgEng':>8s}")
        lines.append(f"  {'-'*8:8s} {'-'*6:>6s} {'-'*10:>10s} {'-'*8:>8s} {'-'*10:>10s} {'-'*8:>8s}")
        for t in trends:
            lines.append(
                f"  {t['week']:8s} {t['runs']:6d} {t['published']:10d} "
                f"{t['conversion_rate']:7.1f}% {t['avg_score']:9.1f} {t['avg_engagement']:7.1f}"
            )
        lines.append("")

    lines.append("=" * 70)
    lines.append("")

    return "\n".join(lines)


def format_json_report(analytics: PipelineAnalytics, args) -> str:
    """Format a machine-readable JSON report."""
    report = analytics.health_report(
        content_type=args.content_type,
        days=args.days
    )

    if not report:
        return json.dumps({"error": "No data found"}, indent=2)

    data = {
        "content_type": args.content_type,
        "days": args.days,
        "period_start": report.period_start.isoformat(),
        "period_end": report.period_end.isoformat(),
        "total_runs": report.total_runs,
        "outcomes": report.outcomes,
        "conversion_rate": round(report.conversion_rate, 3),
        "avg_final_score": round(report.avg_final_score, 2),
        "avg_candidates_per_run": round(report.avg_candidates_per_run, 2),
        "filter_breakdown": report.filter_breakdown,
        "score_distribution": report.score_distribution,
        "refinement_stats": report.refinement_stats,
        "avg_engagement_by_score_band": {
            k: round(v, 2) for k, v in report.avg_engagement_by_score_band.items()
        },
        "weekly_trends": analytics.trend(content_type=args.content_type, weeks=8),
        "filter_effectiveness": analytics.filter_effectiveness(days=args.days),
        "score_engagement_correlation": analytics.score_engagement_correlation(
            content_type=args.content_type
        )[:20],  # Limit to 20 most recent
    }

    return json.dumps(data, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate pipeline analytics report"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to look back (default: 30)"
    )
    parser.add_argument(
        "--content-type",
        default="x_thread",
        choices=["x_post", "x_thread"],
        help="Content type to analyze (default: x_thread)"
    )
    parser.add_argument(
        "--format",
        default="text",
        choices=["text", "json"],
        help="Output format (default: text)"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.WARNING,  # Suppress info logs for cleaner output
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        analytics = PipelineAnalytics(db)

        if args.format == "text":
            output = format_text_report(analytics, args)
        else:
            output = format_json_report(analytics, args)

        print(output)


if __name__ == "__main__":
    main()
