#!/usr/bin/env python3
"""Generate source quality scoring report.

Analyzes which curated knowledge sources (X accounts, blogs) consistently
contribute to high-engagement content.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context, update_monitoring
from knowledge.source_scorer import SourceScorer

logger = logging.getLogger(__name__)


def _int_config(value, default: int) -> int:
    return value if isinstance(value, int) and value > 0 else default


def _cooldown_config(value, default: int) -> int:
    return value if isinstance(value, int) and value >= 0 else default


def _source_health_config(config) -> tuple[int, int]:
    curated_sources = getattr(config, "curated_sources", None)
    return (
        _int_config(getattr(curated_sources, "source_failure_threshold", 3), 3),
        _cooldown_config(getattr(curated_sources, "source_cooldown_hours", 24), 24),
    )


def _print_quarantined_sources(db, failure_threshold: int, cooldown_hours: int) -> int:
    getter = getattr(db, "get_quarantined_curated_sources", None)
    if not callable(getter):
        return 0

    rows = getter(failure_threshold, cooldown_hours)
    if not rows:
        return 0

    print("QUARANTINED SOURCES (Skipped by Ingestion Health):")
    print(f"Threshold: {failure_threshold} consecutive failures; cooldown: {cooldown_hours} hours")
    print(f"{'Source':<28} {'Type':<12} {'Failures':<8} {'Last Failure':<25} {'Feed URL / Account':<36} Last Error")
    print("-" * 130)
    for row in rows:
        source_type = row.get("source_type", "")
        identifier = row.get("identifier", "")
        display = f"@{identifier}" if source_type == "x_account" else identifier
        locator = row.get("feed_url") or ("X API credentials/account access" if source_type == "x_account" else "")
        error = (row.get("last_error") or "").replace("\n", " ")
        if len(error) > 80:
            error = error[:77] + "..."
        if len(locator) > 34:
            locator = locator[:31] + "..."
        print(
            f"{display:<28} {source_type:<12} "
            f"{row.get('consecutive_failures') or 0!s:<8} "
            f"{row.get('last_failure_at') or 'unknown':<25} "
            f"{locator:<36} {error}"
        )
    print()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate source quality scoring report"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=90,
        help="Number of days to look back (default: 90)",
    )
    parser.add_argument(
        "--min-uses",
        type=int,
        default=2,
        help="Minimum uses required to score a source (default: 2)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        scorer = SourceScorer(db)
        failure_threshold, cooldown_hours = _source_health_config(config)

        logger.info(f"Computing source quality scores (last {args.days} days, min {args.min_uses} uses)...")
        scores = scorer.compute_scores(days=args.days, min_uses=args.min_uses)
        quarantined_count = _print_quarantined_sources(db, failure_threshold, cooldown_hours)

        if not scores:
            print("\nNo source quality data available yet.")
            print("Sources need to be used in published content with engagement metrics.")
            if quarantined_count:
                print("Resolve quarantined source health issues above before expecting new source data.")
            return

        print("\n" + "=" * 80)
        print("SOURCE QUALITY SCORING REPORT")
        print("=" * 80)
        print()

        # Tier breakdown
        tier_counts = {
            'gold': len([s for s in scores if s.tier == 'gold']),
            'silver': len([s for s in scores if s.tier == 'silver']),
            'bronze': len([s for s in scores if s.tier == 'bronze']),
        }

        print(f"Total sources scored: {len(scores)}")
        print(f"  Gold tier (top 20%):    {tier_counts['gold']} sources")
        print(f"  Silver tier (20-60%):   {tier_counts['silver']} sources")
        print(f"  Bronze tier (bottom 40%): {tier_counts['bronze']} sources")
        print()

        # Top 5 gold sources
        gold_sources = [s for s in scores if s.tier == 'gold']
        if gold_sources:
            print("TOP GOLD TIER SOURCES (Consistently Drive Engagement):")
            print(f"{'Author':<25} {'Type':<15} {'Uses':<6} {'Avg Eng':<10} {'Hit Rate':<10} {'Quality':<10}")
            print("-" * 86)
            for source in gold_sources[:5]:
                author_display = f"@{source.author}" if source.source_type == 'curated_x' else source.author
                print(
                    f"{author_display:<25} {source.source_type:<15} "
                    f"{source.usage_count:<6} {source.avg_engagement:<10.2f} "
                    f"{source.hit_rate:<10.1%} {source.quality_score:<10.3f}"
                )
            print()

        # Bottom 5 bronze sources
        bronze_sources = [s for s in scores if s.tier == 'bronze']
        if bronze_sources:
            print("BOTTOM BRONZE TIER SOURCES (Low Engagement Correlation):")
            print(f"{'Author':<25} {'Type':<15} {'Uses':<6} {'Avg Eng':<10} {'Hit Rate':<10} {'Quality':<10}")
            print("-" * 86)
            for source in bronze_sources[-5:]:
                author_display = f"@{source.author}" if source.source_type == 'curated_x' else source.author
                print(
                    f"{author_display:<25} {source.source_type:<15} "
                    f"{source.usage_count:<6} {source.avg_engagement:<10.2f} "
                    f"{source.hit_rate:<10.1%} {source.quality_score:<10.3f}"
                )
            print()

        # Retrieval boost context
        context = scorer.generate_retrieval_boost_context(days=args.days)
        if context:
            print("RETRIEVAL BOOST CONTEXT (for knowledge retrieval prompts):")
            print("-" * 80)
            print(context)
            print("-" * 80)
            print()

        # Stats summary
        avg_engagement_gold = sum(s.avg_engagement for s in gold_sources) / len(gold_sources) if gold_sources else 0
        avg_engagement_bronze = sum(s.avg_engagement for s in bronze_sources) / len(bronze_sources) if bronze_sources else 0
        avg_hit_rate_gold = sum(s.hit_rate for s in gold_sources) / len(gold_sources) if gold_sources else 0
        avg_hit_rate_bronze = sum(s.hit_rate for s in bronze_sources) / len(bronze_sources) if bronze_sources else 0

        print("PERFORMANCE SUMMARY:")
        print(f"  Gold tier avg engagement:   {avg_engagement_gold:.2f}")
        print(f"  Bronze tier avg engagement: {avg_engagement_bronze:.2f}")
        if avg_engagement_bronze > 0:
            lift = (avg_engagement_gold - avg_engagement_bronze) / avg_engagement_bronze * 100
            print(f"  Gold vs Bronze lift:        {lift:+.1f}%")
        print()
        print(f"  Gold tier avg hit rate:     {avg_hit_rate_gold:.1%}")
        print(f"  Bronze tier avg hit rate:   {avg_hit_rate_bronze:.1%}")
        print()

        update_monitoring('source_quality')
        logger.info("Source quality report complete.")


if __name__ == "__main__":
    main()
