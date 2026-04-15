#!/usr/bin/env python3
"""Content calendar CLI for topic planning and gap analysis.

Provides commands to:
- Extract topics from published content (backfill)
- Generate topic frequency and gap reports
- Plan future topics
- List planned topics
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from evaluation.topic_extractor import TopicExtractor, TOPIC_TAXONOMY

logger = logging.getLogger(__name__)


def cmd_backfill(db, config):
    """Extract topics for all published content without topic entries."""
    content_items = db.get_content_without_topics()

    if not content_items:
        logger.info("No content items need topic extraction.")
        return

    logger.info(f"Found {len(content_items)} content items without topics.")
    logger.info("Starting topic extraction (this may take a while)...")

    extractor = TopicExtractor(api_key=config.anthropic_api_key)

    for i, item in enumerate(content_items, 1):
        content_id = item["id"]
        content_text = item["content"]
        content_type = item["content_type"]

        logger.info(f"  [{i}/{len(content_items)}] Processing content {content_id} ({content_type})...")

        topics = extractor.extract_topics(content_text)

        if topics:
            db.insert_content_topics(content_id, topics)
            topic_str = ", ".join(f"{t[0]}" for t in topics)
            logger.info(f"    Extracted topics: {topic_str}")
        else:
            logger.warning(f"    No topics extracted for content {content_id}")

    logger.info(f"Backfill complete. Processed {len(content_items)} items.")


def cmd_report(db, days: int = 30, min_gap_days: int = 7):
    """Generate topic frequency report and gap analysis."""
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"Content Calendar Report (last {days} days)")
    logger.info("=" * 70)
    logger.info("")

    # Topic frequency
    frequencies = db.get_topic_frequency(days=days)

    if not frequencies:
        logger.info("No topic data available. Run 'backfill' command first.")
        logger.info("")
        return

    logger.info("Topic Distribution:")
    logger.info("")

    # Find max count for bar scaling
    max_count = max(f["count"] for f in frequencies) if frequencies else 1
    bar_width = 40

    for freq in frequencies:
        topic = freq["topic"]
        count = freq["count"]
        last_date = freq["last_published_at"][:10] if freq["last_published_at"] else "N/A"

        # Create bar chart
        bar_length = int((count / max_count) * bar_width)
        bar = "█" * bar_length

        logger.info(f"  {topic:20s} {bar:40s} {count:3d}  (last: {last_date})")

    logger.info("")

    # Gap analysis
    gaps = db.get_topic_gaps(days=days, min_gap_days=min_gap_days)

    if gaps:
        logger.info(f"Topic Gaps (not covered in last {min_gap_days} days):")
        logger.info("")
        for gap in gaps:
            logger.info(f"  ⚠️  {gap}")
        logger.info("")
    else:
        logger.info("✓ No topic gaps detected — all topics covered recently.")
        logger.info("")

    # Saturation warnings
    if frequencies:
        top_topic = frequencies[0]
        if top_topic["count"] >= 5:
            logger.info(f"⚠️  Topic saturation warning: '{top_topic['topic']}' appears {top_topic['count']} times")
            logger.info(f"   Consider diversifying content to avoid repetition.")
            logger.info("")

    # Planned topics summary
    planned = db.get_planned_topics(status="planned")
    if planned:
        logger.info(f"Upcoming Planned Topics ({len(planned)}):")
        for plan in planned[:5]:  # Show first 5
            topic = plan["topic"]
            angle = plan["angle"] or "(no specific angle)"
            target = plan["target_date"][:10] if plan["target_date"] else "unscheduled"
            logger.info(f"  • {topic:20s} {angle:40s} target: {target}")
        if len(planned) > 5:
            logger.info(f"  ... and {len(planned) - 5} more (use 'list' to see all)")
        logger.info("")

    logger.info("=" * 70)
    logger.info("")


def cmd_plan(db, topic: str, angle: str = None, target_date: str = None):
    """Add a planned topic to the calendar."""
    if topic not in TOPIC_TAXONOMY:
        logger.error(f"Invalid topic '{topic}'. Must be one of: {', '.join(TOPIC_TAXONOMY)}")
        sys.exit(1)

    # Validate date format if provided
    if target_date:
        try:
            datetime.fromisoformat(target_date)
        except ValueError:
            logger.error(f"Invalid date format '{target_date}'. Use YYYY-MM-DD.")
            sys.exit(1)

    planned_id = db.insert_planned_topic(
        topic=topic,
        angle=angle,
        target_date=target_date
    )

    logger.info(f"Planned topic added (ID: {planned_id})")
    logger.info(f"  Topic:  {topic}")
    if angle:
        logger.info(f"  Angle:  {angle}")
    if target_date:
        logger.info(f"  Target: {target_date}")
    logger.info("")


def cmd_list(db):
    """List all planned topics."""
    planned = db.get_planned_topics(status="planned")

    if not planned:
        logger.info("No planned topics.")
        return

    logger.info("")
    logger.info(f"Planned Topics ({len(planned)}):")
    logger.info("")
    logger.info(f"  {'ID':>4s}  {'Topic':20s}  {'Angle':40s}  {'Target Date':12s}")
    logger.info(f"  {'-'*4:>4s}  {'-'*20:20s}  {'-'*40:40s}  {'-'*12:12s}")

    for plan in planned:
        plan_id = plan["id"]
        topic = plan["topic"]
        angle = plan["angle"] or ""
        target = plan["target_date"][:10] if plan["target_date"] else ""

        logger.info(f"  {plan_id:4d}  {topic:20s}  {angle:40s}  {target:12s}")

    logger.info("")


def main():
    parser = argparse.ArgumentParser(
        description="Content calendar for topic planning and gap analysis"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Backfill command
    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Extract topics for all published content without topic entries"
    )

    # Report command
    report_parser = subparsers.add_parser(
        "report",
        help="Generate topic frequency report and gap analysis"
    )
    report_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to analyze (default: 30)"
    )
    report_parser.add_argument(
        "--min-gap-days",
        type=int,
        default=7,
        help="Minimum days without coverage to count as gap (default: 7)"
    )

    # Plan command
    plan_parser = subparsers.add_parser(
        "plan",
        help="Add a planned topic to the calendar"
    )
    plan_parser.add_argument("topic", help="Topic from taxonomy")
    plan_parser.add_argument("angle", nargs="?", help="Specific angle to cover")
    plan_parser.add_argument(
        "--date",
        dest="target_date",
        help="Target publication date (YYYY-MM-DD)"
    )

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List all planned topics"
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    with script_context() as (config, db):
        if args.command == "backfill":
            cmd_backfill(db, config)
        elif args.command == "report":
            cmd_report(db, days=args.days, min_gap_days=args.min_gap_days)
        elif args.command == "plan":
            cmd_plan(
                db,
                topic=args.topic,
                angle=args.angle,
                target_date=args.target_date
            )
        elif args.command == "list":
            cmd_list(db)


if __name__ == "__main__":
    main()
