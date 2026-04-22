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
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context
from evaluation.topic_extractor import TopicExtractor, TOPIC_TAXONOMY

logger = logging.getLogger(__name__)

UID_DOMAIN = "presence.local"
DEFAULT_ICS_OUTPUT = "content-calendar.ics"


def validate_date(date_value: str, field_name: str = "date") -> None:
    """Validate an ISO date or datetime string."""
    if not date_value:
        return
    try:
        datetime.fromisoformat(date_value)
    except ValueError:
        logger.error(f"Invalid {field_name} format '{date_value}'. Use YYYY-MM-DD.")
        sys.exit(1)


def validate_limit(limit_value: int, field_name: str) -> None:
    """Validate an optional positive pacing limit."""
    if limit_value is not None and limit_value < 1:
        logger.error(f"Invalid {field_name}: must be a positive integer.")
        sys.exit(1)


def validate_days(days: int) -> None:
    """Validate a positive lookahead period."""
    if days < 1:
        logger.error("Invalid days: must be a positive integer.")
        sys.exit(1)


def parse_calendar_datetime(value: str) -> datetime | None:
    """Parse an ISO date or datetime into an aware datetime."""
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_calendar_date(value: str) -> date | None:
    """Parse an ISO date or datetime into a calendar date."""
    if not value:
        return None
    return parse_calendar_datetime(value).date()


def ics_escape(value: object) -> str:
    """Escape text for an iCalendar TEXT value."""
    text = "" if value is None else str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("\n", "\\n")
        .replace(";", "\\;")
        .replace(",", "\\,")
    )


def fold_ics_line(line: str) -> list[str]:
    """Fold an iCalendar content line."""
    if len(line) <= 75:
        return [line]

    lines = []
    remaining = line
    first = True
    while remaining:
        width = 75 if first else 74
        chunk = remaining[:width]
        remaining = remaining[width:]
        lines.append(chunk if first else f" {chunk}")
        first = False
    return lines


def format_ics_datetime(value: datetime) -> str:
    """Format a datetime as UTC for iCalendar."""
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def format_ics_date(value: date) -> str:
    """Format a date for iCalendar."""
    return value.strftime("%Y%m%d")


def add_ics_line(lines: list[str], name: str, value: object, params: str = "") -> None:
    """Append one escaped folded iCalendar content line."""
    for folded in fold_ics_line(f"{name}{params}:{ics_escape(value)}"):
        lines.append(folded)


def add_ics_list_line(lines: list[str], name: str, values: list[object]) -> None:
    """Append a folded comma-separated iCalendar value list."""
    value = ",".join(ics_escape(item) for item in values)
    for folded in fold_ics_line(f"{name}:{value}"):
        lines.append(folded)


def build_ics_event(event: dict, dtstamp: datetime) -> list[str]:
    """Build a VEVENT block."""
    lines = ["BEGIN:VEVENT"]
    add_ics_line(lines, "UID", event["uid"])
    lines.append(f"DTSTAMP:{format_ics_datetime(dtstamp)}")

    starts_at = event["starts_at"]
    if event.get("all_day"):
        lines.append(f"DTSTART;VALUE=DATE:{format_ics_date(starts_at)}")
        lines.append(f"DTEND;VALUE=DATE:{format_ics_date(starts_at + timedelta(days=1))}")
    else:
        lines.append(f"DTSTART:{format_ics_datetime(starts_at)}")

    if event.get("created_at"):
        lines.append(f"CREATED:{format_ics_datetime(event['created_at'])}")
    if event.get("last_modified"):
        lines.append(f"LAST-MODIFIED:{format_ics_datetime(event['last_modified'])}")

    add_ics_line(lines, "SUMMARY", event["summary"])
    add_ics_line(lines, "DESCRIPTION", event["description"])
    if event.get("categories"):
        add_ics_list_line(lines, "CATEGORIES", event["categories"])
    for key, value in event.get("metadata", {}).items():
        if value not in (None, ""):
            add_ics_line(lines, key, value)
    lines.append("END:VEVENT")
    return lines


def write_ics(events: list[dict], output: str | Path, now: datetime = None) -> None:
    """Write a dependency-free iCalendar file."""
    dtstamp = now or datetime.now(timezone.utc)
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Presence//Content Calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    for event in sorted(events, key=lambda item: (item["starts_at"], item["uid"])):
        lines.extend(build_ics_event(event, dtstamp))
    lines.append("END:VCALENDAR")

    Path(output).write_text("\r\n".join(lines) + "\r\n", encoding="utf-8")


def _description_from_pairs(pairs: list[tuple[str, object]]) -> str:
    """Build a compact multi-line event description."""
    return "\n".join(f"{label}: {value}" for label, value in pairs if value not in (None, ""))


def get_planned_topic_ics_events(db, now: datetime, days: int) -> list[dict]:
    """Return future planned topic events for the export window."""
    today = now.astimezone(timezone.utc).date()
    through = today + timedelta(days=days)
    events = []

    for plan in db.get_planned_topics(status="planned"):
        target = plan.get("target_date")
        target_day = parse_calendar_date(target)
        if target_day is None or target_day < today or target_day > through:
            continue

        topic = plan["topic"]
        campaign_name = plan.get("campaign_name")
        description = _description_from_pairs([
            ("Type", "Planned topic"),
            ("Topic", topic),
            ("Angle", plan.get("angle")),
            ("Campaign", campaign_name),
            ("Campaign goal", plan.get("campaign_goal")),
            ("Status", plan.get("status")),
            ("Planned topic ID", plan.get("id")),
        ])

        categories = ["planned", f"topic:{topic}"]
        if campaign_name:
            categories.append(f"campaign:{campaign_name}")

        events.append({
            "uid": f"planned-topic-{plan['id']}@{UID_DOMAIN}",
            "starts_at": target_day,
            "all_day": True,
            "created_at": parse_calendar_datetime(plan.get("created_at")),
            "summary": f"Planned: {topic}",
            "description": description,
            "categories": categories,
            "metadata": {
                "X-PRESENCE-TYPE": "PLANNED_TOPIC",
                "X-PRESENCE-PLANNED-TOPIC-ID": plan.get("id"),
                "X-PRESENCE-TOPIC": topic,
                "X-PRESENCE-CAMPAIGN-ID": plan.get("campaign_id"),
                "X-PRESENCE-CAMPAIGN": campaign_name,
            },
        })

    return events


def get_queued_publication_ics_events(db, now: datetime, days: int) -> list[dict]:
    """Return queued publication events for the export window."""
    through = now + timedelta(days=days)
    cursor = db.conn.execute(
        """SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform AS queue_platform,
                  pq.status AS queue_status,
                  pq.error AS queue_error,
                  pq.created_at AS queue_created_at,
                  gc.content,
                  gc.content_type,
                  pt.id AS planned_topic_id,
                  pt.topic AS topic,
                  pt.angle AS angle,
                  pt.campaign_id,
                  cc.name AS campaign_name,
                  cc.goal AS campaign_goal
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           LEFT JOIN planned_topics pt ON pt.content_id = gc.id
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE pq.status IN ('queued', 'failed')
           ORDER BY pq.scheduled_at ASC, pq.id ASC"""
    )

    events = []
    for row in cursor.fetchall():
        item = dict(row)
        scheduled_at = parse_calendar_datetime(item.get("scheduled_at"))
        if scheduled_at is None or scheduled_at > through:
            continue

        platform = item.get("queue_platform") or "all"
        platform_label = platform if platform != "all" else "all platforms"
        topic = item.get("topic")
        campaign_name = item.get("campaign_name")
        content_preview = (item.get("content") or "").strip()
        if len(content_preview) > 280:
            content_preview = f"{content_preview[:277]}..."

        description = _description_from_pairs([
            ("Type", "Queued publication"),
            ("Platform", platform_label),
            ("Status", item.get("queue_status")),
            ("Topic", topic),
            ("Angle", item.get("angle")),
            ("Campaign", campaign_name),
            ("Campaign goal", item.get("campaign_goal")),
            ("Content type", item.get("content_type")),
            ("Content ID", item.get("content_id")),
            ("Queue ID", item.get("queue_id")),
            ("Error", item.get("queue_error")),
            ("Content", content_preview),
        ])

        categories = ["queued", f"platform:{platform}"]
        if topic:
            categories.append(f"topic:{topic}")
        if campaign_name:
            categories.append(f"campaign:{campaign_name}")

        events.append({
            "uid": f"publish-queue-{item['queue_id']}@{UID_DOMAIN}",
            "starts_at": scheduled_at,
            "all_day": False,
            "created_at": parse_calendar_datetime(item.get("queue_created_at")),
            "summary": f"Publish ({platform_label}): {item.get('content_type')}",
            "description": description,
            "categories": categories,
            "metadata": {
                "X-PRESENCE-TYPE": "PUBLISH_QUEUE",
                "X-PRESENCE-QUEUE-ID": item.get("queue_id"),
                "X-PRESENCE-CONTENT-ID": item.get("content_id"),
                "X-PRESENCE-PLATFORM": platform,
                "X-PRESENCE-TOPIC": topic,
                "X-PRESENCE-PLANNED-TOPIC-ID": item.get("planned_topic_id"),
                "X-PRESENCE-CAMPAIGN-ID": item.get("campaign_id"),
                "X-PRESENCE-CAMPAIGN": campaign_name,
            },
        })

    return events


def get_content_publication_ics_events(db, now: datetime, days: int) -> list[dict]:
    """Return durable publication retry events for the export window."""
    through = now + timedelta(days=days)
    cursor = db.conn.execute(
        """SELECT cp.id AS publication_id,
                  cp.content_id,
                  cp.platform,
                  cp.status,
                  cp.next_retry_at,
                  cp.updated_at,
                  cp.error,
                  cp.error_category,
                  cp.attempt_count,
                  gc.content,
                  gc.content_type,
                  pt.id AS planned_topic_id,
                  pt.topic AS topic,
                  pt.angle AS angle,
                  pt.campaign_id,
                  cc.name AS campaign_name,
                  cc.goal AS campaign_goal
           FROM content_publications cp
           INNER JOIN generated_content gc ON gc.id = cp.content_id
           LEFT JOIN planned_topics pt ON pt.content_id = gc.id
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE cp.status = 'failed'
             AND cp.next_retry_at IS NOT NULL
           ORDER BY cp.next_retry_at ASC, cp.id ASC"""
    )

    events = []
    for row in cursor.fetchall():
        item = dict(row)
        retry_at = parse_calendar_datetime(item.get("next_retry_at"))
        if retry_at is None or retry_at > through:
            continue

        topic = item.get("topic")
        campaign_name = item.get("campaign_name")
        content_preview = (item.get("content") or "").strip()
        if len(content_preview) > 280:
            content_preview = f"{content_preview[:277]}..."

        description = _description_from_pairs([
            ("Type", "Publication retry"),
            ("Platform", item.get("platform")),
            ("Status", item.get("status")),
            ("Topic", topic),
            ("Angle", item.get("angle")),
            ("Campaign", campaign_name),
            ("Campaign goal", item.get("campaign_goal")),
            ("Content type", item.get("content_type")),
            ("Content ID", item.get("content_id")),
            ("Publication ID", item.get("publication_id")),
            ("Attempt count", item.get("attempt_count")),
            ("Error category", item.get("error_category")),
            ("Error", item.get("error")),
            ("Content", content_preview),
        ])

        categories = ["retry", f"platform:{item.get('platform')}"]
        if topic:
            categories.append(f"topic:{topic}")
        if campaign_name:
            categories.append(f"campaign:{campaign_name}")

        events.append({
            "uid": f"content-publication-{item['publication_id']}@{UID_DOMAIN}",
            "starts_at": retry_at,
            "all_day": False,
            "last_modified": parse_calendar_datetime(item.get("updated_at")),
            "summary": f"Retry publish ({item.get('platform')}): {item.get('content_type')}",
            "description": description,
            "categories": categories,
            "metadata": {
                "X-PRESENCE-TYPE": "CONTENT_PUBLICATION",
                "X-PRESENCE-PUBLICATION-ID": item.get("publication_id"),
                "X-PRESENCE-CONTENT-ID": item.get("content_id"),
                "X-PRESENCE-PLATFORM": item.get("platform"),
                "X-PRESENCE-TOPIC": topic,
                "X-PRESENCE-PLANNED-TOPIC-ID": item.get("planned_topic_id"),
                "X-PRESENCE-CAMPAIGN-ID": item.get("campaign_id"),
                "X-PRESENCE-CAMPAIGN": campaign_name,
            },
        })

    return events


def cmd_export_ics(
    db,
    days: int = 30,
    output: str = DEFAULT_ICS_OUTPUT,
    include_queued: bool = True,
    include_planned: bool = True,
):
    """Export planned topics and queued publications to an iCalendar file."""
    validate_days(days)
    if not include_queued and not include_planned:
        logger.error("Nothing to export: enable --include-queued or --include-planned.")
        sys.exit(1)

    now = datetime.now(timezone.utc)
    events = []
    if include_planned:
        events.extend(get_planned_topic_ics_events(db, now=now, days=days))
    if include_queued:
        events.extend(get_queued_publication_ics_events(db, now=now, days=days))
        events.extend(get_content_publication_ics_events(db, now=now, days=days))

    write_ics(events, output=output, now=now)
    logger.info(f"Exported {len(events)} calendar events to {output}")


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
    else:
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
        top_topic = frequencies[0]
        if top_topic["count"] >= 5:
            logger.info(f"⚠️  Topic saturation warning: '{top_topic['topic']}' appears {top_topic['count']} times")
            logger.info(f"   Consider diversifying content to avoid repetition.")
            logger.info("")

    # Planned topics summary
    planned = db.get_planned_topics(status="planned")
    if planned:
        logger.info(f"Upcoming Planned Topics ({len(planned)}):")

        grouped = defaultdict(list)
        campaign_labels = {}
        for plan in planned:
            campaign_id = plan.get("campaign_id")
            group_key = campaign_id if campaign_id is not None else "uncampaigned"
            grouped[group_key].append(plan)
            if campaign_id is not None:
                campaign_labels[group_key] = plan.get("campaign_name") or f"Campaign {campaign_id}"
            else:
                campaign_labels[group_key] = "No campaign"

        shown = 0
        for group_key, plans in grouped.items():
            if shown >= 5:
                break
            logger.info(f"  {campaign_labels[group_key]}:")
            for plan in plans:
                if shown >= 5:
                    break
                topic = plan["topic"]
                angle = plan["angle"] or "(no specific angle)"
                target = plan["target_date"][:10] if plan["target_date"] else "unscheduled"
                logger.info(f"    • {topic:20s} {angle:40s} target: {target}")
                shown += 1

        if len(planned) > 5:
            logger.info(f"  ... and {len(planned) - 5} more (use 'list' to see all)")
        logger.info("")

    logger.info("=" * 70)
    logger.info("")


def cmd_plan(
    db,
    topic: str,
    angle: str = None,
    target_date: str = None,
    campaign_id: int = None
):
    """Add a planned topic to the calendar."""
    if topic not in TOPIC_TAXONOMY:
        logger.error(f"Invalid topic '{topic}'. Must be one of: {', '.join(TOPIC_TAXONOMY)}")
        sys.exit(1)

    # Validate date format if provided
    validate_date(target_date)

    campaign = None
    if campaign_id is not None:
        campaign = db.get_campaign(campaign_id)
        if campaign is None:
            logger.error(f"Campaign {campaign_id} does not exist.")
            sys.exit(1)

    planned_id = db.insert_planned_topic(
        topic=topic,
        angle=angle,
        target_date=target_date,
        campaign_id=campaign_id
    )

    logger.info(f"Planned topic added (ID: {planned_id})")
    logger.info(f"  Topic:  {topic}")
    if angle:
        logger.info(f"  Angle:  {angle}")
    if target_date:
        logger.info(f"  Target: {target_date}")
    if campaign:
        logger.info(f"  Campaign: {campaign['name']} (ID: {campaign_id})")
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
    logger.info(f"  {'ID':>4s}  {'Topic':20s}  {'Angle':40s}  {'Target Date':12s}  {'Campaign':24s}")
    logger.info(f"  {'-'*4:>4s}  {'-'*20:20s}  {'-'*40:40s}  {'-'*12:12s}  {'-'*24:24s}")

    for plan in planned:
        plan_id = plan["id"]
        topic = plan["topic"]
        angle = plan["angle"] or ""
        target = plan["target_date"][:10] if plan["target_date"] else ""
        campaign = plan["campaign_name"] or ""

        logger.info(f"  {plan_id:4d}  {topic:20s}  {angle:40s}  {target:12s}  {campaign:24s}")

    logger.info("")


def cmd_campaign_create(
    db,
    name: str,
    goal: str = None,
    start_date: str = None,
    end_date: str = None,
    daily_limit: int = None,
    weekly_limit: int = None,
    status: str = "planned"
):
    """Create a campaign for grouping planned topics."""
    validate_date(start_date, "start date")
    validate_date(end_date, "end date")
    validate_limit(daily_limit, "daily limit")
    validate_limit(weekly_limit, "weekly limit")

    campaign_id = db.create_campaign(
        name=name,
        goal=goal,
        start_date=start_date,
        end_date=end_date,
        daily_limit=daily_limit,
        weekly_limit=weekly_limit,
        status=status
    )

    logger.info(f"Campaign created (ID: {campaign_id})")
    logger.info(f"  Name:   {name}")
    if goal:
        logger.info(f"  Goal:   {goal}")
    if start_date:
        logger.info(f"  Start:  {start_date}")
    if end_date:
        logger.info(f"  End:    {end_date}")
    if daily_limit:
        logger.info(f"  Daily limit:  {daily_limit}")
    if weekly_limit:
        logger.info(f"  Weekly limit: {weekly_limit}")
    logger.info(f"  Status: {status}")
    logger.info("")


def cmd_campaign_list(db, status: str = None):
    """List content campaigns."""
    campaigns = db.get_campaigns(status=status)

    if not campaigns:
        logger.info("No campaigns.")
        return

    logger.info("")
    logger.info(f"Campaigns ({len(campaigns)}):")
    logger.info("")
    logger.info(
        f"  {'ID':>4s}  {'Name':24s}  {'Status':10s}  {'Start':12s}  {'End':12s}  "
        f"{'Daily':>6s}  {'Weekly':>6s}  {'Goal':36s}"
    )
    logger.info(
        f"  {'-'*4:>4s}  {'-'*24:24s}  {'-'*10:10s}  {'-'*12:12s}  {'-'*12:12s}  "
        f"{'-'*6:>6s}  {'-'*6:>6s}  {'-'*36:36s}"
    )

    for campaign in campaigns:
        campaign_id = campaign["id"]
        name = campaign["name"]
        campaign_status = campaign["status"]
        start = campaign["start_date"][:10] if campaign["start_date"] else ""
        end = campaign["end_date"][:10] if campaign["end_date"] else ""
        daily_limit = str(campaign["daily_limit"] or "")
        weekly_limit = str(campaign["weekly_limit"] or "")
        goal = campaign["goal"] or ""

        logger.info(
            f"  {campaign_id:4d}  {name:24s}  {campaign_status:10s}  {start:12s}  {end:12s}  "
            f"{daily_limit:>6s}  {weekly_limit:>6s}  {goal:36s}"
        )

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
    plan_parser.add_argument(
        "--campaign",
        dest="campaign_id",
        type=int,
        help="Campaign ID to group this planned topic under"
    )

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List all planned topics"
    )

    # iCalendar export command
    export_ics_parser = subparsers.add_parser(
        "export-ics",
        help="Export future planned topics and queued publications to .ics"
    )
    export_ics_parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of future days to export (default: 30)"
    )
    export_ics_parser.add_argument(
        "--output",
        default=DEFAULT_ICS_OUTPUT,
        help=f"Output .ics path (default: {DEFAULT_ICS_OUTPUT})"
    )
    export_ics_parser.add_argument(
        "--include-queued",
        action="store_true",
        help="Include queued publications and retry events"
    )
    export_ics_parser.add_argument(
        "--include-planned",
        action="store_true",
        help="Include planned topic target dates"
    )

    # Campaign commands
    campaign_parser = subparsers.add_parser(
        "campaign",
        help="Manage multi-post content campaigns"
    )
    campaign_subparsers = campaign_parser.add_subparsers(
        dest="campaign_command",
        required=True
    )

    campaign_create_parser = campaign_subparsers.add_parser(
        "create",
        help="Create a content campaign"
    )
    campaign_create_parser.add_argument("name", help="Campaign name")
    campaign_create_parser.add_argument("--goal", help="Campaign goal")
    campaign_create_parser.add_argument("--start-date", help="Campaign start date (YYYY-MM-DD)")
    campaign_create_parser.add_argument("--end-date", help="Campaign end date (YYYY-MM-DD)")
    campaign_create_parser.add_argument("--daily-limit", type=int, help="Maximum campaign items per day")
    campaign_create_parser.add_argument("--weekly-limit", type=int, help="Maximum campaign items per week")
    campaign_create_parser.add_argument(
        "--status",
        default="planned",
        help="Campaign status (default: planned)"
    )

    campaign_list_parser = campaign_subparsers.add_parser(
        "list",
        help="List content campaigns"
    )
    campaign_list_parser.add_argument("--status", help="Filter campaigns by status")

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
                target_date=args.target_date,
                campaign_id=args.campaign_id
            )
        elif args.command == "list":
            cmd_list(db)
        elif args.command == "export-ics":
            include_any = args.include_queued or args.include_planned
            cmd_export_ics(
                db,
                days=args.days,
                output=args.output,
                include_queued=args.include_queued if include_any else True,
                include_planned=args.include_planned if include_any else True
            )
        elif args.command == "campaign":
            if args.campaign_command == "create":
                cmd_campaign_create(
                    db,
                    name=args.name,
                    goal=args.goal,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    daily_limit=args.daily_limit,
                    weekly_limit=args.weekly_limit,
                    status=args.status
                )
            elif args.campaign_command == "list":
                cmd_campaign_list(db, status=args.status)


if __name__ == "__main__":
    main()
