"""iCalendar export for planned topics and scheduled content."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable


UID_DOMAIN = "presence.local"
PRODID = "-//Presence//Content Calendar//EN"
URL_RE = re.compile(r"https?://[^\s<>)\"']+")


@dataclass(frozen=True)
class CalendarEvent:
    """Small, dependency-free event shape for text/calendar output."""

    uid: str
    starts_at: date | datetime
    summary: str
    description: str
    url: str | None = None
    all_day: bool = False


def parse_utc_datetime(value: Any) -> datetime | None:
    """Parse an ISO date/datetime as UTC."""
    if value in (None, ""):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_date(value: Any) -> date | None:
    """Parse an ISO date or datetime into a date."""
    if value in (None, ""):
        return None
    raw = str(value).strip()
    try:
        return date.fromisoformat(raw[:10])
    except ValueError:
        parsed = parse_utc_datetime(raw)
        return parsed.date() if parsed else None


def normalize_start(value: date | datetime) -> datetime:
    """Normalize a reporting start date/datetime to UTC."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def escape_text(value: Any) -> str:
    """Escape iCalendar TEXT fields."""
    text_value = "" if value is None else str(value)
    return (
        text_value.replace("\\", "\\\\")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\n")
        .replace(";", "\\;")
        .replace(",", "\\,")
    )


def fold_line(line: str) -> list[str]:
    """Fold an iCalendar content line at 75 characters."""
    if len(line) <= 75:
        return [line]

    folded: list[str] = []
    remaining = line
    first = True
    while remaining:
        width = 75 if first else 74
        chunk = remaining[:width]
        folded.append(chunk if first else f" {chunk}")
        remaining = remaining[width:]
        first = False
    return folded


def format_utc_datetime(value: datetime) -> str:
    """Format a datetime as a UTC iCalendar timestamp."""
    return value.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def format_date(value: date) -> str:
    """Format a date as an iCalendar DATE value."""
    return value.strftime("%Y%m%d")


def first_url(value: Any) -> str | None:
    """Return the first URL embedded in source material."""
    if value in (None, ""):
        return None
    match = URL_RE.search(str(value))
    return match.group(0).rstrip(".,;") if match else None


def description_from_pairs(pairs: Iterable[tuple[str, Any]]) -> str:
    """Build a compact multi-line event description."""
    return "\n".join(
        f"{label}: {value}" for label, value in pairs if value not in (None, "")
    )


def _add_text_line(lines: list[str], name: str, value: Any) -> None:
    for folded in fold_line(f"{name}:{escape_text(value)}"):
        lines.append(folded)


def _add_raw_line(lines: list[str], name: str, value: Any) -> None:
    for folded in fold_line(f"{name}:{value}"):
        lines.append(folded)


def _event_lines(event: CalendarEvent, dtstamp: datetime) -> list[str]:
    lines = ["BEGIN:VEVENT"]
    _add_text_line(lines, "UID", event.uid)
    _add_raw_line(lines, "DTSTAMP", format_utc_datetime(dtstamp))

    if event.all_day:
        starts_on = event.starts_at.date() if isinstance(event.starts_at, datetime) else event.starts_at
        _add_raw_line(lines, "DTSTART;VALUE=DATE", format_date(starts_on))
    else:
        starts_at = event.starts_at
        if isinstance(starts_at, date) and not isinstance(starts_at, datetime):
            starts_at = datetime.combine(starts_at, time.min, tzinfo=timezone.utc)
        _add_raw_line(lines, "DTSTART", format_utc_datetime(starts_at))

    _add_text_line(lines, "SUMMARY", event.summary)
    _add_text_line(lines, "DESCRIPTION", event.description)
    if event.url:
        _add_raw_line(lines, "URL", event.url)
    lines.append("END:VEVENT")
    return lines


def _sort_start(value: date | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return datetime.combine(value, time.min, tzinfo=timezone.utc)


def render_ics(
    events: Iterable[CalendarEvent],
    *,
    calendar_name: str = "Presence Content Calendar",
    dtstamp: datetime | None = None,
) -> str:
    """Render events as text/calendar output."""
    stamp = dtstamp or datetime(1970, 1, 1, tzinfo=timezone.utc)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    stamp = stamp.astimezone(timezone.utc)

    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        f"PRODID:{PRODID}",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
    ]
    _add_text_line(lines, "X-WR-CALNAME", calendar_name)
    for event in sorted(events, key=lambda item: (_sort_start(item.starts_at), item.uid)):
        lines.extend(_event_lines(event, stamp))
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines) + "\r\n"


def planned_topic_events(
    db: Any,
    *,
    start: date | datetime,
    days: int,
) -> list[CalendarEvent]:
    """Return VEVENT data for planned topics in the reporting window."""
    if days < 1:
        raise ValueError("days must be positive")

    start_day = normalize_start(start).date()
    end_day = start_day + timedelta(days=days)
    events: list[CalendarEvent] = []

    for topic in db.get_planned_topics(status="planned"):
        target_day = parse_date(topic.get("target_date"))
        if target_day is None or target_day < start_day or target_day > end_day:
            continue

        source_url = first_url(topic.get("source_material"))
        campaign_name = topic.get("campaign_name")
        description = description_from_pairs(
            [
                ("Type", "Planned topic"),
                ("Topic", topic.get("topic")),
                ("Angle", topic.get("angle")),
                ("Campaign", campaign_name),
                ("Source", topic.get("source_material")),
                ("Planned topic ID", topic.get("id")),
            ]
        )
        events.append(
            CalendarEvent(
                uid=f"planned-topic-{topic['id']}@{UID_DOMAIN}",
                starts_at=target_day,
                all_day=True,
                summary=f"Planned: {topic.get('topic')}",
                description=description,
                url=source_url,
            )
        )
    return events


def queue_events(
    db: Any,
    *,
    start: date | datetime,
    days: int,
) -> list[CalendarEvent]:
    """Return VEVENT data for scheduled queue items and publication retries."""
    if days < 1:
        raise ValueError("days must be positive")

    start_at = normalize_start(start)
    end_at = start_at + timedelta(days=days)
    events: list[CalendarEvent] = []

    queue_rows = db.conn.execute(
        """SELECT pq.id AS queue_id,
                  pq.content_id,
                  pq.scheduled_at,
                  pq.platform,
                  pq.status,
                  pq.error,
                  gc.content,
                  gc.content_type,
                  gc.published_url,
                  pt.id AS planned_topic_id,
                  pt.topic,
                  pt.angle,
                  pt.source_material,
                  cc.name AS campaign_name
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           LEFT JOIN planned_topics pt ON pt.content_id = gc.id
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE pq.status IN ('queued', 'failed')
           ORDER BY pq.scheduled_at ASC, pq.id ASC"""
    ).fetchall()

    for row in queue_rows:
        item = dict(row)
        scheduled_at = parse_utc_datetime(item.get("scheduled_at"))
        if scheduled_at is None or scheduled_at < start_at or scheduled_at > end_at:
            continue
        platform = item.get("platform") or "all"
        content_preview = (item.get("content") or "").strip()
        if len(content_preview) > 280:
            content_preview = f"{content_preview[:277]}..."
        description = description_from_pairs(
            [
                ("Type", "Queued publication"),
                ("Platform", platform),
                ("Status", item.get("status")),
                ("Topic", item.get("topic")),
                ("Angle", item.get("angle")),
                ("Campaign", item.get("campaign_name")),
                ("Content type", item.get("content_type")),
                ("Content ID", item.get("content_id")),
                ("Queue ID", item.get("queue_id")),
                ("Error", item.get("error")),
                ("Content", content_preview),
            ]
        )
        events.append(
            CalendarEvent(
                uid=f"publish-queue-{item['queue_id']}@{UID_DOMAIN}",
                starts_at=scheduled_at,
                summary=f"Publish ({platform}): {item.get('content_type')}",
                description=description,
                url=item.get("published_url") or first_url(item.get("source_material")),
            )
        )

    publication_rows = db.conn.execute(
        """SELECT cp.id AS publication_id,
                  cp.content_id,
                  cp.platform,
                  cp.status,
                  cp.next_retry_at,
                  cp.error,
                  cp.error_category,
                  cp.attempt_count,
                  gc.content,
                  gc.content_type,
                  gc.published_url,
                  pt.id AS planned_topic_id,
                  pt.topic,
                  pt.angle,
                  pt.source_material,
                  cc.name AS campaign_name
           FROM content_publications cp
           INNER JOIN generated_content gc ON gc.id = cp.content_id
           LEFT JOIN planned_topics pt ON pt.content_id = gc.id
           LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
           WHERE cp.status = 'failed'
             AND cp.next_retry_at IS NOT NULL
           ORDER BY cp.next_retry_at ASC, cp.id ASC"""
    ).fetchall()

    for row in publication_rows:
        item = dict(row)
        retry_at = parse_utc_datetime(item.get("next_retry_at"))
        if retry_at is None or retry_at < start_at or retry_at > end_at:
            continue
        content_preview = (item.get("content") or "").strip()
        if len(content_preview) > 280:
            content_preview = f"{content_preview[:277]}..."
        description = description_from_pairs(
            [
                ("Type", "Publication retry"),
                ("Platform", item.get("platform")),
                ("Status", item.get("status")),
                ("Topic", item.get("topic")),
                ("Angle", item.get("angle")),
                ("Campaign", item.get("campaign_name")),
                ("Content type", item.get("content_type")),
                ("Content ID", item.get("content_id")),
                ("Publication ID", item.get("publication_id")),
                ("Attempt count", item.get("attempt_count")),
                ("Error category", item.get("error_category")),
                ("Error", item.get("error")),
                ("Content", content_preview),
            ]
        )
        events.append(
            CalendarEvent(
                uid=f"content-publication-{item['publication_id']}@{UID_DOMAIN}",
                starts_at=retry_at,
                summary=f"Retry publish ({item.get('platform')}): {item.get('content_type')}",
                description=description,
                url=item.get("published_url") or first_url(item.get("source_material")),
            )
        )

    return events


def export_calendar(
    db: Any,
    *,
    start: date | datetime,
    days: int = 30,
    include_queue: bool = False,
    dtstamp: datetime | None = None,
) -> str:
    """Build text/calendar output for planned topics and optional queue items."""
    events = planned_topic_events(db, start=start, days=days)
    if include_queue:
        events.extend(queue_events(db, start=start, days=days))
    return render_ics(events, dtstamp=dtstamp or normalize_start(start))
