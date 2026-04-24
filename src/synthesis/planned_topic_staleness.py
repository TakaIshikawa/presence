"""Find stale planned topics before they keep steering generation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any


STALE_CLASSIFICATIONS = {"overdue", "campaign_ended", "missing_target_date"}


@dataclass(frozen=True)
class PlannedTopicStaleness:
    topic_id: int
    campaign_id: int | None
    classification: str
    days_overdue: int | None
    recommendation: str
    reason: str
    topic: str
    target_date: str | None
    campaign_end_date: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_date(value: str | None) -> date | None:
    """Parse an ISO date/datetime string to a date."""
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return datetime.fromisoformat(text).date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def analyze_planned_topic_staleness(
    planned_topics: list[dict[str, Any]],
    *,
    today: date | datetime | None = None,
    days_overdue: int = 0,
    campaign_id: int | None = None,
) -> list[PlannedTopicStaleness]:
    """Classify planned topics that match the requested staleness criteria.

    Only rows with ``status='planned'`` and no linked content are eligible. A
    positive ``days_overdue`` requires target dates to be at least that many
    whole days late before they classify as overdue.
    """
    if days_overdue < 0:
        raise ValueError("days_overdue must be non-negative")

    current_date = _coerce_date(today)
    stale: list[PlannedTopicStaleness] = []

    for row in planned_topics:
        if row.get("status") != "planned" or row.get("content_id") is not None:
            continue
        if campaign_id is not None and row.get("campaign_id") != campaign_id:
            continue

        item = classify_planned_topic(row, today=current_date, days_overdue=days_overdue)
        if item is not None:
            stale.append(item)

    return sorted(
        stale,
        key=lambda item: (
            item.campaign_id is None,
            item.campaign_id or 0,
            item.days_overdue is None,
            -(item.days_overdue or 0),
            item.topic_id,
        ),
    )


def classify_planned_topic(
    row: dict[str, Any],
    *,
    today: date,
    days_overdue: int = 0,
) -> PlannedTopicStaleness | None:
    """Classify a single planned topic if it is stale."""
    target = parse_date(row.get("target_date"))
    campaign_end = parse_date(row.get("campaign_end_date"))
    topic_id = int(row["id"])
    topic = str(row.get("topic") or "")
    campaign_id = row.get("campaign_id")

    if target is None:
        return PlannedTopicStaleness(
            topic_id=topic_id,
            campaign_id=campaign_id,
            classification="missing_target_date",
            days_overdue=None,
            recommendation="review",
            reason=f"planned topic #{topic_id} has no target date",
            topic=topic,
            target_date=row.get("target_date"),
            campaign_end_date=row.get("campaign_end_date"),
        )

    if campaign_end is not None and campaign_end < today:
        overdue_by = max(0, (today - target).days)
        return PlannedTopicStaleness(
            topic_id=topic_id,
            campaign_id=campaign_id,
            classification="campaign_ended",
            days_overdue=overdue_by,
            recommendation="skip",
            reason=(
                f"campaign ended on {campaign_end.isoformat()} before planned topic "
                f"#{topic_id} generated"
            ),
            topic=topic,
            target_date=row.get("target_date"),
            campaign_end_date=row.get("campaign_end_date"),
        )

    overdue_by = (today - target).days
    if overdue_by >= days_overdue and overdue_by > 0:
        return PlannedTopicStaleness(
            topic_id=topic_id,
            campaign_id=campaign_id,
            classification="overdue",
            days_overdue=overdue_by,
            recommendation="reschedule",
            reason=f"target date {target.isoformat()} is {overdue_by} day(s) overdue",
            topic=topic,
            target_date=row.get("target_date"),
            campaign_end_date=row.get("campaign_end_date"),
        )

    return None


def fetch_planned_topic_rows(db, campaign_id: int | None = None) -> list[dict[str, Any]]:
    """Read planned topics with campaign dates from SQLite."""
    sql = """SELECT pt.*,
                    cc.end_date AS campaign_end_date
             FROM planned_topics pt
             LEFT JOIN content_campaigns cc ON cc.id = pt.campaign_id
             WHERE pt.status = 'planned'"""
    params: list[Any] = []
    if campaign_id is not None:
        sql += " AND pt.campaign_id = ?"
        params.append(campaign_id)
    sql += " ORDER BY pt.target_date ASC NULLS LAST, pt.id ASC"
    return [dict(row) for row in db.conn.execute(sql, params).fetchall()]


def scan_planned_topic_staleness(
    db,
    *,
    today: date | datetime | None = None,
    days_overdue: int = 0,
    campaign_id: int | None = None,
) -> list[PlannedTopicStaleness]:
    """Fetch and classify stale planned topics from the database."""
    rows = fetch_planned_topic_rows(db, campaign_id=campaign_id)
    return analyze_planned_topic_staleness(
        rows,
        today=today,
        days_overdue=days_overdue,
        campaign_id=campaign_id,
    )


def mark_stale_topics_skipped(db, stale_topics: list[PlannedTopicStaleness]) -> list[dict[str, Any]]:
    """Mark only stale planned topic IDs as skipped and return update details."""
    updates = []
    for item in stale_topics:
        cursor = db.conn.execute(
            """UPDATE planned_topics
               SET status = 'skipped'
               WHERE id = ?
                 AND status = 'planned'
                 AND content_id IS NULL""",
            (item.topic_id,),
        )
        updates.append(
            {
                "topic_id": item.topic_id,
                "updated": cursor.rowcount == 1,
                "reason": item.reason,
            }
        )
    db.conn.commit()
    return updates


def staleness_to_dict(items: list[PlannedTopicStaleness]) -> list[dict[str, Any]]:
    return [item.to_dict() for item in items]


def _coerce_date(value: date | datetime | None) -> date:
    if value is None:
        return datetime.now(timezone.utc).date()
    if isinstance(value, datetime):
        return value.date()
    return value
