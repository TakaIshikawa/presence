"""Expand active content campaigns into planned topic calendars."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable


DEFAULT_ALLOWED_WEEKDAYS = (0, 1, 2, 3, 4, 5, 6)
DEFAULT_MAX_TOPICS_PER_WEEK = 3


@dataclass(frozen=True)
class ScheduledCampaignTopic:
    """One scheduler decision for a campaign/date/topic slot."""

    status: str
    campaign_id: int
    campaign_name: str
    topic: str
    target_date: str
    angle: str | None = None
    record_id: int | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "campaign_id": self.campaign_id,
            "campaign_name": self.campaign_name,
            "topic": self.topic,
            "angle": self.angle,
            "target_date": self.target_date,
            "record_id": self.record_id,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CampaignScheduleReport:
    """Created and skipped scheduler output."""

    items: list[ScheduledCampaignTopic]

    @property
    def created(self) -> list[ScheduledCampaignTopic]:
        return [item for item in self.items if item.status == "created"]

    @property
    def proposed(self) -> list[ScheduledCampaignTopic]:
        return [item for item in self.items if item.status == "proposed"]

    @property
    def skipped(self) -> list[ScheduledCampaignTopic]:
        return [item for item in self.items if item.status == "skipped"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "created": [item.to_dict() for item in self.created],
            "proposed": [item.to_dict() for item in self.proposed],
            "skipped": [item.to_dict() for item in self.skipped],
            "summary": {
                "created": len(self.created),
                "proposed": len(self.proposed),
                "skipped": len(self.skipped),
            },
        }


class CampaignTopicScheduler:
    """Deterministic cadence scheduler for campaign planned topics."""

    def __init__(
        self,
        *,
        allowed_weekdays: Iterable[int] = DEFAULT_ALLOWED_WEEKDAYS,
        max_topics_per_week: int = DEFAULT_MAX_TOPICS_PER_WEEK,
    ) -> None:
        weekdays = tuple(int(day) for day in allowed_weekdays)
        if not weekdays:
            raise ValueError("allowed_weekdays must contain at least one weekday")
        invalid = [day for day in weekdays if day < 0 or day > 6]
        if invalid:
            raise ValueError("allowed_weekdays values must be between 0 and 6")
        if max_topics_per_week < 1:
            raise ValueError("max_topics_per_week must be a positive integer")
        self.allowed_weekdays = tuple(sorted(set(weekdays)))
        self.max_topics_per_week = int(max_topics_per_week)

    def expand_campaigns(
        self,
        campaigns: Iterable[dict[str, Any]],
        *,
        start_date: date | str,
        end_date: date | str,
        topic_rotation: Iterable[str] | None = None,
        angles: Iterable[str | None] | None = None,
        existing_topics: Iterable[dict[str, Any]] | None = None,
        dry_run: bool = False,
    ) -> CampaignScheduleReport:
        """Return planned topic decisions for active campaigns within a date window."""
        window_start = _parse_date(start_date, "start_date")
        window_end = _parse_date(end_date, "end_date")
        if window_end < window_start:
            raise ValueError("end_date must be on or after start_date")

        existing_keys = {
            (
                int(row["campaign_id"]),
                str(row["topic"]),
                _parse_date(row["target_date"], "target_date").isoformat(),
            )
            for row in existing_topics or []
            if row.get("campaign_id") is not None
            and row.get("topic")
            and row.get("target_date")
        }
        items: list[ScheduledCampaignTopic] = []

        for campaign in sorted(campaigns, key=_campaign_sort_key):
            if campaign.get("status") != "active":
                continue
            campaign_id = int(campaign["id"])
            campaign_name = str(campaign.get("name") or campaign_id)
            campaign_start = _max_date(
                window_start,
                _parse_optional_date(campaign.get("start_date"), "campaign start_date"),
            )
            campaign_end = _min_date(
                window_end,
                _parse_optional_date(campaign.get("end_date"), "campaign end_date"),
            )
            if campaign_end < campaign_start:
                continue

            rotation = _normalize_rotation(topic_rotation, campaign)
            angle_rotation = tuple(angles or ())
            slot_index = 0
            weekly_counts: dict[tuple[int, int], int] = {}
            for target_day in _date_range(campaign_start, campaign_end):
                if target_day.weekday() not in self.allowed_weekdays:
                    continue
                week_key = target_day.isocalendar()[:2]
                if weekly_counts.get(week_key, 0) >= self.max_topics_per_week:
                    continue
                topic = rotation[slot_index % len(rotation)]
                angle = (
                    angle_rotation[slot_index % len(angle_rotation)]
                    if angle_rotation
                    else None
                )
                target_iso = target_day.isoformat()
                key = (campaign_id, topic, target_iso)
                if key in existing_keys:
                    items.append(
                        ScheduledCampaignTopic(
                            status="skipped",
                            campaign_id=campaign_id,
                            campaign_name=campaign_name,
                            topic=topic,
                            angle=angle,
                            target_date=target_iso,
                            reason="existing planned topic",
                        )
                    )
                else:
                    items.append(
                        ScheduledCampaignTopic(
                            status="proposed" if dry_run else "created",
                            campaign_id=campaign_id,
                            campaign_name=campaign_name,
                            topic=topic,
                            angle=angle,
                            target_date=target_iso,
                            reason="dry run" if dry_run else "created",
                        )
                    )
                    existing_keys.add(key)
                weekly_counts[week_key] = weekly_counts.get(week_key, 0) + 1
                slot_index += 1

        return CampaignScheduleReport(items)


def schedule_campaign_topics(
    db: Any,
    *,
    campaigns: Iterable[dict[str, Any]] | None = None,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    days: int = 14,
    allowed_weekdays: Iterable[int] = DEFAULT_ALLOWED_WEEKDAYS,
    max_topics_per_week: int | None = None,
    topic_rotation: Iterable[str] | None = None,
    angles: Iterable[str | None] | None = None,
    campaign_id: int | None = None,
    dry_run: bool = False,
    now: datetime | date | None = None,
) -> CampaignScheduleReport:
    """Expand campaigns and persist missing planned topics unless dry-run is enabled."""
    if days < 1:
        raise ValueError("days must be a positive integer")

    window_start = _coerce_today(now) if start_date is None else _parse_date(start_date, "start_date")
    window_end = (
        window_start + timedelta(days=days - 1)
        if end_date is None
        else _parse_date(end_date, "end_date")
    )

    selected_campaigns = list(campaigns) if campaigns is not None else _load_campaigns(db, campaign_id)
    if campaign_id is not None and not selected_campaigns:
        raise ValueError(f"Campaign {campaign_id} does not exist")

    existing = _load_existing_campaign_topics(db, selected_campaigns)
    resolved_rotation = tuple(topic_rotation or _rotation_from_existing(existing))
    resolved_angles = tuple(angles or _angles_from_existing(existing))
    resolved_max = max_topics_per_week or _max_weekly_limit(selected_campaigns) or DEFAULT_MAX_TOPICS_PER_WEEK

    scheduler = CampaignTopicScheduler(
        allowed_weekdays=allowed_weekdays,
        max_topics_per_week=resolved_max,
    )
    report = scheduler.expand_campaigns(
        selected_campaigns,
        start_date=window_start,
        end_date=window_end,
        topic_rotation=resolved_rotation,
        angles=resolved_angles,
        existing_topics=existing,
        dry_run=dry_run,
    )

    persisted: list[ScheduledCampaignTopic] = []
    for item in report.items:
        if item.status != "created" or dry_run:
            persisted.append(item)
            continue
        existing_item = db.find_planned_topic(
            topic=item.topic,
            target_date=item.target_date,
            campaign_id=item.campaign_id,
        )
        if existing_item:
            persisted.append(
                ScheduledCampaignTopic(
                    status="skipped",
                    campaign_id=item.campaign_id,
                    campaign_name=item.campaign_name,
                    topic=item.topic,
                    angle=item.angle,
                    target_date=item.target_date,
                    record_id=existing_item["id"],
                    reason="existing planned topic",
                )
            )
            continue
        record_id = db.insert_planned_topic(
            topic=item.topic,
            angle=item.angle,
            target_date=item.target_date,
            campaign_id=item.campaign_id,
            status="planned",
        )
        persisted.append(
            ScheduledCampaignTopic(
                status=item.status,
                campaign_id=item.campaign_id,
                campaign_name=item.campaign_name,
                topic=item.topic,
                angle=item.angle,
                target_date=item.target_date,
                record_id=record_id,
                reason=item.reason,
            )
        )
    return CampaignScheduleReport(persisted)


def _load_campaigns(db: Any, campaign_id: int | None) -> list[dict[str, Any]]:
    if campaign_id is not None:
        campaign = db.get_campaign(campaign_id)
        return [campaign] if campaign else []
    return db.get_campaigns(status="active")


def _load_existing_campaign_topics(
    db: Any,
    campaigns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    campaign_ids = {campaign["id"] for campaign in campaigns}
    existing: list[dict[str, Any]] = []
    for status in ("planned", "generated", "skipped"):
        existing.extend(
            row
            for row in db.get_planned_topics(status=status)
            if row.get("campaign_id") in campaign_ids
        )
    return sorted(existing, key=lambda row: (row.get("target_date") or "", row.get("id") or 0))


def _rotation_from_existing(existing: Iterable[dict[str, Any]]) -> tuple[str, ...]:
    rotation = []
    seen = set()
    for row in existing:
        topic = row.get("topic")
        if topic and topic not in seen:
            rotation.append(str(topic))
            seen.add(topic)
    return tuple(rotation)


def _angles_from_existing(existing: Iterable[dict[str, Any]]) -> tuple[str | None, ...]:
    angles = []
    seen = set()
    for row in existing:
        angle = row.get("angle")
        if angle and angle not in seen:
            angles.append(str(angle))
            seen.add(angle)
    return tuple(angles)


def _max_weekly_limit(campaigns: list[dict[str, Any]]) -> int | None:
    limits = [int(campaign["weekly_limit"]) for campaign in campaigns if campaign.get("weekly_limit")]
    return min(limits) if limits else None


def _normalize_rotation(
    topic_rotation: Iterable[str] | None,
    campaign: dict[str, Any],
) -> tuple[str, ...]:
    rotation = tuple(str(topic).strip() for topic in topic_rotation or () if str(topic).strip())
    if rotation:
        return rotation
    fallback = str(campaign.get("name") or "").strip()
    if not fallback:
        raise ValueError("topic_rotation is required when campaign name is empty")
    return (fallback,)


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def _parse_optional_date(value: Any, field_name: str) -> date | None:
    if value in (None, ""):
        return None
    return _parse_date(value, field_name)


def _parse_date(value: Any, field_name: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be an ISO date string")
    try:
        return datetime.fromisoformat(value).date()
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date string") from exc


def _coerce_today(value: datetime | date | None) -> date:
    if value is None:
        return datetime.now(timezone.utc).date()
    if isinstance(value, datetime):
        return value.date()
    return value


def _max_date(left: date, right: date | None) -> date:
    if right is None:
        return left
    return max(left, right)


def _min_date(left: date, right: date | None) -> date:
    if right is None:
        return left
    return min(left, right)


def _campaign_sort_key(campaign: dict[str, Any]) -> tuple[str, int]:
    return (str(campaign.get("start_date") or ""), int(campaign.get("id") or 0))
