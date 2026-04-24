"""Audit scheduled publish queue items for platform timing collisions."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from evaluation.posting_schedule import next_allowed_slot
from output.publish_caps import DEFAULT_DAILY_PLATFORM_LIMITS

DEFAULT_COLLISION_WINDOW_MINUTES = 30
AUDIT_PLATFORMS = ("x", "bluesky")


@dataclass(frozen=True)
class ExpandedQueueItem:
    queue_id: int
    content_id: int
    queue_platform: str
    platform: str
    scheduled_at: datetime
    scheduled_at_raw: str
    content_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "queue_id": self.queue_id,
            "content_id": self.content_id,
            "queue_platform": self.queue_platform,
            "platform": self.platform,
            "scheduled_at": self.scheduled_at.isoformat(),
            "content_type": self.content_type,
        }


@dataclass(frozen=True)
class QueueCollisionGroup:
    platform: str
    items: list[ExpandedQueueItem]
    window_minutes: int

    @property
    def start_at(self) -> datetime:
        return self.items[0].scheduled_at

    @property
    def end_at(self) -> datetime:
        return self.items[-1].scheduled_at

    @property
    def queue_ids(self) -> list[int]:
        return sorted({item.queue_id for item in self.items})

    @property
    def deferred_queue_ids(self) -> list[int]:
        return sorted({item.queue_id for item in self.items[1:]})

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "window_minutes": self.window_minutes,
            "start_at": self.start_at.isoformat(),
            "end_at": self.end_at.isoformat(),
            "queue_ids": self.queue_ids,
            "deferred_queue_ids": self.deferred_queue_ids,
            "items": [item.to_dict() for item in self.items],
        }


@dataclass
class PublishQueueAuditResult:
    window_minutes: int
    collision_groups: list[QueueCollisionGroup]
    hold_reasons: dict[int, str] = field(default_factory=dict)
    applied_holds: list[dict[str, Any]] = field(default_factory=list)

    @property
    def collision_count(self) -> int:
        return len(self.collision_groups)

    @property
    def affected_queue_ids(self) -> list[int]:
        return sorted(self.hold_reasons)

    def to_dict(self) -> dict[str, Any]:
        return {
            "window_minutes": self.window_minutes,
            "collision_count": self.collision_count,
            "affected_queue_ids": self.affected_queue_ids,
            "hold_reasons": self.hold_reasons,
            "collision_groups": [group.to_dict() for group in self.collision_groups],
            "applied_holds": self.applied_holds,
        }


def audit_publish_queue(
    db,
    *,
    window_minutes: int = DEFAULT_COLLISION_WINDOW_MINUTES,
    daily_platform_limits: Mapping[str, int] | None = None,
    embargo_windows: list[Any] | None = None,
    apply_holds: bool = False,
) -> PublishQueueAuditResult:
    """Find queued items scheduled too close together for each platform."""
    if window_minutes <= 0:
        raise ValueError("window_minutes must be positive")

    rows = db.get_queued_publish_queue_items_for_audit()
    expanded = _expand_queue_rows(rows)
    groups = _collision_groups(expanded, window_minutes=window_minutes)
    hold_reasons = _hold_reasons(
        groups,
        window_minutes=window_minutes,
        daily_platform_limits=daily_platform_limits,
        embargo_windows=embargo_windows or [],
    )
    applied_holds: list[dict[str, Any]] = []
    if apply_holds and hold_reasons:
        applied_holds = db.apply_publish_queue_audit_holds(hold_reasons)

    return PublishQueueAuditResult(
        window_minutes=window_minutes,
        collision_groups=groups,
        hold_reasons=hold_reasons,
        applied_holds=applied_holds,
    )


def _expand_queue_rows(rows: list[dict[str, Any]]) -> list[ExpandedQueueItem]:
    expanded: list[ExpandedQueueItem] = []
    for row in rows:
        scheduled_at = _parse_scheduled_at(row["scheduled_at"])
        platform = row.get("platform") or "all"
        platforms = AUDIT_PLATFORMS if platform == "all" else (platform,)
        for target_platform in platforms:
            if target_platform not in AUDIT_PLATFORMS:
                continue
            expanded.append(
                ExpandedQueueItem(
                    queue_id=int(row["id"]),
                    content_id=int(row["content_id"]),
                    queue_platform=platform,
                    platform=target_platform,
                    scheduled_at=scheduled_at,
                    scheduled_at_raw=row["scheduled_at"],
                    content_type=row.get("content_type"),
                )
            )
    return expanded


def _collision_groups(
    expanded: list[ExpandedQueueItem],
    *,
    window_minutes: int,
) -> list[QueueCollisionGroup]:
    groups: list[QueueCollisionGroup] = []
    window = timedelta(minutes=window_minutes)

    for platform in AUDIT_PLATFORMS:
        platform_items = sorted(
            (item for item in expanded if item.platform == platform),
            key=lambda item: (item.scheduled_at, item.queue_id),
        )
        current: list[ExpandedQueueItem] = []
        for item in platform_items:
            if not current:
                current = [item]
                continue
            if item.scheduled_at - current[-1].scheduled_at <= window:
                current.append(item)
                continue
            if len(current) > 1:
                groups.append(
                    QueueCollisionGroup(
                        platform=platform,
                        items=current,
                        window_minutes=window_minutes,
                    )
                )
            current = [item]
        if len(current) > 1:
            groups.append(
                QueueCollisionGroup(
                    platform=platform,
                    items=current,
                    window_minutes=window_minutes,
                )
            )

    return groups


def _hold_reasons(
    groups: list[QueueCollisionGroup],
    *,
    window_minutes: int,
    daily_platform_limits: Mapping[str, int] | None,
    embargo_windows: list[Any],
) -> dict[int, str]:
    limits = dict(daily_platform_limits or DEFAULT_DAILY_PLATFORM_LIMITS)
    reasons: dict[int, list[str]] = {}

    for group in groups:
        for index, item in enumerate(group.items[1:], start=1):
            previous = group.items[index - 1]
            suggested_at = _suggested_slot(
                after=previous.scheduled_at + timedelta(minutes=window_minutes),
                platform=group.platform,
                planned_items=group.items[:index],
                limits=limits,
                embargo_windows=embargo_windows,
            )
            reasons.setdefault(item.queue_id, []).append(
                "publish_queue_collision: {platform} item {queue_id} at {scheduled_at} "
                "is within {window} minutes of queue item {previous_id}; "
                "suggest defer until {suggested_at}".format(
                    platform=group.platform,
                    queue_id=item.queue_id,
                    scheduled_at=item.scheduled_at.isoformat(),
                    window=window_minutes,
                    previous_id=previous.queue_id,
                    suggested_at=suggested_at.isoformat(),
                )
            )

    return {
        queue_id: " | ".join(platform_reasons)
        for queue_id, platform_reasons in sorted(reasons.items())
    }


def _suggested_slot(
    *,
    after: datetime,
    platform: str,
    planned_items: list[ExpandedQueueItem],
    limits: Mapping[str, int],
    embargo_windows: list[Any],
) -> datetime:
    candidate = next_allowed_slot(after, embargo_windows)
    limit = limits.get(platform)
    if not limit:
        return candidate

    while _planned_count_on_day(planned_items, platform, candidate) >= limit:
        candidate = next_allowed_slot(
            _next_utc_day(candidate),
            embargo_windows,
        )
    return candidate


def _planned_count_on_day(
    planned_items: list[ExpandedQueueItem],
    platform: str,
    candidate: datetime,
) -> int:
    start = candidate.astimezone(timezone.utc).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    end = start + timedelta(days=1)
    return sum(
        1
        for item in planned_items
        if item.platform == platform
        and start <= item.scheduled_at.astimezone(timezone.utc) < end
    )


def _next_utc_day(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    day_start = utc_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return day_start + timedelta(days=1)


def _parse_scheduled_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
