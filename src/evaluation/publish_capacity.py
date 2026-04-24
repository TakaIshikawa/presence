"""Forecast publish queue capacity against caps and posting windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from evaluation.posting_schedule import (
    PostingScheduleAnalyzer,
    TimeWindow,
    embargo_windows_from_config,
    is_embargoed,
)
from output.publish_caps import (
    daily_platform_limits_from_config,
    platform_cap_status,
    utc_day_bounds,
)

DEFAULT_PLATFORMS = ("x", "bluesky")


@dataclass(frozen=True)
class PlatformCapacityForecast:
    """Capacity forecast for one publishing platform."""

    platform: str
    queued_count: int
    projected_publish_slots: list[str]
    overflow_count: int
    estimated_clearance_time: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "queued_count": self.queued_count,
            "projected_publish_slots": list(self.projected_publish_slots),
            "overflow_count": self.overflow_count,
            "estimated_clearance_time": self.estimated_clearance_time,
        }


@dataclass(frozen=True)
class PublishCapacityForecast:
    """Read-only forecast for the current publish backlog."""

    generated_at: str
    horizon_days: int
    platforms: list[PlatformCapacityForecast]

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "horizon_days": self.horizon_days,
            "platforms": [platform.as_dict() for platform in self.platforms],
        }


def forecast_publish_capacity(
    db,
    config: Any,
    *,
    days: int = 7,
    platform: str = "all",
    now: datetime | None = None,
) -> PublishCapacityForecast:
    """Estimate whether queued content will clear within the forecast horizon."""
    if days < 0:
        raise ValueError("days must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    platforms = _selected_platforms(platform)
    queued_counts = _queued_counts(db, platforms)
    limits = daily_platform_limits_from_config(config)
    embargo_windows = embargo_windows_from_config(config)
    windows = PostingScheduleAnalyzer(db).analyze_optimal_windows(days=90)

    forecasts = [
        _forecast_platform(
            db,
            platform_name,
            queued_counts.get(platform_name, 0),
            limits,
            windows,
            embargo_windows,
            generated_at,
            days,
        )
        for platform_name in platforms
    ]
    return PublishCapacityForecast(
        generated_at=generated_at.isoformat(),
        horizon_days=days,
        platforms=forecasts,
    )


def _selected_platforms(platform: str) -> list[str]:
    if platform == "all":
        return list(DEFAULT_PLATFORMS)
    if platform not in DEFAULT_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    return [platform]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _queued_counts(db, platforms: list[str]) -> dict[str, int]:
    counts = {platform: 0 for platform in platforms}
    cursor = db.conn.execute(
        """SELECT pq.platform, gc.published, gc.bluesky_uri
           FROM publish_queue pq
           INNER JOIN generated_content gc ON gc.id = pq.content_id
           WHERE pq.status IN ('queued', 'failed')
           ORDER BY pq.scheduled_at ASC, pq.id ASC"""
    )
    for row in cursor.fetchall():
        for target in _queue_targets(row["platform"]):
            if target not in counts:
                continue
            if target == "x" and bool(row["published"]):
                continue
            if target == "bluesky" and row["bluesky_uri"] is not None:
                continue
            counts[target] += 1
    return counts


def _queue_targets(platform: str) -> tuple[str, ...]:
    if platform == "all":
        return DEFAULT_PLATFORMS
    if platform in DEFAULT_PLATFORMS:
        return (platform,)
    return ()


def _forecast_platform(
    db,
    platform: str,
    queued_count: int,
    limits: Mapping[str, int],
    windows: list[TimeWindow],
    embargo_windows: list[Any],
    now: datetime,
    days: int,
) -> PlatformCapacityForecast:
    if queued_count == 0:
        return PlatformCapacityForecast(
            platform=platform,
            queued_count=0,
            projected_publish_slots=[],
            overflow_count=0,
            estimated_clearance_time=None,
        )

    slots: list[datetime] = []
    remaining = queued_count
    day_capacity: dict[str, int | None] = {}
    for candidate in _candidate_slots(windows, now, days):
        if is_embargoed(candidate, embargo_windows):
            continue

        day_start, _ = utc_day_bounds(candidate)
        day_key = day_start.date().isoformat()
        if day_key not in day_capacity:
            day_capacity[day_key] = _remaining_day_capacity(
                db,
                platform,
                limits,
                candidate,
            )

        available = day_capacity[day_key]
        if available == 0:
            continue
        assignable = remaining if available is None else min(remaining, available)
        slots.extend(candidate for _ in range(assignable))
        remaining -= assignable
        if available is not None:
            day_capacity[day_key] = available - assignable
        if remaining == 0:
            break

    slot_strings = [slot.isoformat() for slot in slots]
    return PlatformCapacityForecast(
        platform=platform,
        queued_count=queued_count,
        projected_publish_slots=slot_strings,
        overflow_count=max(0, queued_count - len(slot_strings)),
        estimated_clearance_time=slot_strings[-1] if slot_strings else None,
    )


def _remaining_day_capacity(
    db,
    platform: str,
    limits: Mapping[str, int],
    when: datetime,
) -> int | None:
    status = platform_cap_status(db, platform, limits, when, include_queued=False)
    if status.limit is None:
        return None
    return max(0, status.limit - status.published_count)


def _candidate_slots(
    windows: list[TimeWindow],
    now: datetime,
    days: int,
) -> list[datetime]:
    if not windows:
        return []

    ranked_windows = windows[:3]
    horizon_end = now + timedelta(days=days)
    day_count = max(0, days) + 1
    candidates: list[tuple[datetime, int]] = []
    for offset in range(day_count):
        day = (now + timedelta(days=offset)).date()
        for rank, window in enumerate(ranked_windows):
            if day.weekday() != window.day_of_week:
                continue
            candidate = datetime.combine(
                day,
                datetime.min.time(),
                tzinfo=timezone.utc,
            ).replace(hour=window.hour_utc)
            if now <= candidate <= horizon_end:
                candidates.append((candidate, rank))

    candidates.sort(key=lambda item: (item[0], item[1]))
    return [candidate for candidate, _rank in candidates]

