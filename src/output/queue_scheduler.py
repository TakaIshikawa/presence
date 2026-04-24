"""Schedule generated content into publish_queue using recommended windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from evaluation.posting_schedule import (
    embargo_windows_from_config,
    next_allowed_slot,
)
from evaluation.posting_windows import PostingWindow, PostingWindowRecommender
from output.publish_caps import (
    capped_platforms,
    daily_platform_limits_from_config,
    next_daily_cap_slot,
)


VALID_QUEUE_PLATFORMS = {"x", "bluesky", "all"}


@dataclass(frozen=True)
class QueueScheduleResult:
    """Result of scheduling content into the publish queue."""

    queue_id: int
    scheduled_at: datetime
    platform: str


class QueueScheduler:
    """Choose publish queue slots from historical recommendations and policy."""

    def __init__(
        self,
        db: Any,
        config: Any | None = None,
        *,
        recommender: PostingWindowRecommender | None = None,
        embargo_windows: list[Any] | None = None,
        daily_platform_limits: Mapping[str, int] | None = None,
        recommendation_days: int = 90,
        recommendation_limit: int = 3,
    ) -> None:
        self.db = db
        self.recommender = recommender or PostingWindowRecommender(db)
        self.embargo_windows = (
            list(embargo_windows)
            if embargo_windows is not None
            else embargo_windows_from_config(config)
        )
        self.daily_platform_limits = (
            dict(daily_platform_limits)
            if daily_platform_limits is not None
            else daily_platform_limits_from_config(config)
        )
        self.recommendation_days = max(1, int(recommendation_days))
        self.recommendation_limit = max(1, int(recommendation_limit))

    def next_recommended_slot(
        self,
        platform: str,
        *,
        now: datetime | None = None,
    ) -> datetime:
        """Return the next recommended, non-embargoed, cap-aware slot."""
        platform = _normalize_platform(platform)
        now = _as_utc(now or datetime.now(timezone.utc))
        windows = self.recommender.recommend(
            days=self.recommendation_days,
            platform=platform,
            limit=self.recommendation_limit,
        )
        if not windows:
            raise ValueError(f"no posting window recommendations available for {platform}")

        cursor = now
        deadline = now + timedelta(days=366)
        while cursor <= deadline:
            for candidate in self._candidate_slots(windows, cursor):
                adjusted = next_allowed_slot(candidate, self.embargo_windows)
                if self._platforms_capped(platform, adjusted):
                    cursor = next_daily_cap_slot(adjusted)
                    break
                return adjusted
            else:
                cursor += timedelta(days=7)

        raise ValueError(f"no available recommended publish slot found for {platform}")

    def schedule_content(
        self,
        content_id: int,
        platform: str,
        *,
        now: datetime | None = None,
    ) -> QueueScheduleResult:
        """Queue content at the next recommended slot."""
        platform = _normalize_platform(platform)
        scheduled_at = self.next_recommended_slot(platform, now=now)
        queue_id = self.db.queue_for_publishing(
            content_id,
            scheduled_at.isoformat(),
            platform=platform,
        )
        return QueueScheduleResult(
            queue_id=queue_id,
            scheduled_at=scheduled_at,
            platform=platform,
        )

    def _candidate_slots(
        self,
        windows: list[PostingWindow],
        after: datetime,
    ) -> list[datetime]:
        candidates = [
            _next_occurrence(window.day_of_week, window.hour_utc, after)
            for window in windows[: self.recommendation_limit]
        ]
        return sorted(set(candidates))

    def _platforms_capped(self, platform: str, when: datetime) -> bool:
        platforms = ["x", "bluesky"] if platform == "all" else [platform]
        return bool(
            capped_platforms(
                self.db,
                platforms,
                self.daily_platform_limits,
                when,
                include_queued=True,
            )
        )


def queue_at_next_recommended_slot(
    db: Any,
    content_id: int,
    platform: str,
    config: Any | None = None,
    *,
    now: datetime | None = None,
) -> QueueScheduleResult:
    """Queue content at the next recommended slot using config policy."""
    return QueueScheduler(db, config).schedule_content(
        content_id,
        platform,
        now=now,
    )


def _normalize_platform(platform: str) -> str:
    normalized = str(platform).strip().lower()
    if normalized not in VALID_QUEUE_PLATFORMS:
        raise ValueError("platform must be one of: x, bluesky, all")
    return normalized


def _next_occurrence(day_of_week: int, hour_utc: int, after: datetime) -> datetime:
    current = _as_utc(after).replace(minute=0, second=0, microsecond=0)
    if current <= after:
        current += timedelta(hours=1)

    for _ in range(7 * 24):
        if current.weekday() == day_of_week and current.hour == hour_utc:
            return current
        current += timedelta(hours=1)

    days_ahead = (day_of_week - after.weekday()) % 7 or 7
    return (after + timedelta(days=days_ahead)).replace(
        hour=hour_utc,
        minute=0,
        second=0,
        microsecond=0,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
