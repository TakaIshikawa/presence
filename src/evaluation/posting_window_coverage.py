"""Coverage planner for upcoming platform posting windows."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from storage.db import Database

from .posting_schedule import is_embargoed
from .posting_windows import DAY_NAMES, PostingWindowRecommender, _parse_datetime


PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}
DEFAULT_FALLBACK_HOURS = (9, 15)


@dataclass(frozen=True)
class CoverageSlot:
    """Recommended open posting slot for one platform."""

    platform: str
    scheduled_at: datetime
    day_of_week: int
    day_name: str
    hour_utc: int
    source: str
    score: float
    confidence: float
    confidence_label: str
    sample_size: int


class PostingWindowCoveragePlanner:
    """Compare preferred posting windows with scheduled and recent coverage."""

    def __init__(
        self,
        db: Database,
        *,
        recommender: PostingWindowRecommender | None = None,
        fallback_hours: Iterable[int] = DEFAULT_FALLBACK_HOURS,
    ) -> None:
        self.db = db
        self.recommender = recommender or PostingWindowRecommender(db)
        self.fallback_hours = tuple(int(hour) % 24 for hour in fallback_hours)

    def recommend_slots(
        self,
        *,
        days_ahead: int = 7,
        platform: str = "all",
        include_published: bool = False,
        limit_per_platform: int = 3,
        history_days: int = 90,
        now: datetime | None = None,
        embargo_windows: list[Any] | None = None,
    ) -> list[CoverageSlot]:
        """Return open preferred slots per platform within the requested horizon."""
        platforms = _platforms_for(platform)
        days_ahead = max(1, int(days_ahead))
        limit_per_platform = max(0, int(limit_per_platform))
        if limit_per_platform == 0:
            return []

        start = _to_utc(now or datetime.now(timezone.utc))
        end = start + timedelta(days=days_ahead)
        occupied = self._occupied_buckets(
            start=start,
            end=end,
            platforms=platforms,
            include_published=include_published,
        )

        slots: list[CoverageSlot] = []
        for platform_name in platforms:
            windows = self._preferred_windows(platform_name, history_days)
            platform_slots = self._open_slots_for_platform(
                platform=platform_name,
                windows=windows,
                start=start,
                end=end,
                occupied=occupied,
                embargo_windows=embargo_windows,
                limit=limit_per_platform,
            )
            slots.extend(platform_slots)

        return sorted(slots, key=lambda slot: (slot.scheduled_at, slot.platform))

    def _preferred_windows(
        self,
        platform: str,
        history_days: int,
    ) -> list[tuple[int, int, float, float, str, int, str]]:
        learned = self.recommender.recommend(
            days=max(1, int(history_days)),
            platform=platform,
            limit=12,
        )
        if learned:
            return [
                (
                    window.day_of_week,
                    window.hour_utc,
                    window.normalized_engagement,
                    window.confidence,
                    window.confidence_label,
                    window.sample_size,
                    "learned",
                )
                for window in learned
            ]

        return [
            (day, hour, 0.0, 0.0, "fallback", 0, "fallback")
            for day in range(7)
            for hour in self.fallback_hours
        ]

    def _open_slots_for_platform(
        self,
        *,
        platform: str,
        windows: list[tuple[int, int, float, float, str, int, str]],
        start: datetime,
        end: datetime,
        occupied: set[tuple[str, datetime]],
        embargo_windows: list[Any] | None,
        limit: int,
    ) -> list[CoverageSlot]:
        candidates: list[tuple[int, datetime, tuple[int, int, float, float, str, int, str]]] = []
        for rank, window in enumerate(windows):
            day_of_week, hour_utc = window[0], window[1]
            current = _next_occurrence(day_of_week, hour_utc, start)
            while current <= end:
                if (platform, _bucket_start(current)) not in occupied and not is_embargoed(
                    current,
                    embargo_windows,
                ):
                    candidates.append((rank, current, window))
                current += timedelta(days=7)

        candidates.sort(key=lambda item: (item[1], item[0]))
        slots: list[CoverageSlot] = []
        seen: set[datetime] = set()
        for _, scheduled_at, window in candidates:
            if scheduled_at in seen:
                continue
            seen.add(scheduled_at)
            day_of_week, hour_utc, score, confidence, confidence_label, sample_size, source = window
            slots.append(
                CoverageSlot(
                    platform=platform,
                    scheduled_at=scheduled_at,
                    day_of_week=day_of_week,
                    day_name=DAY_NAMES[day_of_week],
                    hour_utc=hour_utc,
                    source=source,
                    score=round(score, 2),
                    confidence=round(confidence, 2),
                    confidence_label=confidence_label,
                    sample_size=sample_size,
                )
            )
            if len(slots) >= limit:
                break
        return slots

    def _occupied_buckets(
        self,
        *,
        start: datetime,
        end: datetime,
        platforms: tuple[str, ...],
        include_published: bool,
    ) -> set[tuple[str, datetime]]:
        occupied: set[tuple[str, datetime]] = set()
        platform_set = set(platforms)

        for row in self._fetch_queue_rows(start, end):
            scheduled_at = _parse_datetime(row["scheduled_at"])
            if scheduled_at is None:
                continue
            for target in _platform_targets(row["platform"]):
                if target in platform_set:
                    occupied.add((target, _bucket_start(scheduled_at)))

        if include_published:
            for row in self._fetch_published_rows(start, end, platforms):
                published_at = _parse_datetime(row["published_at"])
                if published_at is not None:
                    occupied.add((row["platform"], _bucket_start(published_at)))

        return occupied

    def _fetch_queue_rows(self, start: datetime, end: datetime) -> list[dict]:
        cursor = self.db.conn.execute(
            """SELECT platform, scheduled_at
               FROM publish_queue
               WHERE status IN ('queued', 'held')
               ORDER BY scheduled_at, id""",
        )
        rows = []
        for row in cursor.fetchall():
            scheduled_at = _parse_datetime(row["scheduled_at"])
            if scheduled_at is not None and start <= scheduled_at <= end:
                rows.append(dict(row))
        return rows

    def _fetch_published_rows(
        self,
        start: datetime,
        end: datetime,
        platforms: tuple[str, ...],
    ) -> list[dict]:
        placeholders = ",".join("?" for _ in platforms)
        cursor = self.db.conn.execute(
            f"""SELECT platform, published_at
                FROM content_publications
                WHERE status = 'published'
                  AND published_at IS NOT NULL
                  AND platform IN ({placeholders})
                ORDER BY published_at, id""",
            platforms,
        )
        rows = []
        for row in cursor.fetchall():
            published_at = _parse_datetime(row["published_at"])
            if published_at is not None and start <= published_at <= end:
                rows.append(dict(row))
        return rows


def recommend_posting_window_coverage(
    db: Database,
    *,
    days_ahead: int = 7,
    platform: str = "all",
    include_published: bool = False,
    limit_per_platform: int = 3,
    history_days: int = 90,
    now: datetime | None = None,
    embargo_windows: list[Any] | None = None,
) -> list[CoverageSlot]:
    """Convenience wrapper for ``PostingWindowCoveragePlanner.recommend_slots``."""
    return PostingWindowCoveragePlanner(db).recommend_slots(
        days_ahead=days_ahead,
        platform=platform,
        include_published=include_published,
        limit_per_platform=limit_per_platform,
        history_days=history_days,
        now=now,
        embargo_windows=embargo_windows,
    )


def coverage_slots_to_dicts(slots: Iterable[CoverageSlot]) -> list[dict]:
    """Serialize coverage slots for JSON callers."""
    return [
        {
            "platform": slot.platform,
            "scheduled_at": slot.scheduled_at.isoformat(),
            "day_of_week": slot.day_of_week,
            "day_name": slot.day_name,
            "hour_utc": slot.hour_utc,
            "source": slot.source,
            "score": slot.score,
            "confidence": slot.confidence,
            "confidence_label": slot.confidence_label,
            "sample_size": slot.sample_size,
        }
        for slot in slots
    ]


def _platforms_for(platform: str) -> tuple[str, ...]:
    normalized = str(platform).strip().lower()
    if normalized not in VALID_PLATFORMS:
        raise ValueError("platform must be one of: all, x, bluesky")
    return PLATFORMS if normalized == "all" else (normalized,)


def _platform_targets(platform: str) -> tuple[str, ...]:
    normalized = str(platform).strip().lower()
    if normalized == "all":
        return PLATFORMS
    if normalized in PLATFORMS:
        return (normalized,)
    return ()


def _to_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _bucket_start(value: datetime) -> datetime:
    return _to_utc(value).replace(minute=0, second=0, microsecond=0)


def _next_occurrence(day_of_week: int, hour_utc: int, after: datetime) -> datetime:
    current = after.replace(minute=0, second=0, microsecond=0)
    if current <= after:
        current += timedelta(hours=1)
    hours_ahead = ((day_of_week - current.weekday()) % 7) * 24 + (hour_utc - current.hour)
    if hours_ahead < 0:
        hours_ahead += 7 * 24
    return current + timedelta(hours=hours_ahead)
