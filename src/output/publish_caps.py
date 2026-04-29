"""Shared daily publish cap helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Sequence


DEFAULT_DAILY_PLATFORM_LIMITS: dict[str, int] = {}


@dataclass(frozen=True)
class PlatformCapStatus:
    """Daily cap status for one platform."""

    platform: str
    limit: int | None
    published_count: int
    queued_count: int
    effective_count: int
    capped: bool


def daily_platform_limits_from_config(config: Any) -> dict[str, int]:
    """Read non-negative integer daily platform limits from config."""
    publishing_config = getattr(config, "publishing", None)
    value = getattr(publishing_config, "daily_platform_limits", None)
    if not isinstance(value, dict):
        return dict(DEFAULT_DAILY_PLATFORM_LIMITS)
    limits: dict[str, int] = {}
    for platform, limit in value.items():
        if isinstance(limit, bool):
            continue
        try:
            normalized_limit = int(limit)
        except (TypeError, ValueError):
            continue
        if normalized_limit >= 0:
            limits[str(platform)] = normalized_limit
    return limits


def utc_day_bounds(value: datetime) -> tuple[datetime, datetime]:
    """Return UTC start and end datetimes for value's day."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    utc_value = value.astimezone(timezone.utc)
    start = utc_value.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def next_daily_cap_slot(value: datetime) -> datetime:
    """Return the next UTC day start after value."""
    start, _ = utc_day_bounds(value)
    return start + timedelta(days=1)


def platform_cap_status(
    db: Any,
    platform: str,
    limits: Mapping[str, int],
    when: datetime,
    *,
    include_queued: bool = True,
) -> PlatformCapStatus:
    """Return cap pressure for a platform on the UTC day containing when."""
    limit = limits.get(platform)
    day_start, day_end = utc_day_bounds(when)
    published_count = _count_published(db, platform, day_start, day_end)
    queued_count = (
        _count_queued(db, platform, day_start, day_end) if include_queued else 0
    )
    effective_count = published_count + queued_count
    return PlatformCapStatus(
        platform=platform,
        limit=limit,
        published_count=published_count,
        queued_count=queued_count,
        effective_count=effective_count,
        capped=limit is not None and effective_count >= limit,
    )


def capped_platforms(
    db: Any,
    platforms: Sequence[str],
    limits: Mapping[str, int],
    when: datetime,
    *,
    include_queued: bool = True,
) -> list[str]:
    """Return platforms whose daily cap is reached for the day."""
    return [
        platform
        for platform in platforms
        if platform_cap_status(
            db,
            platform,
            limits,
            when,
            include_queued=include_queued,
        ).capped
    ]


def _count_published(
    db: Any,
    platform: str,
    day_start: datetime,
    day_end: datetime,
) -> int:
    row = db.conn.execute(
        """SELECT COUNT(*) AS count
           FROM content_publications
           WHERE platform = ?
             AND status = 'published'
             AND published_at IS NOT NULL
             AND published_at >= ?
             AND published_at < ?""",
        (platform, day_start.isoformat(), day_end.isoformat()),
    ).fetchone()
    return _safe_count(row["count"] if row is not None else 0)


def _count_queued(
    db: Any,
    platform: str,
    day_start: datetime,
    day_end: datetime,
) -> int:
    if not hasattr(db, "count_platform_queue_items_between"):
        return 0
    return _safe_count(
        db.count_platform_queue_items_between(
            platform,
            day_start.isoformat(),
            day_end.isoformat(),
        )
    )


def _safe_count(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0
    return int(value)
